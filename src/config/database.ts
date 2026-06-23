import { PrismaClient, Prisma } from "@prisma/client";
import { withAccelerate } from "@prisma/extension-accelerate";
import { config } from "./env";
import { logger } from "./logger";
import { trace, SpanStatusCode } from "@opentelemetry/api";

// B-056: Validate URL assignments at boot to prevent runtime/migration confusion.
// DATABASE_URL  → direct PostgreSQL only (used by prisma migrate)
// PRISMA_ACCELERATE_URL → prisma:// or prisma+postgres:// protocol (runtime connection pooling)
const ACCELERATE_PROTOCOL_RE = /^prisma(\+postgres)?:\/\//i;

if (ACCELERATE_PROTOCOL_RE.test(config.databaseUrl)) {
  throw new Error(
    "[database] DATABASE_URL must be a direct PostgreSQL connection string " +
      "(postgresql:// or postgres://). " +
      "An Accelerate URL (prisma://) was detected — " +
      "set that value in PRISMA_ACCELERATE_URL instead. " +
      "Using Accelerate for migrations will fail.",
  );
}

if (config.prismaAccelerateUrl && !ACCELERATE_PROTOCOL_RE.test(config.prismaAccelerateUrl)) {
  logger.warn(
    "[database] PRISMA_ACCELERATE_URL does not start with prisma:// — " +
      "expected an Accelerate connection string. " +
      "If you intended a direct URL, set DATABASE_URL and leave PRISMA_ACCELERATE_URL unset.",
  );
}

const useAccelerate = Boolean(config.prismaAccelerateUrl);
const databaseUrl = useAccelerate ? config.prismaAccelerateUrl! : config.databaseUrl;

logger.info(
  `[database] Runtime connection: ${useAccelerate ? "Prisma Accelerate (pooled)" : "direct PostgreSQL"}`,
);
logger.info(
  "[database] Migration connection: direct PostgreSQL via DATABASE_URL " +
    "(run prisma migrate against DATABASE_URL, never against PRISMA_ACCELERATE_URL)",
);

const basePrisma = new PrismaClient({
  datasources: { db: { url: databaseUrl } },
  log: [
    { level: "query", emit: "event" },
    { level: "error", emit: "stdout" },
    { level: "warn", emit: "stdout" },
  ],
});

const replicaUrl = config.databaseUrlReplica || databaseUrl;

const basePrismaReplica = new PrismaClient({
  datasources: { db: { url: replicaUrl } },
  log: [
    { level: "query", emit: "event" },
    { level: "error", emit: "stdout" },
    { level: "warn", emit: "stdout" },
  ],
});

// Retry config for connection pool exhaustion (Prisma Accelerate)
const MAX_RETRIES = 3;
const BASE_BACKOFF_MS = 200;

function isPoolExhaustionError(err: unknown): boolean {
  if (err instanceof Prisma.PrismaClientKnownRequestError) {
    return err.code === "P2024";
  }
  return false;
}

// OTel: wrap every Prisma query in a span so traces link DB calls to parent spans
basePrisma.$use(async (params, next) => {
  const tracer = trace.getTracer("prisma");
  const spanName = `prisma.${params.model ?? "raw"}.${params.action}`;
  return tracer.startActiveSpan(spanName, async (span) => {
    span.setAttributes({
      "db.system": "postgresql",
      "db.operation": params.action,
      ...(params.model ? { "db.prisma.model": params.model } : {}),
    });
    try {
      const result = await next(params);
      span.setStatus({ code: SpanStatusCode.OK });
      return result;
    } catch (err) {
      span.setStatus({ code: SpanStatusCode.ERROR, message: String(err) });
      throw err;
    } finally {
      span.end();
    }
  });
});

basePrismaReplica.$use(async (params, next) => {
  const tracer = trace.getTracer("prisma");
  const spanName = `prisma.replica.${params.model ?? "raw"}.${params.action}`;
  return tracer.startActiveSpan(spanName, async (span) => {
    span.setAttributes({
      "db.system": "postgresql",
      "db.operation": params.action,
      ...(params.model ? { "db.prisma.model": params.model } : {}),
    });
    try {
      const result = await next(params);
      span.setStatus({ code: SpanStatusCode.OK });
      return result;
    } catch (err) {
      span.setStatus({ code: SpanStatusCode.ERROR, message: String(err) });
      throw err;
    } finally {
      span.end();
    }
  });
});

// Connection pool exhaustion retry middleware: retry with exponential backoff
// when Prisma Accelerate returns P2024 (connection pool timeout).
// Retries up to MAX_RETRIES-1 times (attempt 1 = first try, attempt 4 throws).
basePrisma.$use(async (params, next) => {
  let lastError: unknown;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await next(params);
    } catch (err) {
      if (!isPoolExhaustionError(err)) {
        throw err;
      }
      if (attempt < MAX_RETRIES) {
        lastError = err;
        const backoff = BASE_BACKOFF_MS * 2 ** (attempt - 1);
        logger.warn("Prisma connection pool exhausted, retrying", {
          model: params.model,
          action: params.action,
          attempt,
          maxRetries: MAX_RETRIES,
          backoffMs: backoff,
        });
        await new Promise((r) => setTimeout(r, backoff));
      } else {
        throw err;
      }
    }
  }
  throw lastError;
});

basePrismaReplica.$use(async (params, next) => {
  let lastError: unknown;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await next(params);
    } catch (err) {
      if (!isPoolExhaustionError(err)) {
        throw err;
      }
      if (attempt < MAX_RETRIES) {
        lastError = err;
        const backoff = BASE_BACKOFF_MS * 2 ** (attempt - 1);
        logger.warn("Prisma replica connection pool exhausted, retrying", {
          model: params.model,
          action: params.action,
          attempt,
          maxRetries: MAX_RETRIES,
          backoffMs: backoff,
        });
        await new Promise((r) => setTimeout(r, backoff));
      } else {
        throw err;
      }
    }
  }
  throw lastError;
});

export const prisma = useAccelerate ? basePrisma.$extends(withAccelerate()) : basePrisma;
export const prismaReplica = useAccelerate
  ? basePrismaReplica.$extends(withAccelerate())
  : basePrismaReplica;

// Log queries in development ($on exists only on base client, not on extended proxy)
if (config.nodeEnv === "development") {
  basePrisma.$on("query" as never, (e: { query: string; params: string; duration: number }) => {
    logger.debug("Query", {
      query: e.query,
      params: e.params,
      duration: `${e.duration}ms`,
    });
  });
  basePrismaReplica.$on(
    "query" as never,
    (e: { query: string; params: string; duration: number }) => {
      logger.debug("Query Replica", {
        query: e.query,
        params: e.params,
        duration: `${e.duration}ms`,
      });
    },
  );
}

/**
 * Establish the initial database connection with bounded exponential backoff and
 * full jitter (#402).
 *
 * After a shared outage or coordinated restart, every instance would otherwise
 * retry on the same fixed schedule and slam the database's limited connection
 * slots simultaneously — a thundering herd that triggers cascading connection
 * rejections. Full jitter (`random(0, cappedBackoff)`) spreads reconnection
 * attempts across the window so the load on connection slots is smoothed out.
 *
 * Resolves once connected; throws after the configured number of attempts is
 * exhausted so the caller can fail startup loudly.
 */
export async function connectWithRetry(): Promise<void> {
  const maxRetries = config.database.connectMaxRetries;
  const baseBackoff = config.database.connectBaseBackoffMs;
  const maxBackoff = config.database.connectMaxBackoffMs;

  let lastError: unknown;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      await Promise.all([basePrisma.$connect(), basePrismaReplica.$connect()]);
      if (attempt > 1) {
        logger.info("[database] Connected after retry", { attempt });
      } else {
        logger.info("[database] Connected");
      }
      return;
    } catch (err) {
      lastError = err;
      if (attempt >= maxRetries) break;

      // Exponential backoff capped at maxBackoff, then full jitter in [0, cap].
      const cappedBackoff = Math.min(maxBackoff, baseBackoff * 2 ** (attempt - 1));
      const delay = Math.floor(Math.random() * cappedBackoff);
      logger.warn("[database] Connection attempt failed, retrying with jitter", {
        attempt,
        maxRetries,
        delayMs: delay,
        error: err instanceof Error ? err.message : String(err),
      });
      await new Promise((r) => setTimeout(r, delay));
    }
  }

  logger.error("[database] Failed to connect after exhausting retries", {
    maxRetries,
    error: lastError instanceof Error ? lastError.message : String(lastError),
  });
  throw lastError instanceof Error
    ? lastError
    : new Error("Database connection failed after exhausting retries");
}

// Handle graceful shutdown
process.on("beforeExit", async () => {
  await Promise.all([basePrisma.$disconnect(), basePrismaReplica.$disconnect()]);
});

export default prisma;
