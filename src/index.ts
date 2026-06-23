import { initTracing } from "./config/tracing";
initTracing();

import express, { type NextFunction, type Request, type Response } from "express";
<<<<<<< fix/trust-proxy-config
import helmet from "helmet";
=======
>>>>>>> dev
import compression from "compression";
import swaggerUi from "swagger-ui-express";
import { config } from "./config/env";
import { logger } from "./config/logger";
import { execSync } from "child_process";
import { connectMongoDB, disconnectMongoDB } from "./config/mongodb";
import { connectRabbitMQ, disconnectRabbitMQ } from "./config/rabbitmq";
import { prisma, connectWithRetry } from "./config/database";
import { corsMiddleware } from "./middleware/cors";
import { securityHeadersMiddleware } from "./middleware/securityHeaders";
import { requestLogger } from "./middleware/logger";
import { errorHandler, AppError } from "./middleware/errorHandler";
import { standardRateLimiter } from "./middleware/rateLimiter";
import { swaggerSpec } from "./config/swagger";
import routes from "./routes";
import webhookRoutes from "./routes/webhookRoutes";
import { ErrorCodes } from "./types/errorCodes";
import { registerGracefulShutdown, setHttpServer } from "./gracefulShutdown";

const app: express.Express = express();

// Parse trust proxy hop count safely from environment variables (Default to 0 for local development)
const trustProxyValue = process.env.TRUST_PROXY
  ? (isNaN(Number(process.env.TRUST_PROXY)) ? process.env.TRUST_PROXY : Number(process.env.TRUST_PROXY))
  : 0;

app.set("trust proxy", trustProxyValue);

const MAX_REQUEST_BODY_SIZE = "1mb";
const SUPPORTED_REQUEST_ENCODINGS = new Set(["identity", "gzip"]);

function normalizeContentEncoding(req: Request): string {
  const header = req.headers["content-encoding"];
  const value = Array.isArray(header) ? header[0] : header;
  return (value || "identity").trim().toLowerCase() || "identity";
}

function validateRequestContentEncoding(
  req: Request,
  _res: Response,
  next: NextFunction,
): void {
  const encoding = normalizeContentEncoding(req);

  if (!SUPPORTED_REQUEST_ENCODINGS.has(encoding)) {
    return next(
      new AppError(
        "Unsupported Content-Encoding. Use identity or gzip.",
        415,
        "UNSUPPORTED_CONTENT_ENCODING",
      ),
    );
  }

  next();
}

// Security middleware
app.use(securityHeadersMiddleware);
app.use(corsMiddleware);

// Compress all JSON/text responses to reduce bandwidth on large payloads
app.use(compression());

// Validate and explicitly enable request body inflation for gzip-compressed clients (#409).
app.use(validateRequestContentEncoding);
app.use(express.urlencoded({ extended: true, inflate: true, limit: MAX_REQUEST_BODY_SIZE }));

// ── Webhook Content-Type validation ────────────────────────────────────────────
// Must check Content-Type BEFORE raw body parser, since non-JSON bodies would
// bypass parsing and cause unexpected behavior in signature verification.
function validateWebhookContentType(req: Request, _res: Response, next: NextFunction): void {
  if (!req.is("application/json")) {
    return next(new AppError("Content-Type must be application/json", 415, "INVALID_CONTENT_TYPE"));
  }
  next();
}

// Webhooks need raw body for signature verification; mount before json()
app.use(
  `/${config.apiVersion}/webhooks`,
  validateWebhookContentType,
  express.raw({ inflate: true, limit: MAX_REQUEST_BODY_SIZE, type: "application/json" }),
  (req: express.Request, res: express.Response, next) => {
    const raw = req.body as Buffer;
    (req as unknown as { rawBody: Buffer }).rawBody = raw;
    try {
      (req as unknown as { body: unknown }).body = JSON.parse(raw.toString());
    } catch {
      throw new AppError("Invalid JSON payload", 400, ErrorCodes.INVALID_JSON);
    }
    next();
  },
  webhookRoutes,
);
app.use(express.json({ inflate: true, limit: MAX_REQUEST_BODY_SIZE }));

app.use(
  (
    err: Error & { type?: string },
    _req: express.Request,
    res: express.Response,
    next: express.NextFunction,
  ) => {
    if (err?.type === "entity.too.large") {
      res.status(413).json({
        error: {
          code: "PAYLOAD_TOO_LARGE",
          message: "Request body exceeds maximum allowed size",
        },
      });
      return;
    }
    if (err?.type === "encoding.unsupported") {
      res.status(415).json({
        error: {
          code: "UNSUPPORTED_CONTENT_ENCODING",
          message: "Unsupported request body encoding",
        },
      });
      return;
    }
    next(err);
  },
);

// Logging
app.use(requestLogger);

// Rate limiting
app.use(standardRateLimiter);

// API Documentation — disabled in production to prevent endpoint enumeration (#274)
if (config.nodeEnv !== "production") {
  app.use("/api-docs", swaggerUi.serve, swaggerUi.setup(swaggerSpec));
  // Raw JSON spec for tooling / CI spec-drift checks (#292)
  app.get("/api-docs.json", (_req, res) => {
    res.json(swaggerSpec);
  });
}

// Routes
app.use(`/api/${config.apiVersion}`, routes);

// Error handling (must be last)
app.use(errorHandler);

// Initialize connections and start server
async function startServer() {
  try {
    // Ensures schema is in sync before accepting traffic; prevents "table does not exist" on new columns.
    logger.info("Applying Prisma migrations...");
    execSync("npx prisma migrate deploy", { stdio: "inherit" });
    logger.info("Prisma migrations applied successfully");

    // Establish the DB connection with backoff + jitter so coordinated restarts
    // don't stampede the database's connection slots (#402).
    await connectWithRetry();

    // Connect to MongoDB (optional: server starts even if unreachable or MONGODB_URI empty)
    if (config.mongodbUri) {
      try {
        await connectMongoDB();
        logger.info("MongoDB connected");
      } catch (mongoError) {
        logger.warn(
          "MongoDB unavailable, continuing without cache. Set MONGODB_URI and ensure network access for cache.",
          mongoError,
        );
      }
    } else {
      logger.warn("MONGODB_URI not set; cache will be disabled.");
    }

    // Connect to RabbitMQ (optional: server starts even if unreachable or credentials invalid)
    let rabbitReady = false;
    if (config.rabbitmqUrl) {
      try {
        await connectRabbitMQ();
        logger.info("RabbitMQ connected");
        rabbitReady = true;
      } catch (rabbitError) {
        logger.warn(
          "RabbitMQ unavailable, continuing without queue-based features. Set RABBITMQ_URL and ensure broker access.",
          rabbitError,
        );
      }
    } else {
      logger.warn("RABBITMQ_URL not set; queue-based features disabled.");
    }

    if (rabbitReady) {
      // Start notification consumer (OTP_SEND + NOTIFICATIONS → email/SMS)
      const { startNotificationConsumer } = await import("./jobs/notificationConsumer");
      await startNotificationConsumer();

      // Start audit consumer (AUDIT_LOGS → database)
      const { startAuditConsumer } = await import("./jobs/auditConsumer");
      await startAuditConsumer();

      // Start outbound webhook consumer (WEBHOOKS → deliver with HMAC-SHA256)
      const { startWebhookConsumer } = await import("./jobs/webhookConsumer");
      await startWebhookConsumer();

      // Start oracle update scheduler (every 6h)
      const { startOracleUpdateScheduler } = await import("./jobs/oracleUpdateJob");
      await startOracleUpdateScheduler();

      // Start reserve tracking scheduler (every 6h)
      const { startReserveTrackingScheduler } = await import("./jobs/reserveTrackingJob");
      await startReserveTrackingScheduler();

      // Start daily rebalancing scheduler (00:00 UTC)
      const { startRebalancingScheduler } = await import("./jobs/rebalancingJob");
      await startRebalancingScheduler();

      // Start proposed basket weights scheduler (metrics → proposed weights, e.g. monthly)
      const { startProposedWeightsScheduler } = await import("./jobs/proposedWeightsJob");
      await startProposedWeightsScheduler();

      // Start USDC conversion consumer (MintEvent → basket allocation)
      const { startUsdcConversionConsumer } = await import("./jobs/usdcConversionJob");
      await startUsdcConversionConsumer();

      // Start withdrawal processing consumer (BurnEvent → fintech disbursement)
      const { startWithdrawalProcessingConsumer } = await import("./jobs/withdrawalProcessingJob");
      await startWithdrawalProcessingConsumer();

      // Start XLM→ACBU consumer (XLM deposit: sell XLM and mint ACBU to user)
      const { startXlmToAcbuConsumer } = await import("./jobs/xlmToAcbuJob");
      await startXlmToAcbuConsumer();

      // Start USDC convert-and-mint consumer (USDC deposit: convert USDC→XLM in backend, then mint)
      const { startUsdcConvertAndMintConsumer } = await import("./jobs/usdcConvertAndMintJob");
      await startUsdcConvertAndMintConsumer();

      // Investment withdrawal: mark requests available at T+24h and send notification
      const { startInvestmentWithdrawalScheduler } = await import("./jobs/investmentWithdrawalJob");
      await startInvestmentWithdrawalScheduler();

      // Start yield accrual scheduler (run once at startup to seed accruals)
      const { startYieldAccrualScheduler } = await import("./jobs/yieldAccrualJob");
      await startYieldAccrualScheduler();

      // Start weekly weight drift audit job (Monday 00:00 UTC)
      const { startWeightDriftAuditScheduler } = await import("./jobs/weightDriftAuditJob");
      await startWeightDriftAuditScheduler();

      // Salary schedule: trigger recurring salary payments
      const { startSalaryScheduleScheduler } = await import("./jobs/salaryScheduleJob");
      await startSalaryScheduleScheduler();

      // Register MintEvent/BurnEvent handlers and start Stellar event listener (runs in background)
      const { startMintEventListener } = await import("./jobs/acbu_minting_event_listener");
      await startMintEventListener();
      const { startBurnEventListener } = await import("./jobs/acbu_burning_event_listener");
      await startBurnEventListener();
      const { startSavingsVaultEventListener } =
        await import("./jobs/acbu_savings_vault_event_listener");
      await startSavingsVaultEventListener();
      const { startLendingPoolEventListener } =
        await import("./jobs/acbu_lending_pool_event_listener");
      await startLendingPoolEventListener();
      const { startEscrowEventListener } = await import("./jobs/acbu_escrow_event_listener");
      await startEscrowEventListener();
    }
    const { eventListener } = await import("./services/stellar/eventListener");
    void eventListener.start();

    // Mark application as ready for health checks
    const { markStartupComplete } = await import("./services/health/healthService");
    markStartupComplete();

    // Start HTTP server
    const server = app.listen(config.port, () => {
      logger.info(`Server running on port ${config.port}`);
      logger.info(`Environment: ${config.nodeEnv}`);
      logger.info(`API Version: ${config.apiVersion}`);
      if (config.nodeEnv !== "production") {
        logger.info(`API Documentation: http://localhost:${config.port}/api-docs`);
      }
    });
    setHttpServer(server);
  } catch (error) {
    logger.error("Failed to start server", error);
    process.exit(1);
  }
}

if (require.main === module) {
  registerGracefulShutdown();
  void startServer();
}

export { startServer };
export default app;
