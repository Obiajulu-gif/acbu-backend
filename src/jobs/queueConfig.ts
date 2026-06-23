import { QUEUES } from "../config/rabbitmq";

export const QUEUE_RETRY_LIMITS: Record<string, number> = {
  [QUEUES.AUDIT_LOGS]: 3,
  [QUEUES.NOTIFICATIONS]: 5,
  [QUEUES.OTP_SEND]: 5,
  [QUEUES.USDC_CONVERSION]: 5,
  [QUEUES.WEBHOOKS]: 5,
  [QUEUES.XLM_TO_ACBU]: 5,
  [QUEUES.USDC_CONVERT_AND_MINT]: 5,
  [QUEUES.WITHDRAWAL_PROCESSING]: 5,
};

export const DEFAULT_MAX_RETRIES = 5;

export function getQueueMaxRetries(queueName: string): number {
  return QUEUE_RETRY_LIMITS[queueName] ?? DEFAULT_MAX_RETRIES;
}
