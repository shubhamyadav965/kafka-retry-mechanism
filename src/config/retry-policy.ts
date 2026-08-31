export const RETRY_TOPICS = [
  'orders.retry.1m',
  'orders.retry.5m',
  'orders.retry.10m',
];

export const RETRY_DELAYS = [
  10_000, // 10 seconds during
  20_000, // 20 seconds during
  30_000, // 30 seconds during
];

export function getRetryTopic(retryCount: number): string {
  return RETRY_TOPICS[retryCount - 1];
}

export function getRetryDelay(retryCount: number): number {
  return RETRY_DELAYS[retryCount - 1];
}
