export const RETRY_CONFIG = {
  maxRetries: 3,

  retryTopics: ['orders.retry.1m', 'orders.retry.5m', 'orders.retry.10m'],

  retryDelays: [
    60_000, // 1 minute
    5 * 60_000, // 5 minutes
    10 * 60_000, // 10 minutes
  ],

  dlqSuffix: '.dlq',
};

export function getRetryTopic(retryCount: number): string {
  return RETRY_CONFIG.retryTopics[retryCount - 1];
}

export function getRetryDelay(retryCount: number): number {
  return RETRY_CONFIG.retryDelays[retryCount - 1];
}

export function getMaxRetries(): number {
  return RETRY_CONFIG.maxRetries;
}

export function getDlqTopic(originalTopic: string): string {
  return `${originalTopic}${RETRY_CONFIG.dlqSuffix}`;
}
