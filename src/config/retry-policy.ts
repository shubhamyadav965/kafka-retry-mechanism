export const RETRY_CONFIG = {
  maxRetries: 3,

  retryDelays: [
    60_000,       // Retry 1: 1 minute
    5 * 60_000,   // Retry 2: 5 minutes
    10 * 60_000,  // Retry 3: 10 minutes
  ],

  dlqSuffix: '.dlq',
};

// Returns the delay for a particular retry attempt.

export function getRetryDelay(retryCount: number): number {
  return RETRY_CONFIG.retryDelays[retryCount - 1];
}

/**
 * Returns the maximum number of retry attempts.
 */
export function getMaxRetries(): number {
  return RETRY_CONFIG.maxRetries;
}

/**
 * Creates the retry topic name for a given original topic.
 *
 * Example:
 * orders + retry 1 → orders.retry.1m
 * orders + retry 2 → orders.retry.5m
 * orders + retry 3 → orders.retry.10m
 */
export function getRetryTopic(
  originalTopic: string,
  retryCount: number,
): string {
  const retryDelay = getRetryDelay(retryCount);

  const delayInMinutes = Math.floor(
    retryDelay / 60_000,
  );
  return `${originalTopic}.retry.${delayInMinutes}m`;
}

/**
 * Creates the DLQ topic name from the original topic.
 * Example: orders → orders.dlq
 */
export function getDlqTopic(originalTopic: string): string {
  return `${originalTopic}${RETRY_CONFIG.dlqSuffix}`;
}