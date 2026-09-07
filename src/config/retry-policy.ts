import { ConfigService } from '@nestjs/config';
import { getRetryConfig } from './retry.config';

/**
 * Returns the configured retry delay for a retry attempt.
 *
 * retryCount starts at 1.
 *
 * Example:
 * retry 1 → 1 minute
 * retry 2 → 5 minutes
 * retry 3 → 10 minutes
 */
export function getRetryDelay(
  configService: ConfigService,
  retryCount: number,
): number {
  const config = getRetryConfig(configService);

  return config.retryDelays[retryCount - 1];
}

/**
 * Returns the maximum number of retry attempts.
 */
export function getMaxRetries(configService: ConfigService): number {
  const config = getRetryConfig(configService);

  return config.maxRetries;
}

/**
 * Creates the retry topic for a retry attempt.
 *
 * Example:
 *
 * orders + retry 1 → orders.retry.1m
 * orders + retry 2 → orders.retry.5m
 * orders + retry 3 → orders.retry.10m
 */
export function getRetryTopic(
  configService: ConfigService,
  originalTopic: string,
  retryCount: number,
): string {
  const retryDelay = getRetryDelay(configService, retryCount);

  const delayInMinutes = Math.floor(retryDelay / 60_000);

  return `${originalTopic}.retry.${delayInMinutes}m`;
}

/**
 * Returns the DLQ topic for the original topic.
 *
 * Example:
 *
 * orders → orders.dlq
 */
export function getDlqTopic(
  configService: ConfigService,
  originalTopic: string,
): string {
  const config = getRetryConfig(configService);

  return `${originalTopic}${config.dlqSuffix}`;
}

/**
 * Returns all retry topics for an original topic.
 */
export function getRetryTopics(
  configService: ConfigService,
  originalTopic: string,
): string[] {
  const maxRetries = getMaxRetries(configService);

  return Array.from({ length: maxRetries }, (_, index) =>
    getRetryTopic(configService, originalTopic, index + 1),
  );
}
