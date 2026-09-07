import { ConfigService } from '@nestjs/config';

export interface RetryConfig {
  maxRetries: number;
  retryDelays: number[];
  dlqSuffix: string;
}

/**
 * Reads retry-related configuration from environment variables.
 *
 * Environment variables are strings, so this function converts
 * them into the correct types used by the application.
 */
export function getRetryConfig(configService: ConfigService): RetryConfig {
  const maxRetries = Number(configService.get<string>('MAX_RETRIES') ?? '3');

  const retryDelays =
    configService
      .get<string>('RETRY_DELAYS_MS')
      ?.split(',')
      .map((delay) => Number(delay.trim()))
      .filter((delay) => !Number.isNaN(delay)) ?? [];

  const dlqSuffix = configService.get<string>('DLQ_SUFFIX') ?? '.dlq';

  return {
    maxRetries,
    retryDelays,
    dlqSuffix,
  };
}
