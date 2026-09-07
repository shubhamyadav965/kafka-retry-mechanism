import { ConfigService } from '@nestjs/config';

export function getRetryConfig(configService: ConfigService) {
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
