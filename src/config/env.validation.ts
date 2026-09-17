// process.env values are always strings or undefined; guard rather than
// call String() directly on `unknown`.
function asString(value: unknown): string {
  return typeof value === 'string' ? value : '';
}

/**
 * Fails NestJS bootstrap fast with a clear message when required
 * configuration is missing or malformed, instead of surfacing as a
 * confusing runtime error (e.g. NaN delays, empty topic lists) later.
 */
export function validateEnv(
  config: Record<string, unknown>,
): Record<string, unknown> {
  const errors: string[] = [];

  const kafkaBrokers = asString(config.KAFKA_BROKERS).trim();
  if (!kafkaBrokers) {
    errors.push('KAFKA_BROKERS is required (e.g. "kafka:9093")');
  }

  const originalTopics = asString(config.KAFKA_ORIGINAL_TOPICS)
    .split(',')
    .map((topic) => topic.trim())
    .filter(Boolean);
  if (originalTopics.length === 0) {
    errors.push('KAFKA_ORIGINAL_TOPICS must list at least one topic');
  }

  const maxRetries = Number(config.MAX_RETRIES);
  if (!Number.isInteger(maxRetries) || maxRetries < 0) {
    errors.push('MAX_RETRIES must be a non-negative integer');
  }

  const rawRetryDelays = asString(config.RETRY_DELAYS_MS)
    .split(',')
    .map((delay) => delay.trim())
    .filter((delay) => delay.length > 0);

  if (rawRetryDelays.length === 0) {
    errors.push('RETRY_DELAYS_MS must list at least one delay in milliseconds');
  } else {
    const parsedDelays = rawRetryDelays.map(Number);
    const invalidDelays = rawRetryDelays.filter(
      (_, index) =>
        !Number.isFinite(parsedDelays[index]) || parsedDelays[index] <= 0,
    );

    if (invalidDelays.length > 0) {
      errors.push(
        `RETRY_DELAYS_MS values must all be numbers greater than 0, got: ${invalidDelays.join(', ')}`,
      );
    } else if (
      Number.isInteger(maxRetries) &&
      parsedDelays.length < maxRetries
    ) {
      // getRetryDelay() indexes retryDelays[retryCount - 1]; fewer delays
      // than MAX_RETRIES would silently produce an Invalid Date for the
      // missing tiers.
      errors.push(
        `RETRY_DELAYS_MS must provide at least MAX_RETRIES (${maxRetries}) delays, got ${parsedDelays.length}`,
      );
    }
  }

  if (
    config.DLQ_SUFFIX !== undefined &&
    asString(config.DLQ_SUFFIX).trim() === ''
  ) {
    errors.push('DLQ_SUFFIX cannot be empty when set');
  }

  const redisHost = asString(config.REDIS_HOST).trim();
  if (!redisHost) {
    errors.push('REDIS_HOST is required');
  }

  const redisPort = Number(config.REDIS_PORT);
  if (!Number.isInteger(redisPort) || redisPort <= 0 || redisPort > 65535) {
    errors.push('REDIS_PORT must be a valid port number');
  }

  if (errors.length > 0) {
    throw new Error(
      `Invalid environment configuration:\n- ${errors.join('\n- ')}`,
    );
  }

  return config;
}
