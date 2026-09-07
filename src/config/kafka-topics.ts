import { ConfigService } from '@nestjs/config';
import { getDlqTopic, getRetryTopics } from './retry-policy';

/**
 * Returns every Kafka topic the framework needs to exist:
 * each configured original topic, its tiered retry topics,
 * and its DLQ topic.
 *
 * Example:
 * orders →
 * [ 'orders', 'orders.retry.1m', 'orders.retry.5m',
 *   'orders.retry.10m', 'orders.dlq' ]
 */
export function getRequiredTopics(configService: ConfigService): string[] {
  const originalTopics =
    configService
      .get<string>('KAFKA_ORIGINAL_TOPICS')
      ?.split(',')
      .map((topic) => topic.trim())
      .filter(Boolean) ?? [];

  return originalTopics.flatMap((topic) => [
    topic,
    ...getRetryTopics(configService, topic),
    getDlqTopic(configService, topic),
  ]);
}
