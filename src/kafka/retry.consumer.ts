import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Consumer, Kafka } from 'kafkajs';
import { OrderProcessor } from '../orders/order.processor';
import { KafkaService } from './kafka.service';
import { RetryService } from '../retry/retry.service';
import { getRetryTopics } from '../config/retry-policy';
import { IdempotencyService } from '../idempotency/idempotency.service';
import { AppLogger } from '../common/logger/app.logger';
import { MetricsService } from '../common/metrics/metrics.service';

@Injectable()
export class RetryConsumer implements OnModuleInit, OnModuleDestroy {
  // Kafka client used by the retry consumer.
  private readonly kafka: Kafka;

  // Consumer responsible for reading failed messages
  // from the retry topic.
  private readonly consumer: Consumer;

  constructor(
    private readonly orderProcessor: OrderProcessor,
    private readonly kafkaService: KafkaService,
    private readonly retryService: RetryService,
    private readonly configService: ConfigService,
    private readonly idempotencyService: IdempotencyService,
    private readonly logger: AppLogger,
    private readonly metricsService: MetricsService,
  ) {
    this.kafka = new Kafka({
      clientId: 'order-retry-consumer',
      brokers: ['kafka:9093'],
    });

    this.consumer = this.kafka.consumer({
      // Separate consumer group because retry messages
      // are processed independently from normal orders.
      groupId: 'order-retry-consumer-group',
    });
  }

  async onModuleInit() {
    // Ensure required topics exist before subscribing, regardless
    // of provider initialization order.
    await this.kafkaService.initializeTopics();

    // Connect retry consumer to Kafka.
    await this.consumer.connect();

    // Which original topics have retry-enabled producers, per config.
    const originalTopics =
      this.configService
        .get<string>('KAFKA_ORIGINAL_TOPICS')
        ?.split(',')
        .map((topic) => topic.trim())
        .filter(Boolean) ?? [];

    // Expand each original topic into its tiered retry topics
    // (e.g. orders → orders.retry.1m, orders.retry.5m, orders.retry.10m).
    const retryTopics = originalTopics.flatMap((topic) =>
      getRetryTopics(this.configService, topic),
    );

    await this.consumer.subscribe({
      topics: retryTopics,

      // Useful during development so we can replay
      // messages already present in the topic.
      fromBeginning: true,
    });

    await this.consumer.run({
      eachMessage: async ({ message }) => {
        // Convert Kafka message value from Buffer to string.
        const value = message.value?.toString();

        // Raw header reads. Every field here is written by our own
        // KafkaService.publishRetryJob(), so a missing/malformed one on
        // a message read from a retry topic means either a foreign
        // producer wrote to the topic or the message is corrupt —
        // reject it rather than guessing at defaults.
        const retryCountHeader = message.headers?.retry_count?.toString();
        const maxRetriesHeader = message.headers?.max_retries?.toString();
        const originalTopicHeader = message.headers?.original_topic?.toString();
        const jobId = message.headers?.job_id?.toString();
        const eventId = message.headers?.event_id?.toString();
        const scheduledRetryAt =
          message.headers?.scheduled_retry_at?.toString();

        const retryCount = Number(retryCountHeader);
        const maxRetries = Number(maxRetriesHeader);
        const scheduledRetryTime = scheduledRetryAt
          ? new Date(scheduledRetryAt).getTime()
          : NaN;

        const missingHeaders = [
          !retryCountHeader && 'retry_count',
          !maxRetriesHeader && 'max_retries',
          !originalTopicHeader && 'original_topic',
          !jobId && 'job_id',
          !eventId && 'event_id',
          !scheduledRetryAt && 'scheduled_retry_at',
        ].filter((header): header is string => Boolean(header));

        const headersInvalid =
          missingHeaders.length > 0 ||
          !Number.isFinite(retryCount) ||
          !Number.isFinite(maxRetries) ||
          Number.isNaN(scheduledRetryTime);

        if (headersInvalid) {
          this.logger.error('invalid_retry_message_headers', {
            missingHeaders,
            retryCount: retryCountHeader,
            maxRetries: maxRetriesHeader,
            originalTopic: originalTopicHeader,
            jobId,
            eventId,
            scheduledRetryAt,
            offset: message.offset,
          });

          return;
        }

        const originalTopic = originalTopicHeader as string;

        this.logger.info('retry_message_received', {
          jobId,
          eventId,
          retryCount,
          maxRetries,
          originalTopic,
          offset: message.offset,
        });

        this.metricsService.increment('retries_processed');

        const latencyMs = Date.now() - scheduledRetryTime;

        if (latencyMs >= 0) {
          this.metricsService.recordRetryLatency(latencyMs);
        }

        try {
          if (!eventId) {
            throw new Error('Retry message is missing event_id');
          }

          // Try processing the failed message again, guarded so the
          // handler runs at most once per eventId even if Kafka
          // redelivers this retry message.
          const processed = await this.idempotencyService.process(
            eventId,
            async () => {
              await this.orderProcessor.process(value ?? '');
            },
          );

          if (!processed) {
            this.logger.info('duplicate_retry_event_skipped', {
              jobId,
              eventId,
              retryCount,
            });

            this.metricsService.increment('duplicate_events');

            return;
          }

          this.logger.info('retry_processing_succeeded', {
            jobId,
            eventId,
            retryCount,
          });
        } catch (error) {
          this.logger.error('retry_processing_failed', {
            jobId,
            eventId,
            retryCount,
            maxRetries,
          });

          // If we have not reached the maximum retry count,
          // create another retry message.
          if (retryCount < maxRetries) {
            const nextRetryCount = retryCount + 1;

            this.logger.info('retry_scheduling_next_attempt', {
              jobId,
              eventId,
              currentRetryCount: retryCount,
              nextRetryCount,
              maxRetries,
            });

            await this.retryService.scheduleRetry(
              value ?? '',
              nextRetryCount,
              originalTopic,
              eventId ?? '',
            );

            this.metricsService.increment('retries_scheduled');

            this.logger.info('retry_job_scheduled', {
              jobId,
              eventId,
              retryCount: nextRetryCount,
              originalTopic,
            });
          } else {
            // Maximum retries have been exhausted.
            // Send the message to the Dead Letter Queue.
            this.logger.error('maximum_retries_reached', {
              jobId,
              eventId,
              retryCount,
              maxRetries,
              originalTopic,
            });

            await this.kafkaService.sendToDlq(
              value ?? '',
              retryCount,
              error instanceof Error ? error.message : 'Unknown error',
              originalTopic,
            );

            this.metricsService.increment('dlq_messages');
          }
        }
      },
    });

    this.logger.info('retry_consumer_connected', {
      consumer: 'order-retry-consumer',
    });
  }

  async onModuleDestroy() {
    // Gracefully disconnect from Kafka when NestJS shuts down.
    await this.consumer.disconnect();

    this.logger.info('retry_consumer_disconnected', {
      consumer: 'order-retry-consumer',
    });
  }
}
