import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

import { Consumer, Kafka } from 'kafkajs';

import { KafkaService } from './kafka.service';
import { OrderProcessor } from '../orders/order.processor';
import { RetryService } from '../retry/retry.service';
import { IdempotencyService } from '../idempotency/idempotency.service';
import { AppLogger } from '../common/logger/app.logger';
import { MetricsService } from '../common/metrics/metrics.service';

@Injectable()
export class KafkaConsumer implements OnModuleInit, OnModuleDestroy {
  // Kafka client used by this consumer.
  private readonly kafka: Kafka;

  // Consumer reads messages from all configured original topics.
  private readonly consumer: Consumer;

  constructor(
    private readonly kafkaService: KafkaService,
    private readonly orderProcessor: OrderProcessor,
    private readonly configService: ConfigService,
    private readonly retryService: RetryService,
    private readonly idempotencyService: IdempotencyService,
    private readonly logger: AppLogger,
    private readonly metricsService: MetricsService,
  ) {
    this.kafka = new Kafka({
      // Identifier for this Kafka client.
      clientId: 'order-consumer',

      // Kafka broker address.
      brokers: ['kafka:9093'],
    });

    this.consumer = this.kafka.consumer({
      // Consumers with the same groupId share the work
      // and maintain a shared committed offset.
      groupId: 'order-consumer-group',
    });
  }

  async onModuleInit() {
    // Ensure required topics exist before subscribing, regardless
    // of provider initialization order.
    await this.kafkaService.initializeTopics();

    // Connect the consumer to Kafka.
    await this.consumer.connect();

    // Which original topics this consumer should handle, per config.
    const originalTopics =
      this.configService
        .get<string>('KAFKA_ORIGINAL_TOPICS')
        ?.split(',')
        .map((topic) => topic.trim())
        .filter(Boolean) ?? [];

    // Subscribe this consumer to every configured original topic.
    await this.consumer.subscribe({
      topics: originalTopics,

      // Start from the beginning when this consumer group
      // does not already have a committed offset.
      fromBeginning: true,
    });

    await this.consumer.run({
      // eachMessage is called whenever Kafka delivers
      // a message to this consumer.
      eachMessage: async ({ topic, message }) => {
        // Kafka message values arrive as Buffers.
        // Convert the value into a string.
        const value = message.value?.toString();

        let eventId: string | undefined;

        try {
          const event = JSON.parse(value ?? '{}') as { event_id?: string };
          eventId = event.event_id;
        } catch {
          this.logger.error('invalid_json_message', {
            topic,
          });
        }

        if (!eventId) {
          this.logger.error('message_missing_event_id', {
            topic,
          });
          return;
        }

        this.logger.info('message_received', {
          topic,
          eventId,
        });

        this.metricsService.increment('messages_received');

        try {
          // Business logic is kept outside the consumer.
          // This makes the same processor reusable by
          // both the main and retry consumers.
          //
          // IdempotencyService guarantees the handler runs at most
          // once per eventId, even if Kafka redelivers the message.
          const processed = await this.idempotencyService.process(
            eventId,
            async () => {
              await this.orderProcessor.process(value ?? '');
            },
          );

          if (!processed) {
            this.logger.info('duplicate_event_skipped', {
              topic,
              eventId,
            });

            this.metricsService.increment('duplicate_events');

            return;
          }

          this.logger.info('event_processed_successfully', {
            topic,
            eventId,
          });

          this.metricsService.increment('messages_processed');
        } catch {
          this.logger.error('event_processing_failed', {
            topic,
            eventId,
          });

          this.metricsService.increment('messages_failed');

          // First failure from the main topic becomes retry #1.
          const retryCount = 1;

          await this.retryService.scheduleRetry(
            value ?? '',
            retryCount,
            topic,
            eventId,
          );

          this.metricsService.increment('retries_scheduled');

          this.logger.info('retry_scheduled', {
            topic,
            eventId,
            retryCount,
          });
        }
      },
    });

    this.logger.info('kafka_consumer_connected', {
      consumer: 'order-consumer',
    });
  }

  async onModuleDestroy() {
    // Gracefully leave the consumer group and close
    // the Kafka connection when the application shuts down.
    await this.consumer.disconnect();

    this.logger.info('kafka_consumer_disconnected', {
      consumer: 'order-consumer',
    });
  }
}
