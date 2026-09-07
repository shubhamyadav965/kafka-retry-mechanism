import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Admin, Kafka, Producer } from 'kafkajs';
import {
  getRetryDelay,
  getMaxRetries,
  getDlqTopic,
} from '../config/retry-policy';
import { getRequiredTopics } from '../config/kafka-topics';
import { RetryJob } from '../redis/retry-job';

@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  // Kafka client used to communicate with our Kafka broker.
  private readonly kafka: Kafka;

  // Producer is responsible for publishing messages to Kafka topics.
  private readonly producer: Producer;

  // Admin client is responsible for managing topics.
  private readonly admin: Admin;

  // Guards against re-running topic creation more than once.
  private topicsInitialized = false;

  // Tracks an in-flight initialization so concurrent callers
  // await the same operation instead of starting another one.
  private topicsInitializationPromise?: Promise<void>;

  constructor(private readonly configService: ConfigService) {
    const brokers =
      this.configService
        .get<string>('KAFKA_BROKERS')
        ?.split(',')
        .map((broker) => broker.trim())
        .filter(Boolean) ?? [];

    this.kafka = new Kafka({
      // Identifies this application when communicating with Kafka.
      clientId: 'kafka-retry-framework',

      brokers,
    });

    // Create the Kafka producer.
    this.producer = this.kafka.producer();

    this.admin = this.kafka.admin();
  }

  async onModuleInit() {
    await this.initializeTopics();

    // Establish connection with Kafka when NestJS starts.
    await this.producer.connect();

    console.log('Kafka producer connected');
  }

  async onModuleDestroy() {
    // Gracefully close the Kafka connection when NestJS shuts down.
    await this.producer.disconnect();

    console.log('Kafka producer disconnected');
  }

  /**
   * Ensures every topic the framework needs (original topics,
   * their tiered retry topics, and their DLQ topics) exists.
   *
   * Safe to call from multiple providers concurrently: only one
   * initialization runs at a time, and repeat calls after success
   * are no-ops.
   */
  async initializeTopics(): Promise<void> {
    // If topics were already initialized, there is nothing to do.
    if (this.topicsInitialized) {
      return;
    }

    // If another part of the application is already initializing
    // the topics, wait for that same operation instead of starting
    // another one.
    if (this.topicsInitializationPromise) {
      return this.topicsInitializationPromise;
    }

    this.topicsInitializationPromise = this.createRequiredTopics();

    try {
      await this.topicsInitializationPromise;
    } finally {
      this.topicsInitializationPromise = undefined;
    }
  }

  private async createRequiredTopics(): Promise<void> {
    const topics = getRequiredTopics(this.configService);

    await this.admin.connect();

    try {
      // Get the topics that already exist in Kafka.
      const existingTopics = await this.admin.listTopics();

      // Only ask Kafka to create topics that don't exist.
      const missingTopics = topics.filter(
        (topic) => !existingTopics.includes(topic),
      );

      if (missingTopics.length === 0) {
        console.log('All Kafka topics already exist');
        this.topicsInitialized = true;
        return;
      }

      await this.admin.createTopics({
        topics: missingTopics.map((topic) => ({
          topic,
          numPartitions: 1,
          replicationFactor: 1,
        })),
        waitForLeaders: true,
      });

      this.topicsInitialized = true;

      console.log('Created Kafka topics:', missingTopics);
    } finally {
      await this.admin.disconnect();
    }
  }

  /**
   * Publish a message to an arbitrary topic.
   *
   * Used by the API layer (e.g. POST /orders) to publish new
   * business events onto the main orders topic.
   */
  async sendMessage(topic: string, message: unknown) {
    await this.producer.send({
      topic,
      messages: [
        {
          // Kafka payload is bytes, not objects.
          value: JSON.stringify(message),
        },
      ],
    });
  }

  /**
   * Publish a failed message to the retry topic.
   *
   * We don't physically move the original Kafka message.
   * Instead, we create a new Kafka message in orders.retry.
   */
  async sendToRetryTopic(
    value: string,
    retryCount: number,
    retryTopic: string,
    originalTopic: string,
  ) {
    const retryDelay = getRetryDelay(this.configService, retryCount);
    const scheduledRetryAt = new Date(Date.now() + retryDelay).toISOString();

    await this.producer.send({
      topic: retryTopic,
      messages: [
        {
          value,
          headers: {
            retry_count: retryCount.toString(),
            max_retries: getMaxRetries(this.configService).toString(),
            original_topic: originalTopic,
            scheduled_retry_at: scheduledRetryAt,
          },
        },
      ],
    });
  }

  //Publish a scheduled retry job back to Kafka. The RetryScheduler calls this method when the retry becomes due in Redis.
  async publishRetryJob(job: RetryJob): Promise<void> {
    await this.producer.send({
      topic: job.retryTopic,
      messages: [
        {
          // Send the original business message unchanged.
          value: job.value,
          // Restore retry metadata as Kafka headers.
          headers: {
            job_id: job.jobId,
            retry_count: job.retryCount.toString(),
            max_retries: getMaxRetries(this.configService).toString(),
            original_topic: job.originalTopic,
            scheduled_retry_at: job.scheduledRetryAt,
          },
        },
      ],
    });
  }

  /**
   * Send a message to the Dead Letter Queue (DLQ)
   * after all retry attempts have been exhausted.
   */
  async sendToDlq(
    value: string,
    retryCount: number,
    errorMessage: string,
    originalTopic: string,
  ) {
    await this.producer.send({
      topic: getDlqTopic(this.configService, originalTopic),
      messages: [
        {
          value,
          headers: {
            retry_count: retryCount.toString(),
            original_topic: originalTopic,
            error_message: errorMessage,
          },
        },
      ],
    });
  }
}
