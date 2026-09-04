import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';

import { KafkaService } from '../kafka/kafka.service';
import { RedisService } from '../redis/redis.service';

@Injectable()
export class RetryScheduler implements OnModuleInit, OnModuleDestroy {
  // Timer used to periodically check Redis.
  private interval?: NodeJS.Timeout;

  // Prevent multiple scheduler runs from happening
  // at the same time.
  private isRunning = false;

  constructor(
    private readonly redisService: RedisService,
    private readonly kafkaService: KafkaService,
  ) {}

  async onModuleInit() {
    console.log('Retry scheduler started');

    // Check for due retry jobs immediately when
    // the application starts.
    await this.processDueRetries();

    // Check Redis every second.
    this.interval = setInterval(() => {
      void this.processDueRetries();
    }, 1000);
  }

  onModuleDestroy() {
    // Stop the scheduler when NestJS shuts down.
    if (this.interval) {
      clearInterval(this.interval);
    }

    console.log('Retry scheduler stopped');
  }

  /**
   * Find retry jobs whose scheduled time has arrived
   * and publish them back to Kafka.
   */
  private async processDueRetries(): Promise<void> {
    // Do not start another run while the previous
    // run is still processing jobs.
    if (this.isRunning) {
      return;
    }

    this.isRunning = true;

    try {
      // Get retry jobs whose scheduled time is <= now.
      const jobs = await this.redisService.getDueRetryJobs();

      if (jobs.length === 0) {
        return;
      }

      console.log(`Found ${jobs.length} due retry job(s)`);

      for (const job of jobs) {
        try {
          console.log(`Publishing retry job to ${job.retryTopic}`);

          // Publish the retry job to Kafka.
          await this.kafkaService.publishRetryJob(job);

          // Remove the job only after Kafka publishing succeeds.
          await this.redisService.removeRetryJob(job);

          console.log('Retry job published successfully');
        } catch (error) {
          // Keep the job in Redis if Kafka publishing fails.
          // The next scheduler cycle will try again.
          console.error('Failed to publish retry job:', error);
        }
      }
    } finally {
      this.isRunning = false;
    }
  }
}
