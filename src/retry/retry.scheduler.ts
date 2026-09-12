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
          // Atomically claim the job so that only one application
          // instance publishes it. Other schedulers that saw the
          // same due job will fail to claim it and move on.
          const claimed = await this.redisService.tryClaimRetryJob(job.jobId);

          if (!claimed) {
            console.log(`Retry job ${job.jobId} is already claimed. Skipping.`);

            continue;
          }

          console.log(`Claimed retry job ${job.jobId}`);

          console.log(`Publishing retry job to ${job.retryTopic}`);

          // Publish the retry job to Kafka.
          await this.kafkaService.publishRetryJob(job);

          // Remove the job only after Kafka publishing succeeds.
          await this.redisService.removeRetryJob(job);

          await this.redisService.releaseRetryJobClaim(job.jobId);

          console.log(`Retry job ${job.jobId} published successfully`);
        } catch (error) {
          // Keep the job in Redis if Kafka publishing fails.
          // The claim expires on its own (TTL), so another instance
          // can pick the job up on a later cycle.
          console.error(`Failed to publish retry job ${job.jobId}:`, error);
        }
      }
    } finally {
      this.isRunning = false;
    }
  }
}
