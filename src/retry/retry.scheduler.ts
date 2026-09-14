import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';

import { KafkaService } from '../kafka/kafka.service';
import { RedisService } from '../redis/redis.service';
import { AppLogger } from '../common/logger/app.logger';

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
    private readonly logger: AppLogger,
  ) {}

  async onModuleInit() {
    this.logger.info('retry_scheduler_started');

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

    this.logger.info('retry_scheduler_stopped');
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

      this.logger.info('retry_jobs_found', { count: jobs.length });

      for (const job of jobs) {
        try {
          // Atomically claim the job so that only one application
          // instance publishes it. Other schedulers that saw the
          // same due job will fail to claim it and move on.
          const claimed = await this.redisService.tryClaimRetryJob(job.jobId);

          if (!claimed) {
            this.logger.info('retry_job_claim_failed', {
              jobId: job.jobId,
            });

            continue;
          }

          this.logger.info('retry_job_claimed', { jobId: job.jobId });

          this.logger.info('retry_job_publishing', {
            jobId: job.jobId,
            retryTopic: job.retryTopic,
            retryCount: job.retryCount,
          });

          // Publish the retry job to Kafka.
          await this.kafkaService.publishRetryJob(job);

          // Remove the job only after Kafka publishing succeeds.
          await this.redisService.removeRetryJob(job);

          this.logger.info('retry_job_published', {
            jobId: job.jobId,
            retryTopic: job.retryTopic,
          });
        } catch (error) {
          // Keep the job in Redis if Kafka publishing fails.
          // The claim expires on its own (TTL), so another instance
          // can pick the job up on a later cycle.
          this.logger.error('retry_job_publish_failed', {
            jobId: job.jobId,
            retryTopic: job.retryTopic,
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }
    } finally {
      this.isRunning = false;
    }
  }
}
