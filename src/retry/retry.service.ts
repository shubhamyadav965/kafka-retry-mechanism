import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { randomUUID } from 'crypto';

import { getRetryDelay, getRetryTopic } from '../config/retry-policy';
import { RedisService } from '../redis/redis.service';
import { RetryJob } from '../redis/retry-job';
import { AppLogger } from '../common/logger/app.logger';

@Injectable()
export class RetryService {
  constructor(
    private readonly redisService: RedisService,
    private readonly configService: ConfigService,
    private readonly logger: AppLogger,
  ) {}

  /**
   * Schedule the next retry attempt.
   *
   * This method is responsible for creating a retry job
   * and storing it in Redis until it becomes due.
   */
  async scheduleRetry(
    value: string,
    retryCount: number,
    originalTopic: string,
    eventId: string,
  ): Promise<void> {
    // Determine which retry topic should receive this attempt.
    const retryTopic = getRetryTopic(
      this.configService,
      originalTopic,
      retryCount,
    );

    // Determine how long this retry should wait.
    const retryDelay = getRetryDelay(this.configService, retryCount);

    // Calculate the exact time when the retry becomes eligible.
    const scheduledRetryAt = new Date(Date.now() + retryDelay).toISOString();

    // Create the retry job that will be stored in Redis.
    const retryJob: RetryJob = {
      jobId: randomUUID(),
      eventId,
      value,
      retryCount,
      retryTopic,
      originalTopic,
      scheduledRetryAt,
    };

    try {
      // Store the job in Redis.
      await this.redisService.addRetryJob(retryJob);

      this.logger.info('retry_job_created', {
        jobId: retryJob.jobId,
        eventId: retryJob.eventId,
        retryCount: retryJob.retryCount,
        retryTopic: retryJob.retryTopic,
        scheduledRetryAt: retryJob.scheduledRetryAt,
      });
    } catch (error) {
      this.logger.error('retry_job_creation_failed', {
        eventId,
        retryCount,
        error: error instanceof Error ? error.message : String(error),
      });

      throw error;
    }
  }
}
