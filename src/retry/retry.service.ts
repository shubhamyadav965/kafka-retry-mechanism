import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { randomUUID } from 'crypto';

import { getRetryDelay, getRetryTopic } from '../config/retry-policy';
import { RedisService } from '../redis/redis.service';
import { RetryJob } from '../redis/retry-job';

@Injectable()
export class RetryService {
  constructor(
    private readonly redisService: RedisService,
    private readonly configService: ConfigService,
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
      value,
      retryCount,
      retryTopic,
      originalTopic,
      scheduledRetryAt,
    };

    // Store the job in Redis.
    await this.redisService.addRetryJob(retryJob);
  }
}
