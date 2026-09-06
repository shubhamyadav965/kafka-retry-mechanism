import { Injectable } from '@nestjs/common';

import { getRetryDelay, getRetryTopic } from '../config/retry-policy';
import { RedisService } from '../redis/redis.service';
import { RetryJob } from '../redis/retry-job';

@Injectable()
export class RetryService {
  constructor(private readonly redisService: RedisService) {}

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
    // Find the Kafka topic for this retry attempt.
    const retryTopic = getRetryTopic(retryCount);

    // Find how long we should wait before executing this retry.
    const retryTopic = getRetryTopic(
        originalTopic,
        retryCount,
      );

    // Calculate the exact time when the retry becomes eligible.
    const scheduledRetryAt = new Date(Date.now() + retryDelay).toISOString();

    // Create the retry job that will be stored in Redis.
    const retryJob: RetryJob = {
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
