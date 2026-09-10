import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import Redis from 'ioredis';
import { RetryJob } from './retry-job';

@Injectable()
export class RedisService implements OnModuleDestroy {
  private readonly redis: Redis;

  // Redis Sorted Set where all scheduled retry jobs are stored. The score will be the retry timestamp.
  // The member will contain the retry job as JSON.
  private readonly retryQueue = 'retry:scheduled';

  // Prevents two consumers from processing the same event at the same time.
  private readonly processingLockPrefix = 'idempotency:processing:';

  // Stores events that have completed successfully.
  private readonly processedEventPrefix = 'idempotency:processed:';

  // How long a processing lock should remain alive.
  // The lock automatically disappears if the application crashes.
  private readonly processingLockTtlSeconds = 300;

  // How long we remember that an event was successfully processed.
  private readonly processedEventTtlSeconds = 86400;

  constructor(private readonly configService: ConfigService) {
    this.redis = new Redis({
      host: this.configService.get<string>('REDIS_HOST'),
      port: this.configService.get<number>('REDIS_PORT'),
    });
  }

  // Add a retry job to Redis.
  // The score determines when the job becomes eligible.
  // The ZSET member is just the jobId; the job payload itself is
  // stored separately so ZSET members stay unique per job.
  async addRetryJob(job: RetryJob): Promise<void> {
    const score = new Date(job.scheduledRetryAt).getTime();

    await this.redis.set(`retry:job:${job.jobId}`, JSON.stringify(job));

    await this.redis.zadd(this.retryQueue, score, job.jobId);
  }

  // Get retry jobs whose scheduled time has arrived. -inf means "no lower limit". Date.now() means "up to right now".
  async getDueRetryJobs(): Promise<RetryJob[]> {
    const jobIds = await this.redis.zrangebyscore(
      this.retryQueue,
      '-inf',
      Date.now(),
    );

    if (jobIds.length === 0) {
      return [];
    }

    // Fetch all job payloads together instead of one request per job.
    const jobs = await this.redis.mget(
      ...jobIds.map((jobId) => `retry:job:${jobId}`),
    );

    return jobs
      .filter((job): job is string => job !== null)
      .map((job) => JSON.parse(job) as RetryJob);
  }

  // Remove a retry job after it has been successfully published back to Kafka.
  async removeRetryJob(job: RetryJob): Promise<void> {
    await this.redis.zrem(this.retryQueue, job.jobId);

    await this.redis.del(`retry:job:${job.jobId}`);
  }

  /**
   * Atomically claims an event for processing.
   *
   * NX means Redis creates the key only if it does not
   * already exist.
   *
   * This prevents two consumers from processing the
   * same event simultaneously.
   */
  async tryAcquireProcessingLock(eventId: string): Promise<boolean> {
    const key = `${this.processingLockPrefix}${eventId}`;

    const result = await this.redis.set(
      key,
      '1',
      'EX',
      this.processingLockTtlSeconds,
      'NX',
    );

    return result === 'OK';
  }

  /**
   * Checks whether an event has already completed successfully.
   */
  async isEventProcessed(eventId: string): Promise<boolean> {
    const key = `${this.processedEventPrefix}${eventId}`;

    const exists = await this.redis.exists(key);

    return exists === 1;
  }

  /**
   * Marks an event as successfully processed.
   */
  async markEventProcessed(eventId: string): Promise<void> {
    const key = `${this.processedEventPrefix}${eventId}`;

    await this.redis.set(key, '1', 'EX', this.processedEventTtlSeconds);
  }

  /**
   * Releases the processing lock.
   *
   * This is important when processing fails so that
   * a later retry can process the event again.
   */
  async releaseProcessingLock(eventId: string): Promise<void> {
    const key = `${this.processingLockPrefix}${eventId}`;

    await this.redis.del(key);
  }

  // Close the Redis connection when NestJS shuts down.
  async onModuleDestroy(): Promise<void> {
    await this.redis.quit();
  }
}
