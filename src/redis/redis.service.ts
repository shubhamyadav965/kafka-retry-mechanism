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

  constructor(private readonly configService: ConfigService) {
    this.redis = new Redis({
      host: this.configService.get<string>('REDIS_HOST'),
      port: this.configService.get<number>('REDIS_PORT'),
    });
  }

  // Add a retry job to Redis.
  // The score determines when the job becomes eligible.
  async addRetryJob(job: RetryJob): Promise<void> {
    const score = new Date(job.scheduledRetryAt).getTime();

    await this.redis.zadd(this.retryQueue, score, JSON.stringify(job));
  }

  // Get retry jobs whose scheduled time has arrived. -inf means "no lower limit". Date.now() means "up to right now".
  async getDueRetryJobs(): Promise<RetryJob[]> {
    const jobs = await this.redis.zrangebyscore(
      this.retryQueue,
      '-inf',
      Date.now(),
    );

    return jobs.map((job) => JSON.parse(job) as RetryJob);
  }

  // Remove a retry job after it has been successfully published back to Kafka.
  async removeRetryJob(job: RetryJob): Promise<void> {
    await this.redis.zrem(this.retryQueue, JSON.stringify(job));
  }

  // Close the Redis connection when NestJS shuts down.
  async onModuleDestroy(): Promise<void> {
    await this.redis.quit();
  }
}
