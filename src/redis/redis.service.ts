import { Injectable, OnModuleDestroy } from '@nestjs/common';
import Redis from 'ioredis';

@Injectable()
export class RedisService implements OnModuleDestroy {
  // Create a Redis client.
  //
  // NestJS and Redis are both running inside Docker Compose.
  //
  // Docker Compose gives the Redis container the service name
  // "redis", so NestJS can connect using redis:6379.
  private readonly redis = new Redis({
    host: 'redis',
    port: 6379,
  });

  // Store a value in Redis using a key.
  //
  // We will later use Redis Sorted Sets for retry scheduling.
  // This simple method is only for testing our Redis connection.
  async set(key: string, value: string): Promise<void> {
    await this.redis.set(key, value);
  }

  // Read a value from Redis using its key.
  async get(key: string): Promise<string | null> {
    return this.redis.get(key);
  }

  // Cleanly close the Redis connection when
  // the NestJS application shuts down.
  async onModuleDestroy(): Promise<void> {
    await this.redis.quit();
  }
}
