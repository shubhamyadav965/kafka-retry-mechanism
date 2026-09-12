import { Injectable } from '@nestjs/common';
import { RedisService } from '../redis/redis.service';

@Injectable()
export class IdempotencyService {
  constructor(private readonly redisService: RedisService) {}

  /**
   * Executes business logic only when this event
   * has not already been successfully processed.
   *
   * Returns:
   * - true  → this consumer processed the event
   * - false → the event was already processed or
   *           another consumer is processing it
   */
  async process(
    eventId: string,
    handler: () => Promise<void>,
  ): Promise<boolean> {
    // Check whether this event was successfully
    // processed previously.
    const alreadyProcessed = await this.redisService.isEventProcessed(eventId);

    if (alreadyProcessed) {
      console.log(`Event ${eventId} already processed. Skipping.`);

      return false;
    }

    // Atomically acquire the processing lock.
    const lockAcquired =
      await this.redisService.tryAcquireProcessingLock(eventId);

    if (!lockAcquired) {
      console.log(`Event ${eventId} is already being processed. Skipping.`);

      return false;
    }

    try {
      // Run the actual business logic.
      await handler();

      // Mark the event only after successful processing.
      await this.redisService.markEventProcessed(eventId);

      console.log(`Event ${eventId} marked as processed.`);

      return true;
    } finally {
      // Always release the temporary processing lock.
      await this.redisService.releaseProcessingLock(eventId);
    }
  }
}
