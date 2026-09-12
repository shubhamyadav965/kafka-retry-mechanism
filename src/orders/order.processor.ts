import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class OrderProcessor {
  constructor(private readonly configService: ConfigService) {}

  /**
   * Contains the actual business logic for processing an order.
   *
   * Consumers should not contain business logic themselves.
   * Both the normal consumer and retry consumer call this method.
   */
  process(value: string): Promise<void> {
    console.log('Processing order:', value);

    // Test switch so we can exercise either the retry/DLQ flow
    // or the successful-processing (idempotency) flow.
    const shouldFail =
      this.configService.get<string>('ORDER_PROCESSOR_SHOULD_FAIL') === 'true';

    if (shouldFail) {
      throw new Error('Order processing failed');
    }

    console.log('Order processing succeeded');

    return Promise.resolve();
  }
}
