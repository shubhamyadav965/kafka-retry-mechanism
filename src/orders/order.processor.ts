import { Injectable } from '@nestjs/common';

@Injectable()
export class OrderProcessor {
  /**
   * Contains the actual business logic for processing an order.
   *
   * Consumers should not contain business logic themselves.
   * Both the normal consumer and retry consumer call this method.
   */
  process(value: string): Promise<void> {
    console.log('Processing order:', value);

    // Temporary failure simulation.
    //
    // We intentionally throw an error so that we can test
    // the retry and DLQ flow.
    //
    // Later this will be replaced with real business logic,
    // such as payment processing, inventory updates, etc.
    throw new Error('Order processing failed');
  }
}
