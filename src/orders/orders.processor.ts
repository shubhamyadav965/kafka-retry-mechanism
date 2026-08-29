import { Injectable } from '@nestjs/common';

@Injectable()
export class OrderProcessor {
  async process(value: string): Promise<void> {
    console.log('Processing order:', value);

    // Temporary business logic
    // We will replace this with real order processing later.

    throw new Error('Order processing failed');
  }
}
