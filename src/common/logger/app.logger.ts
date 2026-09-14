import { Injectable } from '@nestjs/common';

@Injectable()
export class AppLogger {
  info(event: string, data: Record<string, unknown> = {}): void {
    console.log(
      JSON.stringify({
        level: 'info',
        event,
        timestamp: new Date().toISOString(),
        ...data,
      }),
    );
  }

  error(event: string, data: Record<string, unknown> = {}): void {
    console.error(
      JSON.stringify({
        level: 'error',
        event,
        timestamp: new Date().toISOString(),
        ...data,
      }),
    );
  }
}
