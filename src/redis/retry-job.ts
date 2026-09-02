export interface RetryJob {
  // The original Kafka message payload.
  value: string;

  // Which retry attempt this job represents.
  retryCount: number;

  // The Kafka topic where this retry should eventually be published.
  retryTopic: string;

  // The original Kafka topic that produced the failure.
  originalTopic: string;

  // The time at which this retry becomes eligible.
  scheduledRetryAt: string;
}
