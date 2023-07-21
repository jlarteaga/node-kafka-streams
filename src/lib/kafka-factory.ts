import debugFactory from 'debug';
import { KafkaJsClient, KafkaJsClientConfig } from './client/kafka-js-client';
import { ConsumerConfig } from 'kafkajs';

const debug = debugFactory('js-kafka-streams:kafka-factory');

export type KafkaFactoryOptions = {
  driver: 'kafka-js';
  kafkaConfig: KafkaJsClientConfig;
  consumerConfig: ConsumerConfig;
};

export class KafkaFactory<C extends KafkaFactoryOptions> {
  /**
   * helper for KafkaStreams to wrap
   * the setup of Kafka-Client instances
   */
  constructor(private config: C) {}

  getKafkaClient(topic: string): KafkaJsClient {
    // TODO: Create a switch based on config.driver when more drivers are included
    debug('creating new native kafka-js client');
    const { kafkaConfig, consumerConfig } = this.config;
    return new KafkaJsClient([topic], kafkaConfig, consumerConfig);
  }
}
