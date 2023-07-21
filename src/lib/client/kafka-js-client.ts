import {
  Consumer,
  ConsumerConfig,
  IHeaders,
  Kafka,
  KafkaConfig,
  Producer,
  ProducerConfig
} from 'kafkajs';
import { KafkaClient } from './kafka-client';
import { EventEmitter } from 'node:events';
import { NOOP } from '../utils';
import debugFactory from 'debug';

const debug = debugFactory('js-kafka-streams:kafka-js-client');

export type ErrorCallback = (error: unknown) => void;

export type KafkaJsClientConfig = KafkaConfig;

export class KafkaJsClient extends KafkaClient {
  public static readonly events = Object.freeze({
    PRODUCER_READY: 'kafka-producer-ready'
  });

  private kafka: Kafka;
  private consumer?: Consumer;
  private producer?: Producer;
  private produceTopic?: string;
  private producePartitionCount: number;
  private produceHandler?: EventEmitter;

  /**
   * KafkaJsClient (EventEmitter)
   * that wraps an internal instance of a
   * KafkaJs- Consumer and/or Producer
   */
  constructor(
    private topics: string[],
    private kafkaConfig: KafkaConfig,
    private consumerConfig: ConsumerConfig
  ) {
    super();
    this.producePartitionCount = 1;
    this.kafka = new Kafka(this.kafkaConfig);
  }

  /**
   * sets a handler for produce messages
   * (emits whenever kafka messages are produced/delivered)
   * @param handler {EventEmitter}
   */
  setProduceHandler(handler: EventEmitter) {
    this.produceHandler = handler;
  }

  /**
   * returns the produce handler instance if present
   * @returns {null|EventEmitter}
   */
  getProduceHandler() {
    return this.produceHandler;
  }

  overwriteTopics(topics: string[]) {
    this.topics = topics;
  }

  // TODO: Check later
  // adjustDefaultPartitionCount(partitionCount = 1) {
  //   this.producePartitionCount = partitionCount;
  //   this.producer.defaultPartitionCount = partitionCount;
  // }

  /**
   * starts a new kafka consumer
   * will await a kafka-producer-ready-event if started withProducer=true
   */
  start(
    readyCallback = NOOP,
    kafkaErrorCallback: ErrorCallback = NOOP,
    withProducer = false
  ) {
    //might be possible if the parent stream is build to produce messages only
    if (!this.topics || !this.topics.length) {
      return;
    }

    kafkaErrorCallback = kafkaErrorCallback || NOOP;

    this.consumer = this.kafka.consumer(this.consumerConfig);
    this.consumer.on(this.consumer.events.CONNECT, readyCallback);
    this.consumer.on(this.consumer.events.CRASH, kafkaErrorCallback);

    //consumer has to wait for producer
    super.once(KafkaJsClient.events.PRODUCER_READY, () => {
      const streamOptions = {
        asString: false,
        asJSON: false
      };

      this.consumer
        ?.connect()
        .then(
          () =>
            this.consumer?.subscribe({
              topics: this.topics
            })
        )
        .then(() => {
          debug('consumer ready');
          this.consumer?.run({
            eachMessage: async ({ message }) => {
              this.emit('message', message);
            }
          });
        })
        .catch(kafkaErrorCallback);
    });

    if (!withProducer) {
      super.emit('kafka-producer-ready', true);
    }
  }

  /**
   * starts a new kafka-producer
   * will fire kafka-producer-ready-event
   * requires a topic's partition count during initialisation
   */
  setupProducer(
    produceTopic: string,
    partitions = 1,
    readyCallback = NOOP,
    kafkaErrorCallback: ErrorCallback = NOOP,
    outputKafkaConfig: ProducerConfig
  ) {
    this.produceTopic = produceTopic || this.produceTopic;
    this.producePartitionCount = partitions;

    //might be possible if the parent stream is build to produce messages only
    if (!this.producer) {
      this.producer = this.kafka.producer(outputKafkaConfig);

      //consumer is awaiting producer
      this.producer.on('producer.connect', () => {
        debug('producer ready');
        super.emit('kafka-producer-ready', true);
        if (readyCallback) {
          readyCallback();
        }
      });
      this.producer.connect().catch((e) => kafkaErrorCallback(e));
    }
  }

  /**
   * simply produces a message or multiple on a topic
   * if producerPartitionCount is > 1 it will randomize
   * the target partition for the message/s
   */
  send(
    topic: string,
    message: string | Buffer | null,
    partition?: number,
    key?: string,
    headers?: IHeaders
  ) {
    if (!this.producer) {
      return Promise.reject('producer is not yet setup.');
    }

    return this.producer.send({
      topic,
      messages: [
        {
          key,
          headers,
          partition,
          value: message
        }
      ]
    });
  }

  // TODO: Check later
  // pause() {
  //   //no consumer pause
  //
  //   if (this.producer) {
  //     this.producer.pause();
  //   }
  // }

  // TODO: Check later
  // resume() {
  //   //no consumer resume
  //
  //   if (this.producer) {
  //     this.producer.resume();
  //   }
  // }

  getStats() {
    return {
      inTopic: this.topics ?? null,
      outTopic: this.produceTopic ?? null
    };
  }

  close(commit = false) {
    this.consumer?.disconnect();
    this.producer?.disconnect();
  }

  //required by KTable
  closeConsumer(commit = false) {
    this.consumer?.disconnect();
    this.consumer = undefined;
  }
}
