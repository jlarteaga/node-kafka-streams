import { EventEmitter } from 'events';

export class KafkaClient extends EventEmitter {
  constructor() {
    super();
  }

  static getRandomIntInclusive(min: number, max: number) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min + 1)) + min;
  }
}
