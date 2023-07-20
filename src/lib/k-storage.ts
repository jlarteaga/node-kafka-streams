import { Observer, SubscriptionLike } from 'rxjs';

type KStorageKey = string;

export type KStorageState<T> = {
  [key: KStorageKey]: T;
};

export interface KStorageEntry<T> {
  key: KStorageKey;
  value: T;
}

export class KStorage<KSV> implements Observer<KStorageEntry<KSV>> {
  private state: KStorageState<KSV>;
  private subscription?: SubscriptionLike;

  /**
   * be aware that even though KStorage is built on Promises
   * its operations must always be ATOMIC (or ACID) because
   * the stream will access them parallel, therefore having
   * an async get + async set operation will always yield
   * in a large amount of missing get operations followed by
   * set operations
   */
  constructor() {
    this.state = {};
  }

  set(key: KStorageKey, value: KSV) {
    this.state[key] = value;
    return Promise.resolve(value);
  }

  setSmaller(key: KStorageKey = 'min', value: KSV) {
    if (!this.state[key]) {
      this.state[key] = value;
    }

    if (value < this.state[key]) {
      this.state[key] = value;
    }

    return Promise.resolve(this.state[key]);
  }

  setGreatest(key: KStorageKey = 'max', value: KSV) {
    if (!this.state[key]) {
      this.state[key] = value;
    }

    if (value > this.state[key]) {
      this.state[key] = value;
    }

    return Promise.resolve(this.state[key]);
  }

  increment(key: KStorageKey, by: number = 1) {
    const initialValue = this.state[key];
    if (
      typeof initialValue !== 'undefined' &&
      typeof initialValue !== 'number'
    ) {
      console.warn(`${initialValue} is not a number and cannot be incremented`);
      return Promise.resolve(undefined);
    }
    this.state[key] = ((initialValue ?? 0) + by) as KSV;
    return Promise.resolve(this.state[key]);
  }

  sum(key: KStorageKey, value: KSV) {
    if (typeof value !== 'number') {
      return Promise.resolve(undefined);
    }
    return this.increment(key, value);
  }

  get(key: KStorageKey) {
    return Promise.resolve(this.state[key]);
  }

  getState() {
    return Promise.resolve(this.state);
  }

  setState(newState: KStorageState<KSV>) {
    this.state = newState;
    return Promise.resolve(true);
  }

  start(subscription: SubscriptionLike) {
    this.subscription = subscription;
    return Promise.resolve(true);
  }

  getMin(key: KStorageKey = 'min') {
    return Promise.resolve(this.state[key]);
  }

  getMax(key: KStorageKey = 'max') {
    return Promise.resolve(this.state[key]);
  }

  close() {
    this.subscription?.unsubscribe();
    return Promise.resolve(true);
  }

  complete(): void {
    this.subscription?.unsubscribe();
  }

  /**
   * Error handler for Observable
   */
  error(error: unknown): void {
    console.error(error);
  }

  /**
   * Adapter for set(), taking an object with props key and value.
   * Called by the Observable API on write to a given topic, storing the
   * latest value for a given key in the store.
   */
  next({ key, value }: KStorageEntry<KSV>): void {
    this.set(key, value);
  }
}
