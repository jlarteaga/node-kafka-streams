import { EventEmitter } from 'events';
import { KStorage } from './k-storage';
import * as assert from 'assert';
import { catchError, EMPTY, fromEvent, map } from 'rxjs';

class FakeKafka extends EventEmitter {
  constructor() {
    super();
  }
}

const validateStoreValue = async (
  store: KStorage<string>,
  key: string,
  expectedValue: unknown
) => {
  const actualValue = await store.get(key);
  expect(actualValue).toBe(expectedValue);
};

describe('KStorage UNIT', function () {
  it('should store incoming values under corresponding keys, and unsubscribe when closed', function (done) {
    const kafka = new FakeKafka();
    const store = new KStorage<string>();
    const stream$ = fromEvent(kafka, 'message').pipe(
      map((value: any) => value.toLowerCase().split(' ')),
      map((value) => ({ key: value[0], value: value[1] })),
      catchError((error) => {
        console.error(error);
        return EMPTY;
      })
    );

    store.start(stream$.subscribe(store));
    stream$.forEach((value) => console.log(value));

    kafka.emit('message', 'key1 value1');
    kafka.emit('message', 'key2 value2');

    setTimeout(() => {
      store.close();
      kafka.emit('message', 'key3 value3');

      setTimeout(async () => {
        await Promise.all([
          validateStoreValue(store, 'key1', 'value1'),
          validateStoreValue(store, 'key2', 'value2'),
          validateStoreValue(store, 'key3', undefined)
        ]);
        done();
      });
    });
  });
});
