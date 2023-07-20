import { sayHi } from './say-hi';

describe('sayHi', () => {
  it('should say hi', () => {
    expect(sayHi('Jorge')).toBe('Hello Jorge!');
  });
});
