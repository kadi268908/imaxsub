const { isTransactionUnsupportedError } = require('../src/utils/mongoErrors');

describe('mongoErrors', () => {
  it('returns false for null', () => {
    expect(isTransactionUnsupportedError(null)).toBe(false);
  });

  it('detects code 20 IllegalOperation', () => {
    expect(isTransactionUnsupportedError({ code: 20, codeName: 'IllegalOperation' })).toBe(true);
  });

  it('detects code 251', () => {
    expect(isTransactionUnsupportedError({ code: 251, message: 'x' })).toBe(true);
  });

  it('detects message patterns', () => {
    expect(
      isTransactionUnsupportedError({ message: 'Transaction numbers are only allowed on a replica set' })
    ).toBe(true);
  });
});
