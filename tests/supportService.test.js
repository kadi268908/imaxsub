describe('supportService', () => {
  let snapshotContact;
  beforeAll(() => {
    snapshotContact = process.env.SUPPORT_CONTACT;
  });
  afterEach(() => {
    if (snapshotContact === undefined) delete process.env.SUPPORT_CONTACT;
    else process.env.SUPPORT_CONTACT = snapshotContact;
  });

  it('getSupportContactUrl returns https URL as-is', () => {
    jest.resetModules();
    process.env.SUPPORT_CONTACT = 'https://example.com/help';
    const { getSupportContactUrl } = require('../src/services/supportService');
    expect(getSupportContactUrl()).toBe('https://example.com/help');
  });

  it('getSupportContactUrl maps @username to t.me', () => {
    jest.resetModules();
    process.env.SUPPORT_CONTACT = '@MySupportBot';
    const { getSupportContactUrl } = require('../src/services/supportService');
    expect(getSupportContactUrl()).toBe('https://t.me/MySupportBot');
  });
});
