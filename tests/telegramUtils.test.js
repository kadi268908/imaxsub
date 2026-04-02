describe('telegramUtils.scheduleDeleteMessage', () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('schedules deleteMessage with delay in minutes', async () => {
    const deleteMessage = jest.fn().mockResolvedValue(true);
    const telegram = { deleteMessage };
    const { scheduleDeleteMessage } = require('../src/utils/telegramUtils');

    scheduleDeleteMessage(telegram, 12345, 67890, 30);
    expect(deleteMessage).not.toHaveBeenCalled();

    jest.advanceTimersByTime(30 * 60 * 1000);
    await Promise.resolve();
    expect(deleteMessage).toHaveBeenCalledWith(12345, 67890);
  });

  it('does nothing when ids are missing', () => {
    const deleteMessage = jest.fn().mockResolvedValue(true);
    const telegram = { deleteMessage };
    const { scheduleDeleteMessage } = require('../src/utils/telegramUtils');

    scheduleDeleteMessage(telegram, null, 67890, 30);
    jest.runOnlyPendingTimers();
    expect(deleteMessage).not.toHaveBeenCalled();
  });
});
