/**
 * Smoke: main modules load and export expected APIs (no Telegram / DB I/O).
 */

describe('module exports', () => {
  it('subscriptionService', () => {
    const s = require('../src/services/subscriptionService');
    expect(s.createSubscription).toEqual(expect.any(Function));
    expect(s.syncUserStatusFromSubscriptions).toEqual(expect.any(Function));
    expect(s.expireSubscription).toEqual(expect.any(Function));
    expect(s.getSubscriptionsExpiringSoon).toEqual(expect.any(Function));
    expect(s.getExpiredUnprocessed).toEqual(expect.any(Function));
    expect(s.getSalesReport).toEqual(expect.any(Function));
    expect(s.getSalesUserBreakdown).toEqual(expect.any(Function));
    expect(s.getTodayExpiryList).toEqual(expect.any(Function));
  });

  it('cronService', () => {
    const c = require('../src/services/cronService');
    expect(c.initCronJobs).toEqual(expect.any(Function));
    expect(c.expiryEnforcementHandler).toEqual(expect.any(Function));
    expect(c.membershipMonitor).toEqual(expect.any(Function));
    expect(c.reminderScheduler).toEqual(expect.any(Function));
  });

  it('adminService', () => {
    const a = require('../src/services/adminService');
    expect(Object.keys(a).length).toBeGreaterThan(0);
  });

  it('userService', () => {
    const u = require('../src/services/userService');
    expect(Object.keys(u).length).toBeGreaterThan(0);
  });

  it('telegramUtils', () => {
    const t = require('../src/utils/telegramUtils');
    expect(t.safeSend).toEqual(expect.any(Function));
    expect(t.banFromGroup).toEqual(expect.any(Function));
  });
});
