const {
  addDays,
  daysRemaining,
  formatDate,
  startOfToday,
  endOfToday,
} = require('../src/utils/dateUtils');

describe('dateUtils', () => {
  describe('addDays', () => {
    it('adds calendar days', () => {
      const d = new Date(2025, 0, 28);
      const r = addDays(d, 4);
      expect(r.getFullYear()).toBe(2025);
      expect(r.getMonth()).toBe(1);
      expect(r.getDate()).toBe(1);
    });
  });

  describe('daysRemaining', () => {
    beforeEach(() => {
      jest.useFakeTimers();
      jest.setSystemTime(new Date(2025, 5, 15, 12, 0, 0, 0));
    });
    afterEach(() => {
      jest.useRealTimers();
    });

    it('returns positive days before expiry (local dates)', () => {
      const expiry = new Date(2025, 5, 20, 12, 0, 0, 0);
      expect(daysRemaining(expiry)).toBe(5);
    });
  });

  describe('formatDate', () => {
    it('formats as DD/MM/YYYY (en-GB)', () => {
      const s = formatDate(new Date(2025, 2, 9));
      expect(s).toMatch(/^09\/03\/2025$/);
    });
  });

  describe('startOfToday / endOfToday', () => {
    beforeEach(() => {
      jest.useFakeTimers();
      jest.setSystemTime(new Date('2025-03-10T15:30:45.123Z'));
    });
    afterEach(() => {
      jest.useRealTimers();
    });

    it('startOfToday zeros time fields', () => {
      const s = startOfToday();
      expect(s.getHours()).toBe(0);
      expect(s.getMinutes()).toBe(0);
      expect(s.getSeconds()).toBe(0);
      expect(s.getMilliseconds()).toBe(0);
    });

    it('endOfToday is end of local day', () => {
      const e = endOfToday();
      expect(e.getHours()).toBe(23);
      expect(e.getMinutes()).toBe(59);
      expect(e.getSeconds()).toBe(59);
      expect(e.getMilliseconds()).toBe(999);
    });
  });
});
