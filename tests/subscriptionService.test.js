jest.mock('../src/models/Subscription', () => ({
    find: jest.fn(),
    bulkWrite: jest.fn(),
}));

const Subscription = require('../src/models/Subscription');

describe('subscriptionService.extendActiveSubscriptionsByDays', () => {
    beforeEach(() => {
        jest.clearAllMocks();
    });

    it('extends only active subscriptions in the requested category', async () => {
        const firstExpiry = new Date('2026-09-10T00:00:00.000Z');
        const secondExpiry = new Date('2026-09-12T00:00:00.000Z');

        const sort = jest.fn().mockReturnValue({
            lean: jest.fn().mockResolvedValue([
                { _id: 'sub1', telegramId: 111, expiryDate: firstExpiry, planCategory: 'movie' },
                { _id: 'sub2', telegramId: 222, expiryDate: secondExpiry, planCategory: 'movie' },
            ]),
        });

        Subscription.find.mockReturnValue({ sort });
        Subscription.bulkWrite.mockResolvedValue({ modifiedCount: 2 });

        const { extendActiveSubscriptionsByDays } = require('../src/services/subscriptionService');
        const result = await extendActiveSubscriptionsByDays({
            category: 'movie',
            days: 5,
            telegramIds: [111, 222],
        });

        expect(Subscription.find).toHaveBeenCalledWith(expect.objectContaining({
            status: 'active',
            planCategory: 'movie',
            telegramId: { $in: [111, 222] },
        }));
        expect(Subscription.bulkWrite).toHaveBeenCalledTimes(1);
        expect(Subscription.bulkWrite.mock.calls[0][0]).toHaveLength(2);
        expect(result).toMatchObject({
            category: 'movie',
            days: 5,
            matchedCount: 2,
            modifiedCount: 2,
        });
        expect(result.matchedTelegramIds).toEqual([111, 222]);
    });

    it('returns zero counts when no active subscriptions match', async () => {
        const sort = jest.fn().mockReturnValue({
            lean: jest.fn().mockResolvedValue([]),
        });

        Subscription.find.mockReturnValue({ sort });

        const { extendActiveSubscriptionsByDays } = require('../src/services/subscriptionService');
        const result = await extendActiveSubscriptionsByDays({
            category: 'desi',
            days: 3,
            telegramIds: [999],
        });

        expect(Subscription.bulkWrite).not.toHaveBeenCalled();
        expect(result).toMatchObject({
            category: 'desi',
            days: 3,
            matchedCount: 0,
            modifiedCount: 0,
        });
        expect(result.matchedTelegramIds).toEqual([]);
    });
});