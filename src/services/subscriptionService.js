// src/services/subscriptionService.js
// Core subscription business logic: create/renew, expire, reporting

const Subscription = require('../models/Subscription');
const User = require('../models/User');
const Plan = require('../models/Plan');
const Request = require('../models/Request');
const { addDays, startOfToday, endOfToday, startOfWeek, startOfMonth } = require('../utils/dateUtils');
const logger = require('../utils/logger');
const { normalizePlanCategory, getGroupIdForCategory } = require('../utils/premiumGroups');

const syncUserStatusFromSubscriptions = async (telegramId, options = {}) => {
  const session = options.session || null;
  const now = new Date();
  const hasActive = await Subscription.exists({
    telegramId,
    status: 'active',
    expiryDate: { $gt: now },
  }).session(session);

  const nextStatus = hasActive ? 'active' : 'expired';
  await User.findOneAndUpdate(
    { telegramId },
    { status: nextStatus },
    session ? { session } : {}
  );
  return nextStatus;
};

/**
 * Create or RENEW a subscription.
 * Renewal: extends existing active subscription by plan.durationDays from current expiry.
 * New: starts fresh from today.
 * Returns { subscription, isRenewal }
 */
const createSubscription = async (telegramId, plan, adminId, options = {}) => {
  try {
    const session = options.session || null;
    const user = await User.findOne({ telegramId }).session(session);
    if (!user) throw new Error(`User ${telegramId} not found`);

    const planCategory = normalizePlanCategory(options.planCategory || plan.category);
    const premiumGroupId = String(options.premiumGroupId || getGroupIdForCategory(planCategory) || '');

    // Check for an existing active subscription of same category to extend
    const existingSub = await Subscription.findOne({
      telegramId,
      status: 'active',
      planCategory,
    }).session(session);

    if (existingSub) {
      // RENEWAL — extend expiry from current expiry (or today if already past)
      const baseDate = existingSub.expiryDate > new Date()
        ? existingSub.expiryDate
        : new Date();
      const newExpiry = addDays(baseDate, plan.durationDays);

      existingSub.expiryDate = newExpiry;
      existingSub.planId = plan._id;
      existingSub.planName = plan.name;
      existingSub.planCategory = planCategory;
      existingSub.premiumGroupId = premiumGroupId || null;
      existingSub.durationDays = plan.durationDays;
      existingSub.status = 'active';
      existingSub.approvedBy = adminId;
      existingSub.isRenewal = true;
      existingSub.reminderFlags = { day7: false, day3: false, day1: false, day0: false };
      await existingSub.save(session ? { session } : {});

      await User.findByIdAndUpdate(
        user._id,
        { status: 'active' },
        session ? { session } : {}
      );
      logger.info(`Subscription RENEWED for ${telegramId}: +${plan.durationDays} days → expiry ${newExpiry}`);
      return existingSub;
    }

    // NEW subscription
    const startDate = new Date();
    const expiryDate = addDays(startDate, plan.durationDays);

    const subscription = new Subscription({
      userId: user._id,
      telegramId,
      planId: plan._id,
      planName: plan.name,
      planCategory,
      premiumGroupId: premiumGroupId || null,
      durationDays: plan.durationDays,
      startDate,
      expiryDate,
      status: 'active',
      approvedBy: adminId,
      isRenewal: false,
    });
    await subscription.save(session ? { session } : {});

    await User.findByIdAndUpdate(
      user._id,
      { status: 'active' },
      session ? { session } : {}
    );
    logger.info(`Subscription CREATED for ${telegramId}: ${plan.name} until ${expiryDate}`);
    return subscription;
  } catch (error) {
    logger.error(`createSubscription error: ${error.message}`);
    throw error;
  }
};

const expireSubscription = async (subscription, options = {}) => {
  const session = options.session || null;
  await Subscription.findByIdAndUpdate(
    subscription._id,
    { status: 'expired' },
    session ? { session } : {}
  );
  await syncUserStatusFromSubscriptions(subscription.telegramId, { session });
  logger.info(`Subscription expired for user ${subscription.telegramId}`);
};

const getSubscriptionsExpiringSoon = async (daysAhead) => {
  const today = startOfToday();
  const targetDate = addDays(today, daysAhead);
  const targetEnd = new Date(targetDate);
  targetEnd.setHours(23, 59, 59, 999);
  return Subscription.find({
    status: 'active',
    expiryDate: { $gte: targetDate, $lte: targetEnd },
  });
};

const getExpiredUnprocessed = async () => {
  return Subscription.find({
    status: 'active',
    expiryDate: { $lt: startOfToday() },
  });
};

const getSalesReport = async (startDate, endDate) => {
  return Request.aggregate([
    {
      $match: {
        status: 'approved',
        actionDate: { $gte: startDate, $lte: endDate },
      },
    },
    {
      $lookup: {
        from: 'plans',
        localField: 'selectedPlanId',
        foreignField: '_id',
        as: 'plan',
      },
    },
    { $unwind: { path: '$plan', preserveNullAndEmptyArrays: true } },
    {
      $addFields: {
        basePrice: { $ifNull: ['$plan.price', 0] },
        discountPercent: { $ifNull: ['$appliedUserOffer.discountPercent', 0] },
      },
    },
    {
      $addFields: {
        payableAmount: {
          $cond: [
            { $gt: ['$discountPercent', 0] },
            {
              $ceil: {
                $max: [
                  0,
                  {
                    $subtract: [
                      '$basePrice',
                      {
                        $multiply: ['$basePrice', { $divide: ['$discountPercent', 100] }],
                      },
                    ],
                  },
                ],
              },
            },
            '$basePrice',
          ],
        },
      },
    },
    {
      $lookup: {
        from: 'subscriptions',
        let: {
          reqTelegramId: '$telegramId',
          reqPlanId: '$selectedPlanId',
          reqCategory: '$requestCategory',
        },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [
                  { $eq: ['$telegramId', '$$reqTelegramId'] },
                  { $eq: ['$planId', '$$reqPlanId'] },
                  { $eq: ['$planCategory', '$$reqCategory'] },
                  { $eq: ['$status', 'cancelled'] },
                ],
              },
            },
          },
          { $limit: 1 },
        ],
        as: 'revokedSubscriptions',
      },
    },
    {
      $match: {
        $expr: { $eq: [{ $size: '$revokedSubscriptions' }, 0] },
      },
    },
    {
      $group: {
        _id: '$selectedPlanId',
        planName: { $first: { $ifNull: ['$plan.name', 'Unknown Plan'] } },
        count: { $sum: 1 },
        totalRevenue: { $sum: '$payableAmount' },
      },
    },
    { $sort: { totalRevenue: -1 } },
  ]);
};

const getSalesUserBreakdown = async (startDate, endDate) => {
  return Request.aggregate([
    {
      $match: {
        status: 'approved',
        actionDate: { $gte: startDate, $lte: endDate },
      },
    },
    {
      $lookup: {
        from: 'plans',
        localField: 'selectedPlanId',
        foreignField: '_id',
        as: 'plan',
      },
    },
    { $unwind: { path: '$plan', preserveNullAndEmptyArrays: true } },
    {
      $addFields: {
        basePrice: { $ifNull: ['$plan.price', 0] },
        discountPercent: { $ifNull: ['$appliedUserOffer.discountPercent', 0] },
      },
    },
    {
      $addFields: {
        payableAmount: {
          $cond: [
            { $gt: ['$discountPercent', 0] },
            {
              $ceil: {
                $max: [
                  0,
                  {
                    $subtract: [
                      '$basePrice',
                      {
                        $multiply: ['$basePrice', { $divide: ['$discountPercent', 100] }],
                      },
                    ],
                  },
                ],
              },
            },
            '$basePrice',
          ],
        },
      },
    },
    {
      $lookup: {
        from: 'subscriptions',
        let: {
          reqTelegramId: '$telegramId',
          reqPlanId: '$selectedPlanId',
          reqCategory: '$requestCategory',
        },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [
                  { $eq: ['$telegramId', '$$reqTelegramId'] },
                  { $eq: ['$planId', '$$reqPlanId'] },
                  { $eq: ['$planCategory', '$$reqCategory'] },
                  { $eq: ['$status', 'cancelled'] },
                ],
              },
            },
          },
          { $limit: 1 },
        ],
        as: 'revokedSubscriptions',
      },
    },
    {
      $match: {
        $expr: { $eq: [{ $size: '$revokedSubscriptions' }, 0] },
      },
    },
    {
      $project: {
        _id: 0,
        telegramId: 1,
        planCategory: { $ifNull: ['$plan.category', '$requestCategory'] },
        planName: { $ifNull: ['$plan.name', 'Unknown Plan'] },
        planPrice: '$payableAmount',
        createdAt: '$actionDate',
      },
    },
    { $sort: { createdAt: -1 } },
  ]);
};

const getTodayExpiryList = async () => {
  return Subscription.find({
    status: 'active',
    expiryDate: { $gte: startOfToday(), $lte: endOfToday() },
  }).lean();
};

module.exports = {
  createSubscription,
  syncUserStatusFromSubscriptions,
  expireSubscription,
  getSubscriptionsExpiringSoon,
  getExpiredUnprocessed,
  getSalesReport,
  getSalesUserBreakdown,
  getTodayExpiryList,
  startOfToday,
  endOfToday,
  startOfWeek,
  startOfMonth,
};
