// src/services/analyticsService.js
// Analytics queries for dashboard stats, plan performance, daily summary

const User = require('../models/User');
const Subscription = require('../models/Subscription');
const Request = require('../models/Request');
const DailySummary = require('../models/DailySummary');
const { startOfToday, endOfToday } = require('../utils/dateUtils');

/**
 * Full growth dashboard stats
 */
const getGrowthStats = async () => {
  const today = startOfToday();
  const todayEnd = endOfToday();

  const [total, active, expired, blocked, newToday, renewalsToday] = await Promise.all([
    User.countDocuments({ role: 'user' }),
    User.countDocuments({ status: 'active' }),
    User.countDocuments({ status: 'expired' }),
    User.countDocuments({ isBlocked: true }),
    User.countDocuments({ createdAt: { $gte: today, $lte: todayEnd } }),
    Subscription.countDocuments({
      isRenewal: true,
      createdAt: { $gte: today, $lte: todayEnd },
    }),
  ]);

  return { total, active, expired, blocked, newToday, renewalsToday };
};

/**
 * Category-wise snapshot for subscriptions/requests
 */
const getCategoryWiseStats = async () => {
  const today = startOfToday();
  const todayEnd = endOfToday();
  const categories = ['movie', 'desi', 'non_desi'];

  const [activeSubscriptionsRaw, pendingRequestsRaw, approvalsTodayRaw, renewalsTodayRaw] = await Promise.all([
    Subscription.aggregate([
      {
        $match: {
          status: 'active',
          expiryDate: { $gt: new Date() },
        },
      },
      {
        $group: {
          _id: '$planCategory',
          count: { $sum: 1 },
        },
      },
    ]),
    Request.aggregate([
      {
        $match: {
          status: 'pending',
        },
      },
      {
        $group: {
          _id: '$requestCategory',
          count: { $sum: 1 },
        },
      },
    ]),
    Request.aggregate([
      {
        $match: {
          status: 'approved',
          actionDate: { $gte: today, $lte: todayEnd },
        },
      },
      {
        $group: {
          _id: '$requestCategory',
          count: { $sum: 1 },
        },
      },
    ]),
    Subscription.aggregate([
      {
        $match: {
          isRenewal: true,
          createdAt: { $gte: today, $lte: todayEnd },
        },
      },
      {
        $group: {
          _id: '$planCategory',
          count: { $sum: 1 },
        },
      },
    ]),
  ]);

  const toCountMap = (rows) => {
    const map = {};

    const normalizeStatsCategory = (value) => {
      const raw = String(value || '').toLowerCase();
      if (raw === 'movie' || raw === 'desi' || raw === 'non_desi') {
        return raw;
      }
      return null;
    };

    rows.forEach((row) => {
      const key = normalizeStatsCategory(row?._id);
      if (!key) return;
      map[key] = Number(map[key] || 0) + Number(row?.count || 0);
    });
    return map;
  };

  const activeMap = toCountMap(activeSubscriptionsRaw);
  const pendingMap = toCountMap(pendingRequestsRaw);
  const approvalsTodayMap = toCountMap(approvalsTodayRaw);
  const renewalsTodayMap = toCountMap(renewalsTodayRaw);

  return categories.map((category) => ({
    category,
    activeSubscriptions: activeMap[category] || 0,
    pendingRequests: pendingMap[category] || 0,
    approvalsToday: approvalsTodayMap[category] || 0,
    renewalsToday: renewalsTodayMap[category] || 0,
  }));
};

/**
 * Plan performance: active user count per plan
 */
const getPlanPerformance = async () => {
  return Subscription.aggregate([
    {
      $match: {
        status: 'active',
        expiryDate: { $gt: new Date() },
      },
    },
    {
      $group: {
        _id: '$planId',
        planName: { $first: '$planName' },
        durationDays: { $first: '$durationDays' },
        count: { $sum: 1 },
      },
    },
    { $sort: { durationDays: 1 } },
  ]);
};

/**
 * Build today's activity summary (called at end of day by cron)
 */
const buildDailySummary = async () => {
  const today = startOfToday();
  const todayEnd = endOfToday();

  const [newUsers, requestsReceived, approvals, renewals, expiredToday] = await Promise.all([
    User.countDocuments({ createdAt: { $gte: today, $lte: todayEnd } }),
    Request.countDocuments({ requestDate: { $gte: today, $lte: todayEnd } }),
    Request.countDocuments({ status: 'approved', actionDate: { $gte: today, $lte: todayEnd } }),
    Subscription.countDocuments({ isRenewal: true, createdAt: { $gte: today, $lte: todayEnd } }),
    Subscription.countDocuments({
      status: 'expired',
      updatedAt: { $gte: today, $lte: todayEnd },
    }),
  ]);

  // Upsert daily summary
  await DailySummary.findOneAndUpdate(
    { date: today },
    { newUsers, requestsReceived, approvals, renewals, expiredToday },
    { upsert: true, new: true }
  );

  return { newUsers, requestsReceived, approvals, renewals, expiredToday };
};

/**
 * Increment a counter in today's daily summary
 */
const incrementSummaryField = async (field, amount = 1) => {
  const today = startOfToday();
  await DailySummary.findOneAndUpdate(
    { date: today },
    { $inc: { [field]: amount } },
    { upsert: true }
  );
};

/**
 * Build comprehensive daily report with all key metrics (for 11:59 PM report)
 */
const buildComprehensiveDailyReport = async () => {
  const today = startOfToday();
  const todayEnd = endOfToday();
  const now = new Date();

  const [
    growthStats,
    categoryStats,
    planPerformance,
    totalUsers,
    totalActiveSubscriptions,
    totalPendingRequests,
    rejectedRequests,
    pendingPayments,
    newUsersList,
  ] = await Promise.all([
    getGrowthStats(),
    getCategoryWiseStats(),
    getPlanPerformance(),
    User.countDocuments({ role: 'user' }),
    Subscription.countDocuments({ status: 'active', expiryDate: { $gt: now } }),
    Request.countDocuments({ status: 'pending' }),
    Request.countDocuments({ status: 'rejected', actionDate: { $gte: today, $lte: todayEnd } }),
    Subscription.countDocuments({ status: 'pending_payment', createdAt: { $gte: today, $lte: todayEnd } }),
    User.find({
      role: 'user',
      joinDate: { $gte: today, $lte: todayEnd }
    })
      .select('telegramId name username status joinDate')
      .sort({ joinDate: 1 })
      .lean(),
  ]);

  // Enrich new users with their subscription/request categories
  const newUsersToday = await Promise.all(
    newUsersList.map(async (user) => {
      const [subscriptions, requests] = await Promise.all([
        Subscription.find({ telegramId: user.telegramId }).select('planCategory').lean(),
        Request.find({ telegramId: user.telegramId }).select('requestCategory').lean(),
      ]);

      const categories = new Set();
      subscriptions.forEach(s => s.planCategory && categories.add(s.planCategory));
      requests.forEach(r => r.requestCategory && categories.add(r.requestCategory));

      return {
        ...user,
        categories: Array.from(categories).sort(),
      };
    })
  );

  return {
    generatedAt: new Date().toISOString(),
    date: today.toISOString().slice(0, 10),
    growthStats,
    categoryStats,
    planPerformance,
    totalUsers,
    totalActiveSubscriptions,
    totalPendingRequests,
    rejectedRequestsToday: rejectedRequests,
    pendingPaymentsToday: pendingPayments,
    newUsersToday,
  };
};

module.exports = {
  getGrowthStats,
  getCategoryWiseStats,
  getPlanPerformance,
  buildDailySummary,
  incrementSummaryField,
  buildComprehensiveDailyReport,
};
