// src/services/referralService.js
// Handles referral code generation, tracking, and referral/seller rewards

const User = require('../models/User');
const Subscription = require('../models/Subscription');
const UserOffer = require('../models/UserOffer');
const AdminLog = require('../models/AdminLog');
const SellerWithdrawalRequest = require('../models/SellerWithdrawalRequest');
const SellerPayoutLedger = require('../models/SellerPayoutLedger');
const logger = require('../utils/logger');
const { escapeMarkdown } = require('../utils/markdownEscape');

const REFERRAL_REWARD_DISCOUNT_PERCENT = Math.min(100, Math.max(0, parseFloat(process.env.REFERRAL_REWARD_DISCOUNT_PERCENT || '10')));
const SELLER_COMMISSION_PERCENT = parseFloat(process.env.SELLER_COMMISSION_PERCENT || '15');
const SELLER_MIN_WITHDRAW_REFERRALS = parseInt(process.env.SELLER_MIN_WITHDRAW_REFERRALS || '10', 10);
const SELLER_MIN_WITHDRAW_BALANCE = parseFloat(process.env.SELLER_MIN_WITHDRAW_BALANCE || '200');
const SELLER_WITHDRAW_MIN_PROCESS_HOURS = parseInt(process.env.SELLER_WITHDRAW_MIN_PROCESS_HOURS || '24', 10);
/** When `true`, super admins can approve before the minimum wait (user-facing copy can still say "24h"). */
const SELLER_WITHDRAW_ALLOW_EARLY_APPROVAL = String(process.env.SELLER_WITHDRAW_ALLOW_EARLY_APPROVAL || '')
  .toLowerCase() === 'true';
// Local part min 1 char; PSP handle after @ is typically letters (e.g. paytm, ybl, okaxis).
const UPI_ID_REGEX = /^[a-zA-Z0-9._-]{1,64}@[a-zA-Z]{2,64}$/;
const SELLER_PAYOUT_HISTORY_LIMIT = parseInt(process.env.SELLER_PAYOUT_HISTORY_LIMIT || '10', 10);

const getWithdrawalApprovalAllowedAt = (requestedAt) => {
  const hours = Number.isFinite(SELLER_WITHDRAW_MIN_PROCESS_HOURS) ? SELLER_WITHDRAW_MIN_PROCESS_HOURS : 24;
  return new Date(new Date(requestedAt).getTime() + Math.max(0, hours) * 60 * 60 * 1000);
};

const canApproveWithdrawalByTime = (requestedAt) => {
  if (SELLER_WITHDRAW_ALLOW_EARLY_APPROVAL) return true;
  const hours = Number.isFinite(SELLER_WITHDRAW_MIN_PROCESS_HOURS) ? SELLER_WITHDRAW_MIN_PROCESS_HOURS : 24;
  if (hours <= 0) return true;
  const allowedAt = getWithdrawalApprovalAllowedAt(requestedAt);
  return Date.now() >= allowedAt.getTime();
};

const withdrawalApprovalTimeError = (allowedAt) => {
  const hours = Number.isFinite(SELLER_WITHDRAW_MIN_PROCESS_HOURS) ? SELLER_WITHDRAW_MIN_PROCESS_HOURS : 24;
  return (
    `Withdrawal approval allowed after ${allowedAt.toLocaleString('en-IN')} (minimum ${hours} hour(s)). ` +
    'To allow sooner: set SELLER_WITHDRAW_MIN_PROCESS_HOURS=0 or SELLER_WITHDRAW_ALLOW_EARLY_APPROVAL=true in .env, then restart the bot.'
  );
};

const getSuperAdminIds = () => {
  return String(process.env.SUPER_ADMIN_IDS || process.env.SUPER_ADMIN_ID || '')
    .split(',')
    .map(id => parseInt(id.trim(), 10))
    .filter(Boolean);
};

const assertSuperAdmin = (telegramId) => {
  const superAdminIds = getSuperAdminIds();
  if (!superAdminIds.includes(Number(telegramId))) {
    throw new Error('Only super admin can approve/reject seller withdrawals.');
  }
};

const generateSellerCode = () => `SEL${Math.random().toString(36).slice(2, 8).toUpperCase()}`;

const generateUniqueSellerCode = async () => {
  let code = generateSellerCode();
  let exists = await User.exists({ sellerCode: code });

  while (exists) {
    code = generateSellerCode();
    exists = await User.exists({ sellerCode: code });
  }

  return code;
};

/**
 * Process referral when a new user starts the bot with ?start=ref_CODE
 */
const processReferral = async (newUser, referralCode) => {
  try {
    if (!referralCode || newUser.referredBy) return; // Already referred or no code

    const referrer = await User.findOne({ referralCode });
    if (!referrer || referrer.telegramId === newUser.telegramId) return;

    await User.findByIdAndUpdate(newUser._id, { referredBy: referrer.telegramId });
    logger.info(`Referral attribution: referredBy=${referrer.telegramId}, referredTo=${newUser.telegramId}, code=${referralCode}`);
  } catch (err) {
    logger.error(`processReferral error: ${err.message}`);
  }
};

/**
 * Award referral discount coupon to referrer when referred user gets their FIRST approved subscription.
 * Coupon auto-applies on next request/renewal via UserOffer consumption flow.
 */
const awardReferralBonus = async (bot, newSubscriberTelegramId) => {
  try {
    const newUser = await User.findOne({ telegramId: newSubscriberTelegramId });
    if (!newUser || !newUser.referredBy || newUser.referralBonusApplied) return;

    // Only on first-ever subscription
    const subCount = await Subscription.countDocuments({
      telegramId: newSubscriberTelegramId,
      status: { $in: ['active', 'expired'] },
    });
    if (subCount > 1) return;

    const referrer = await User.findOne({ telegramId: newUser.referredBy });
    if (!referrer) return;

    if (REFERRAL_REWARD_DISCOUNT_PERCENT > 0) {
      const validTill = new Date();
      validTill.setFullYear(validTill.getFullYear() + 10);

      await UserOffer.create({
        targetTelegramId: referrer.telegramId,
        title: 'Referral Reward',
        description: `Referral reward from ${newUser.name} subscription`,
        discountPercent: REFERRAL_REWARD_DISCOUNT_PERCENT,
        validTill,
        isActive: true,
        isUsed: false,
        createdBy: 0,
      });

      try {
        const safeReferredUserName = escapeMarkdown(newUser.name || 'User');
        await bot.telegram.sendMessage(
          referrer.telegramId,
          `🎁 *Referral Reward Unlocked!*\n\nYour referral *${safeReferredUserName}* just subscribed!\n` +
          `You earned *${REFERRAL_REWARD_DISCOUNT_PERCENT}% OFF* on your next premium purchase/renewal. 🎉`,
          { parse_mode: 'Markdown' }
        );
      } catch (_) { }
    }

    await User.findByIdAndUpdate(newUser._id, { referralBonusApplied: true });

    await AdminLog.create({
      adminId: 0, // system action
      actionType: 'referral_bonus',
      targetUserId: referrer.telegramId,
      details: {
        referredUser: newSubscriberTelegramId,
        rewardType: 'discount_percent',
        discountPercent: REFERRAL_REWARD_DISCOUNT_PERCENT,
      },
    });

    logger.info(`Referral reward: ${REFERRAL_REWARD_DISCOUNT_PERCENT}% coupon awarded to ${referrer.telegramId}`);
  } catch (err) {
    logger.error(`awardReferralBonus error: ${err.message}`);
  }
};

/**
 * Register a user into seller program.
 */
const registerSellerProgram = async (telegramId) => {
  const user = await User.findOne({ telegramId });
  if (!user) return null;

  if (user.isSeller && user.sellerCode) return user;

  const sellerCode = user.sellerCode || await generateUniqueSellerCode();

  await User.findByIdAndUpdate(user._id, {
    isSeller: true,
    sellerCode,
    $setOnInsert: {
      sellerStats: {
        totalReferrals: 0,
        qualifiedReferrals: 0,
        lifetimeEarnings: 0,
        availableBalance: 0,
      },
    },
    $set: {
      'meta.sellerRegisteredAt': user.meta?.sellerRegisteredAt || new Date(),
    },
  });

  return User.findOne({ telegramId });
};

/**
 * Process seller referral when user starts with ?start=seller_CODE
 */
const processSellerReferral = async (newUser, sellerCode) => {
  try {
    if (!sellerCode || newUser.sellerReferredBy) return;

    const seller = await User.findOne({ sellerCode, isSeller: true });
    if (!seller || seller.telegramId === newUser.telegramId) return;

    await User.findByIdAndUpdate(newUser._id, { sellerReferredBy: seller.telegramId });
    await User.findByIdAndUpdate(seller._id, { $inc: { 'sellerStats.totalReferrals': 1 } });

    logger.info(`Seller referral attribution: referredBy=${seller.telegramId}, referredTo=${newUser.telegramId}, code=${sellerCode}`);
  } catch (err) {
    logger.error(`processSellerReferral error: ${err.message}`);
  }
};

/**
 * Credit seller commission when referred user gets first approved subscription.
 */
const awardSellerCommission = async (bot, newSubscriberTelegramId, saleValue = 0) => {
  try {
    const newUser = await User.findOne({ telegramId: newSubscriberTelegramId });
    if (!newUser || !newUser.sellerReferredBy || newUser.sellerCommissionApplied) return;

    const subCount = await Subscription.countDocuments({
      telegramId: newSubscriberTelegramId,
      status: { $in: ['active', 'expired'] },
    });
    if (subCount > 1) return;

    const seller = await User.findOne({ telegramId: newUser.sellerReferredBy, isSeller: true });
    if (!seller) return;

    const numericSale = Number(saleValue) || 0;
    const commission = Number(((numericSale * SELLER_COMMISSION_PERCENT) / 100).toFixed(2));

    await User.findByIdAndUpdate(seller._id, {
      $inc: {
        'sellerStats.qualifiedReferrals': 1,
        'sellerStats.lifetimeEarnings': commission,
        'sellerStats.availableBalance': commission,
      },
    });

    const updatedSeller = await User.findById(seller._id).select('sellerStats.availableBalance').lean();
    await SellerPayoutLedger.create({
      sellerTelegramId: seller.telegramId,
      entryType: 'credit',
      source: 'commission',
      amount: commission,
      balanceAfter: Number(updatedSeller?.sellerStats?.availableBalance || 0),
      relatedUserTelegramId: newSubscriberTelegramId,
      note: `Commission from referral subscription: ${newSubscriberTelegramId}`,
      createdBy: 0,
    });

    await User.findByIdAndUpdate(newUser._id, { sellerCommissionApplied: true });

    try {
      const safeReferredUserName = escapeMarkdown(newUser.name || 'User');
      await bot.telegram.sendMessage(
        seller.telegramId,
        `💰 *Seller Commission Credited!*\n\n` +
        `Referral: *${safeReferredUserName}*\n` +
        `Sale Value: *₹${numericSale.toFixed(2)}*\n` +
        `Commission (${SELLER_COMMISSION_PERCENT}%): *₹${commission.toFixed(2)}*\n\n` +
        `Use /seller to view your seller dashboard.`,
        { parse_mode: 'Markdown' }
      );
    } catch (_) { }

    await AdminLog.create({
      adminId: 0,
      actionType: 'referral_bonus',
      targetUserId: seller.telegramId,
      details: {
        type: 'seller_commission',
        referredUser: newSubscriberTelegramId,
        saleValue: numericSale,
        commission,
        commissionPercent: SELLER_COMMISSION_PERCENT,
      },
    });
  } catch (err) {
    logger.error(`awardSellerCommission error: ${err.message}`);
  }
};

const getSellerProgramSummary = async (telegramId) => {
  const user = await User.findOne({ telegramId });
  if (!user) return null;

  const stats = user.sellerStats || {
    totalReferrals: 0,
    qualifiedReferrals: 0,
    lifetimeEarnings: 0,
    availableBalance: 0,
  };

  const canWithdraw =
    stats.qualifiedReferrals >= SELLER_MIN_WITHDRAW_REFERRALS ||
    stats.availableBalance >= SELLER_MIN_WITHDRAW_BALANCE;

  return {
    isSeller: !!user.isSeller,
    sellerCode: user.sellerCode,
    stats,
    canWithdraw,
    withdrawRules: {
      minReferrals: SELLER_MIN_WITHDRAW_REFERRALS,
      minBalance: SELLER_MIN_WITHDRAW_BALANCE,
    },
  };
};

const requestSellerWithdrawal = async (telegramId, upiIdRaw) => {
  const summary = await getSellerProgramSummary(telegramId);
  if (!summary || !summary.isSeller) {
    throw new Error('Seller program not registered.');
  }

  const upiId = String(upiIdRaw || '').trim().toLowerCase();
  if (!upiId) {
    throw new Error('UPI ID is required for withdrawal request.');
  }

  if (!UPI_ID_REGEX.test(upiId)) {
    throw new Error('Invalid UPI ID format. Example: name@bank');
  }

  if (!summary.canWithdraw) {
    throw new Error(
      `Withdrawal not unlocked. Need ${summary.withdrawRules.minReferrals} qualified referrals or ₹${summary.withdrawRules.minBalance} balance.`
    );
  }

  const pending = await SellerWithdrawalRequest.findOne({ sellerTelegramId: telegramId, status: 'pending' });
  if (pending) {
    throw new Error('You already have a pending withdrawal request.');
  }

  const amount = Number(summary.stats.availableBalance || 0);
  if (amount <= 0) {
    throw new Error('Available balance is zero.');
  }

  let request;
  try {
    request = await SellerWithdrawalRequest.create({
      sellerTelegramId: telegramId,
      upiId,
      amount,
    });
  } catch (err) {
    if (err?.code === 11000) {
      throw new Error('You already have a pending withdrawal request.');
    }
    throw err;
  }

  await AdminLog.create({
    adminId: 0,
    actionType: 'seller_withdraw_request',
    targetUserId: telegramId,
    details: { requestId: request._id, amount, upiId },
  });

  return request;
};

const getSellerWithdrawalRequests = async ({ status = 'pending', limit = 20, sellerTelegramId = null } = {}) => {
  const query = {};
  if (status && status !== 'all') {
    query.status = status;
  }
  if (sellerTelegramId) {
    query.sellerTelegramId = Number(sellerTelegramId);
  }

  return SellerWithdrawalRequest.find(query)
    .sort({ requestedAt: 1 })
    .limit(limit);
};

const getPendingSellerWithdrawalRequests = async (limit = 20) => {
  return getSellerWithdrawalRequests({ status: 'pending', limit });
};

const getSellerWithdrawalHistory = async (sellerTelegramId, limit = SELLER_PAYOUT_HISTORY_LIMIT) => {
  return SellerWithdrawalRequest.find({ sellerTelegramId })
    .sort({ requestedAt: -1 })
    .limit(limit)
    .lean();
};

const getSellerPayoutLedgerHistory = async (sellerTelegramId, limit = SELLER_PAYOUT_HISTORY_LIMIT) => {
  return SellerPayoutLedger.find({ sellerTelegramId })
    .sort({ createdAt: -1 })
    .limit(limit)
    .lean();
};

const approveSellerWithdrawal = async (requestId, adminTelegramId) => {
  assertSuperAdmin(adminTelegramId);

  const request = await SellerWithdrawalRequest.findById(requestId);
  if (!request) throw new Error('Withdrawal request not found.');
  if (request.status !== 'pending') throw new Error('Withdrawal request already processed.');

  const allowedAt = getWithdrawalApprovalAllowedAt(request.requestedAt);
  if (!canApproveWithdrawalByTime(request.requestedAt)) {
    throw new Error(withdrawalApprovalTimeError(allowedAt));
  }

  const mongoose = require('mongoose');
  const session = await mongoose.startSession();
  let approvedRequest;
  const transactionUnsupportedMessage = 'Transaction numbers are only allowed on a replica set member or mongos';

  try {
    await session.withTransaction(async () => {
      const latest = await SellerWithdrawalRequest.findById(requestId).session(session);
      if (!latest) throw new Error('Withdrawal request not found.');
      if (latest.status !== 'pending') throw new Error('Withdrawal request already processed.');

      const latestAllowedAt = getWithdrawalApprovalAllowedAt(latest.requestedAt);
      if (!canApproveWithdrawalByTime(latest.requestedAt)) {
        throw new Error(withdrawalApprovalTimeError(latestAllowedAt));
      }

      const seller = await User.findOneAndUpdate(
        {
          telegramId: latest.sellerTelegramId,
          'sellerStats.availableBalance': { $gte: latest.amount },
        },
        {
          $inc: { 'sellerStats.availableBalance': -latest.amount },
          $set: { 'sellerStats.qualifiedReferrals': 0 },
        },
        { new: true, session }
      );

      if (!seller) {
        throw new Error('Insufficient available balance for this withdrawal request.');
      }

      latest.status = 'approved';
      latest.reviewedAt = new Date();
      latest.reviewedBy = adminTelegramId;
      await latest.save({ session });

      await SellerPayoutLedger.create([{
        sellerTelegramId: latest.sellerTelegramId,
        entryType: 'debit',
        source: 'withdrawal_approved',
        amount: latest.amount,
        balanceAfter: Number(seller?.sellerStats?.availableBalance || 0),
        relatedWithdrawalRequestId: latest._id,
        note: `Withdrawal approved to UPI ${latest.upiId || 'N/A'}`,
        createdBy: adminTelegramId,
      }], { session });

      await AdminLog.create([{
        adminId: adminTelegramId,
        actionType: 'seller_withdraw_approve',
        targetUserId: latest.sellerTelegramId,
        details: { requestId: latest._id, amount: latest.amount, upiId: latest.upiId || null },
      }], { session });

      approvedRequest = latest;
    });
  } catch (err) {
    const message = String(err?.message || '');
    if (!message.includes(transactionUnsupportedMessage)) {
      throw err;
    }

    const latest = await SellerWithdrawalRequest.findById(requestId);
    if (!latest) throw new Error('Withdrawal request not found.');
    if (latest.status !== 'pending') throw new Error('Withdrawal request already processed.');

    const latestAllowedAt = getWithdrawalApprovalAllowedAt(latest.requestedAt);
    if (!canApproveWithdrawalByTime(latest.requestedAt)) {
      throw new Error(withdrawalApprovalTimeError(latestAllowedAt));
    }

    const latestSeller = await User.findOne(
      { telegramId: latest.sellerTelegramId },
      { 'sellerStats.qualifiedReferrals': 1 }
    ).lean();
    const previousQualifiedReferrals = Number(latestSeller?.sellerStats?.qualifiedReferrals || 0);

    const seller = await User.findOneAndUpdate(
      {
        telegramId: latest.sellerTelegramId,
        'sellerStats.availableBalance': { $gte: latest.amount },
      },
      {
        $inc: { 'sellerStats.availableBalance': -latest.amount },
        $set: { 'sellerStats.qualifiedReferrals': 0 },
      },
      { new: true }
    );
    if (!seller) {
      throw new Error('Insufficient available balance for this withdrawal request.');
    }

    const marked = await SellerWithdrawalRequest.findOneAndUpdate(
      { _id: latest._id, status: 'pending' },
      {
        $set: {
          status: 'approved',
          reviewedAt: new Date(),
          reviewedBy: adminTelegramId,
        },
      },
      { new: true }
    );

    if (!marked) {
      await User.findOneAndUpdate(
        { telegramId: latest.sellerTelegramId },
        {
          $inc: { 'sellerStats.availableBalance': latest.amount },
          $set: { 'sellerStats.qualifiedReferrals': previousQualifiedReferrals },
        }
      );
      throw new Error('Withdrawal request already processed.');
    }

    await AdminLog.create({
      adminId: adminTelegramId,
      actionType: 'seller_withdraw_approve',
      targetUserId: marked.sellerTelegramId,
      details: { requestId: marked._id, amount: marked.amount, upiId: marked.upiId || null },
    });

    await SellerPayoutLedger.create({
      sellerTelegramId: marked.sellerTelegramId,
      entryType: 'debit',
      source: 'withdrawal_approved',
      amount: marked.amount,
      balanceAfter: Number(seller?.sellerStats?.availableBalance || 0),
      relatedWithdrawalRequestId: marked._id,
      note: `Withdrawal approved to UPI ${marked.upiId || 'N/A'}`,
      createdBy: adminTelegramId,
    });

    approvedRequest = marked;
  } finally {
    await session.endSession();
  }

  return approvedRequest;
};

const rejectSellerWithdrawal = async (requestId, adminTelegramId, note = '') => {
  assertSuperAdmin(adminTelegramId);

  const request = await SellerWithdrawalRequest.findById(requestId);
  if (!request) throw new Error('Withdrawal request not found.');
  if (request.status !== 'pending') throw new Error('Withdrawal request already processed.');

  request.status = 'rejected';
  request.reviewedAt = new Date();
  request.reviewedBy = adminTelegramId;
  request.note = note || '';
  await request.save();

  await AdminLog.create({
    adminId: adminTelegramId,
    actionType: 'seller_withdraw_reject',
    targetUserId: request.sellerTelegramId,
    details: { requestId: request._id, amount: request.amount, note: request.note },
  });

  return request;
};

module.exports = {
  processReferral,
  awardReferralBonus,
  registerSellerProgram,
  processSellerReferral,
  awardSellerCommission,
  getSellerProgramSummary,
  requestSellerWithdrawal,
  getSellerWithdrawalRequests,
  getPendingSellerWithdrawalRequests,
  getSellerWithdrawalHistory,
  getSellerPayoutLedgerHistory,
  approveSellerWithdrawal,
  rejectSellerWithdrawal,
};
