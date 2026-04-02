// src/services/cronService.js
// All scheduled cron jobs:
//   reminderScheduler    - expiry reminders at 7/3/1/0 days
//   expiryEnforcementHandler - remove expired users from groups
//   inactiveUserDetector - re-engage users inactive 30+ days
//   membershipMonitor    - resend invite / ban mismatched users
//   dailySummary         - nightly channel report at 23:59
//   offerExpiryChecker   - deactivate expired offers
//   pendingRequestReminderJob - remind users whose request is still pending

const cron = require('node-cron');
const fs = require('fs/promises');
const path = require('path');
const os = require('os');
const Subscription = require('../models/Subscription');
const User = require('../models/User');
const Offer = require('../models/Offer');
const Plan = require('../models/Plan');
const AdminLog = require('../models/AdminLog');
const Request = require('../models/Request');
const DailySummary = require('../models/DailySummary');
const CronLeaderLock = require('../models/CronLeaderLock');
const { syncUserStatusFromSubscriptions } = require('./subscriptionService');
const { buildDailySummary } = require('./analyticsService');
const { safeSend, isGroupMember, banFromGroup, renewalKeyboard } = require('../utils/telegramUtils');
const { SUPPORT_CONTACT } = require('./supportService');
const { addDays, formatDate, startOfToday } = require('../utils/dateUtils');
const { getGroupIdForCategory, normalizePlanCategory } = require('../utils/premiumGroups');
const logger = require('../utils/logger');

const CRON_TIMEZONE = process.env.CRON_TIMEZONE || 'Asia/Kolkata';
const REMINDER_CRON_SCHEDULES = (process.env.REMINDER_CRON_SCHEDULES || '15 9 * * *,0 20 * * *,15 23 * * *')
  .split(',')
  .map((schedule) => schedule.trim())
  .filter(Boolean);
const EXPIRY_ENFORCEMENT_CRON_SCHEDULES = (process.env.EXPIRY_ENFORCEMENT_CRON_SCHEDULES || '0 8 * * *,0 14 * * *,30 20 * * *')
  .split(',')
  .map((schedule) => schedule.trim())
  .filter(Boolean);
const PENDING_REQUEST_REMINDER_AFTER_HOURS = Math.max(1, parseInt(process.env.PENDING_REQUEST_REMINDER_AFTER_HOURS || '2', 10));
const PENDING_REQUEST_REMINDER_REPEAT_HOURS = Math.max(1, parseInt(process.env.PENDING_REQUEST_REMINDER_REPEAT_HOURS || '12', 10));
const CLEANUP_CRON_SCHEDULE = process.env.CLEANUP_CRON_SCHEDULE || '20 3 * * *';
const RETENTION_PENDING_REQUEST_DAYS = Math.max(1, parseInt(process.env.RETENTION_PENDING_REQUEST_DAYS || '14', 10));
const RETENTION_ADMIN_LOG_DAYS = Math.max(1, parseInt(process.env.RETENTION_ADMIN_LOG_DAYS || '90', 10));
const RETENTION_DAILY_SUMMARY_DAYS = Math.max(1, parseInt(process.env.RETENTION_DAILY_SUMMARY_DAYS || '180', 10));
const RETENTION_INVITE_METADATA_DAYS = Math.max(1, parseInt(process.env.RETENTION_INVITE_METADATA_DAYS || '7', 10));
const RETENTION_LOG_FILES_DAYS = Math.max(1, parseInt(process.env.RETENTION_LOG_FILES_DAYS || '30', 10));
const CRON_LEADER_LOCK_ENABLED = String(process.env.CRON_LEADER_LOCK_ENABLED || 'true').toLowerCase() === 'true';
const CRON_LEADER_LOCK_TTL_SECONDS = Math.max(30, parseInt(process.env.CRON_LEADER_LOCK_TTL_SECONDS || '120', 10));
const CRON_INSTANCE_ID = String(process.env.CRON_INSTANCE_ID || `${os.hostname()}:${process.pid}`);
let lastCleanupReport = null;

const REQUEST_CATEGORY_LABELS = {
  movie: 'Movie Premium',
  desi: 'Desi Premium',
  non_desi: 'Non Desi Premium',
};

const MONITOR_GROUP_LABELS = {
  movie: 'Movie Premium',
  desi: 'Desi Premium',
  non_desi: 'Non Desi Premium',
};

const getGroupLabelForMonitorLog = (groupId, category) => {
  const id = String(groupId || '');
  const envGroupMap = {
    [String(process.env.MOVIE_PREMIUM_GROUP_ID || '')]: MONITOR_GROUP_LABELS.movie,
    [String(process.env.DESI_PREMIUM_GROUP_ID || '')]: MONITOR_GROUP_LABELS.desi,
    [String(process.env.NON_DESI_PREMIUM_GROUP_ID || '')]: MONITOR_GROUP_LABELS.non_desi,
    [String(process.env.PREMIUM_GROUP_ID || '')]: 'Premium Group',
  };

  if (id && envGroupMap[id]) return envGroupMap[id];
  return MONITOR_GROUP_LABELS[normalizePlanCategory(category)] || 'Premium Group';
};

const getRequestCategoryLabel = (category) => {
  return REQUEST_CATEGORY_LABELS[String(category || 'movie').toLowerCase()] || REQUEST_CATEGORY_LABELS.movie;
};

const logCronTimeSnapshot = () => {
  const now = new Date();
  const localTime = now.toString();
  const istTime = now.toLocaleString('en-GB', { timeZone: 'Asia/Kolkata', hour12: false });
  const configuredTzTime = now.toLocaleString('en-GB', { timeZone: CRON_TIMEZONE, hour12: false });

  logger.info(`[CRON] Server local time: ${localTime}`);
  logger.info(`[CRON] IST time: ${istTime} (Asia/Kolkata)`);
  logger.info(`[CRON] ${CRON_TIMEZONE} time: ${configuredTzTime}`);
};

const getSubscriptionGroupId = (sub) => {
  if (sub?.premiumGroupId) return String(sub.premiumGroupId);
  return getGroupIdForCategory(sub?.planCategory || 'movie');
};

const getRenewalPlansByCategory = async (category) => {
  const normalizedCategory = normalizePlanCategory(category);
  return Plan.find({ isActive: true, category: normalizedCategory }).sort({ durationDays: 1 });
};

/**
 * Strip PII and payment-proof references from backup JSON (log channel + disk).
 * Set DAILY_BACKUP_INCLUDE_USER_IDS=true only on trusted infra to restore legacy full export.
 */
const sanitizeDailyBackupPayload = (payload) => {
  const includeIds = String(process.env.DAILY_BACKUP_INCLUDE_USER_IDS || 'false').toLowerCase() === 'true';
  if (includeIds) {
    return { ...payload, _meta: { redacted: false } };
  }

  const activeSubscriptions = (payload.activeSubscriptions || []).map((s) => ({
    planName: s.planName,
    planCategory: s.planCategory,
    premiumGroupId: s.premiumGroupId,
    startDate: s.startDate,
    expiryDate: s.expiryDate,
    status: s.status,
  }));

  const pendingRequests = (payload.pendingRequests || []).map((r) => ({
    _id: r._id,
    requestCategory: r.requestCategory,
    requestDate: r.requestDate,
  }));

  return {
    ...payload,
    activeSubscriptions,
    pendingRequests,
    _meta: {
      redacted: true,
      note:
        'telegramId, usernames, names, proof file IDs, and admin IDs removed. Set DAILY_BACKUP_INCLUDE_USER_IDS=true for full export (trusted channel only).',
    },
  };
};

const buildDailyBackupPayload = async (summary) => {
  const now = new Date();
  const [
    totalUsers,
    activeUsers,
    blockedUsers,
    activeSubscriptions,
    pendingRequests,
    activePlans,
  ] = await Promise.all([
    User.countDocuments({ role: 'user' }),
    User.countDocuments({ role: 'user', status: 'active' }),
    User.countDocuments({ role: 'user', isBlocked: true }),
    Subscription.find({ status: 'active', expiryDate: { $gt: now } })
      .select('telegramId planName planCategory premiumGroupId startDate expiryDate status approvedBy')
      .sort({ expiryDate: 1 })
      .lean(),
    Request.find({ status: { $in: ['pending', 'processing'] } })
      .select('telegramId username name requestCategory requestDate proofPhotoFileId proofDocumentFileId')
      .sort({ requestDate: 1 })
      .lean(),
    Plan.find({ isActive: true })
      .select('name category durationDays price isActive')
      .sort({ category: 1, durationDays: 1 })
      .lean(),
  ]);

  return {
    generatedAt: now.toISOString(),
    timezone: CRON_TIMEZONE,
    dailySummary: summary,
    counters: {
      totalUsers,
      activeUsers,
      blockedUsers,
      activeSubscriptions: activeSubscriptions.length,
      pendingRequests: pendingRequests.length,
      activePlans: activePlans.length,
    },
    activeSubscriptions,
    pendingRequests,
    activePlans,
  };
};

const sendDailyBackupToLogChannel = async (bot, summary) => {
  try {
    if (!process.env.LOG_CHANNEL_ID) {
      logger.warn('sendDailyBackupToLogChannel skipped: LOG_CHANNEL_ID is not configured');
      return;
    }

    const rawPayload = await buildDailyBackupPayload(summary);
    const payload = sanitizeDailyBackupPayload(rawPayload);
    const dateStamp = new Date().toISOString().slice(0, 10);
    const fileName = `daily-backup-${dateStamp}.json`;
    const json = JSON.stringify(payload, null, 2);

    const logsDir = path.resolve(process.cwd(), 'logs');
    await fs.mkdir(logsDir, { recursive: true });
    await fs.writeFile(path.join(logsDir, fileName), json, 'utf8');

    await bot.telegram.sendDocument(
      process.env.LOG_CHANNEL_ID,
      { source: Buffer.from(json, 'utf8'), filename: fileName },
      {
        caption:
          `🗂 *Daily Backup*\n` +
          `Date: ${dateStamp}\n` +
          `Active Subs: ${payload.counters.activeSubscriptions}\n` +
          `Pending Requests: ${payload.counters.pendingRequests}`,
        parse_mode: 'Markdown',
      }
    );

    logger.info(`Daily backup sent to log channel: ${fileName}`);
  } catch (err) {
    logger.error(`sendDailyBackupToLogChannel error: ${err.message}`);
  }
};

const cutoffDaysAgo = (days) => new Date(Date.now() - days * 24 * 60 * 60 * 1000);

const collectOldLogFilesCount = async (fileCutoffMs, shouldDelete) => {
  let affected = 0;
  try {
    const logsDir = path.resolve(process.cwd(), 'logs');
    const names = await fs.readdir(logsDir);
    for (const name of names) {
      const full = path.join(logsDir, name);
      const stat = await fs.stat(full).catch(() => null);
      if (!stat || !stat.isFile()) continue;

      const lower = String(name).toLowerCase();
      const isRetentionCandidate =
        lower.endsWith('.log')
        || lower.endsWith('.json')
        || lower.endsWith('.csv');
      if (!isRetentionCandidate) continue;

      if (stat.mtimeMs < fileCutoffMs) {
        affected += 1;
        if (shouldDelete) {
          await fs.unlink(full).catch(() => { });
        }
      }
    }
  } catch (err) {
    logger.warn(`cleanupRetentionJob file cleanup skipped: ${err.message}`);
  }
  return affected;
};

const cleanupRetentionJob = async (options = {}) => {
  const dryRun = Boolean(options.dryRun);
  logger.info(`[CRON] Running cleanupRetentionJob (dryRun=${dryRun})...`);
  const pendingCutoff = cutoffDaysAgo(RETENTION_PENDING_REQUEST_DAYS);
  const adminLogCutoff = cutoffDaysAgo(RETENTION_ADMIN_LOG_DAYS);
  const summaryCutoff = cutoffDaysAgo(RETENTION_DAILY_SUMMARY_DAYS);
  const inviteCutoff = cutoffDaysAgo(RETENTION_INVITE_METADATA_DAYS);
  const fileCutoffMs = cutoffDaysAgo(RETENTION_LOG_FILES_DAYS).getTime();
  const pendingQuery = { status: 'pending', requestDate: { $lt: pendingCutoff } };
  const logsQuery = { timestamp: { $lt: adminLogCutoff } };
  const summaryQuery = { date: { $lt: summaryCutoff } };
  const inviteQuery = {
    inviteLinkIssuedAt: { $ne: null, $lt: inviteCutoff },
    $or: [
      { inviteLink: { $ne: null } },
      { inviteLinkTtlMinutes: { $ne: null } },
      { status: { $in: ['expired', 'cancelled'] } },
    ],
  };

  let stalePendingCount = 0;
  let oldLogsCount = 0;
  let oldSummariesCount = 0;
  let inviteCleanupCount = 0;
  let removedFiles = 0;

  if (dryRun) {
    [stalePendingCount, oldLogsCount, oldSummariesCount, inviteCleanupCount, removedFiles] = await Promise.all([
      Request.countDocuments(pendingQuery),
      AdminLog.countDocuments(logsQuery),
      DailySummary.countDocuments(summaryQuery),
      Subscription.countDocuments(inviteQuery),
      collectOldLogFilesCount(fileCutoffMs, false),
    ]);
  } else {
    const stalePending = await Request.updateMany(
      pendingQuery,
      {
        $set: {
          status: 'rejected',
          actionDate: new Date(),
          actionBy: 0,
          pendingReminderAt: null,
        },
      }
    );
    stalePendingCount = Number(stalePending.modifiedCount || 0);

    const oldLogs = await AdminLog.deleteMany(logsQuery);
    oldLogsCount = Number(oldLogs.deletedCount || 0);

    const oldSummaries = await DailySummary.deleteMany(summaryQuery);
    oldSummariesCount = Number(oldSummaries.deletedCount || 0);

    const inviteCleanup = await Subscription.updateMany(
      inviteQuery,
      {
        $set: {
          inviteLink: null,
          inviteLinkIssuedAt: null,
          inviteLinkTtlMinutes: null,
        },
      }
    );
    inviteCleanupCount = Number(inviteCleanup.modifiedCount || 0);
    removedFiles = await collectOldLogFilesCount(fileCutoffMs, true);
  }

  const report = {
    ranAt: new Date(),
    dryRun,
    stalePendingCount,
    oldLogsCount,
    oldSummariesCount,
    inviteCleanupCount,
    removedFiles,
  };
  lastCleanupReport = report;

  logger.info(
    `[CRON] cleanupRetentionJob done (dryRun=${dryRun}): pending=${stalePendingCount}, ` +
    `adminLogs=${oldLogsCount}, summaries=${oldSummariesCount}, ` +
    `inviteMetadata=${inviteCleanupCount}, files=${removedFiles}`
  );
  return report;
};

const getLastCleanupReport = () => lastCleanupReport;

const runAsLeader = async (jobName, runner) => {
  if (!CRON_LEADER_LOCK_ENABLED) return runner();

  const now = new Date();
  const expiresAt = new Date(now.getTime() + CRON_LEADER_LOCK_TTL_SECONDS * 1000);
  const lockDoc = await CronLeaderLock.findOneAndUpdate(
    {
      jobName,
      $or: [{ expiresAt: { $lt: now } }, { ownerId: CRON_INSTANCE_ID }],
    },
    {
      $set: { ownerId: CRON_INSTANCE_ID, expiresAt, updatedAt: now },
      $setOnInsert: { jobName },
    },
    { new: true, upsert: true }
  ).lean();

  if (!lockDoc || lockDoc.ownerId !== CRON_INSTANCE_ID) return null;
  return runner();
};

// ── Helper: post to log channel ──────────────────────────────────────────────
const logToChannel = async (bot, message) => {
  try {
    await bot.telegram.sendMessage(process.env.LOG_CHANNEL_ID, message, {
      parse_mode: 'Markdown',
    });
  } catch (err) {
    logger.warn(`logToChannel failed: ${err.message}`);
  }
};

// ── 1. REMINDER SCHEDULER ───────────────────────────────────────────────────
// Runs daily at 9:00 AM
// Sends reminders at 7, 3, 1 day before expiry and on expiry day
const reminderScheduler = async (bot) => {
  logger.info('[CRON] Running reminderScheduler...');

  const checkpoints = [
    { days: 7, flag: 'day7', label: '7 days' },
    { days: 3, flag: 'day3', label: '3 days' },
    { days: 1, flag: 'day1', label: '1 day' },
    { days: 0, flag: 'day0', label: 'today' },
  ];

  for (const { days, flag, label } of checkpoints) {
    const targetStart = addDays(startOfToday(), days);
    const targetEnd = new Date(targetStart);
    targetEnd.setHours(23, 59, 59, 999);

    const subs = await Subscription.find({
      status: 'active',
      expiryDate: { $gte: targetStart, $lte: targetEnd },
      [`reminderFlags.${flag}`]: false,
    });

    for (const sub of subs) {
      const plans = await getRenewalPlansByCategory(sub.planCategory || 'movie');
      const sent = await safeSend(
        bot,
        sub.telegramId,
        `⏰ *Subscription Reminder*\n\n` +
        `Your subscription expires in *${days === 0 ? 'less than 24 hours' : label}*!\n` +
        `📅 Expiry date: *${formatDate(sub.expiryDate)}*\n\n` +
        `Tap a button below to renew:`,
        {
          parse_mode: 'Markdown',
          reply_markup: renewalKeyboard(plans, sub.planCategory || 'movie'),
        }
      );

      if (sent) {
        sub.reminderFlags[flag] = true;
        await sub.save();
        logger.info(`Reminder (${label}) sent to user ${sub.telegramId}`);
      }
    }
  }
};

// ── 2. EXPIRY ENFORCEMENT HANDLER ───────────────────────────────────────────
// Runs multiple times daily
// Immediately expires overdue active subscriptions and removes group access.
const expiryEnforcementHandler = async (bot) => {
  logger.info('[CRON] Running expiryEnforcementHandler...');
  const today = startOfToday();

  // Find active subscriptions whose expiry has passed.
  const justExpired = await Subscription.find({
    status: 'active',
    expiryDate: { $lt: today },
  });

  for (const sub of justExpired) {
    sub.status = 'expired';
    await sub.save();
    await syncUserStatusFromSubscriptions(sub.telegramId);

    const groupId = getSubscriptionGroupId(sub);
    if (groupId) {
      await banFromGroup(bot, groupId, sub.telegramId);
    }

    let renewalPlans = [];
    if (sub.planId) {
      const samePlan = await Plan.find({ _id: sub.planId, isActive: true });
      renewalPlans = samePlan;
    }
    if (!renewalPlans.length) {
      renewalPlans = await getRenewalPlansByCategory(sub.planCategory || 'movie');
    }

    await safeSend(bot, sub.telegramId,
      `❌ *Subscription Expired*\n\nYour subscription expired on *${formatDate(sub.expiryDate)}*.\n\n` +
      `🚫 Premium group access has been removed. Renew now to continue access.`,
      {
        parse_mode: 'Markdown',
        ...(renewalPlans.length ? { reply_markup: renewalKeyboard(renewalPlans, sub.planCategory || 'movie') } : {}),
      }
    );

    await logToChannel(bot, `🚫 *Expired User Access Removed*\nUser: \`${sub.telegramId}\`\nPlan: ${sub.planName}`);

    await AdminLog.create({
      adminId: 0,
      actionType: 'ban_user',
      targetUserId: sub.telegramId,
      details: { reason: 'Subscription expired' },
    });
  }
};

// ── 3. INACTIVE USER DETECTOR ─────────────────────────────────────────────────
// Runs daily at 10:00 AM
// Re-engage users inactive 30+ days OR expired 7+ days
const inactiveUserDetector = async (bot) => {
  logger.info('[CRON] Running inactiveUserDetector...');
  const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
  const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);

  const inactiveUsers = await User.find({
    role: 'user',
    isBlocked: false,
    $or: [
      { lastInteraction: { $lt: thirtyDaysAgo }, status: 'active' },
      { status: 'expired', updatedAt: { $lt: sevenDaysAgo } },
    ],
  });

  let contacted = 0;
  for (const user of inactiveUsers) {
    const sent = await safeSend(bot, user.telegramId,
      `👋 *We miss you, ${user.name}!*\n\n` +
      `It's been a while since we've seen you. We have a special offer waiting for you!\n\n` +
      `Tap below to see what's available:`,
      {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [{ text: '🎁 View Special Offers', callback_data: 'view_offers' }],
            [{ text: '🌟 Request Access', callback_data: 'request_access' }],
          ],
        },
      }
    );
    if (sent) contacted++;
  }

  if (contacted > 0) {
    logger.info(`inactiveUserDetector: ${contacted} re-engagement messages sent`);
  }
};

// ── 4. MEMBERSHIP MONITOR ─────────────────────────────────────────────────────
// Runs daily at 11:00 AM
// - Active sub but not in group → ask user to contact support
// - Expired but still in group → ban
const membershipMonitor = async (bot) => {
  logger.info('[CRON] Running membershipMonitor...');

  // Active subscriptions
  const activeSubs = await Subscription.find({
    status: 'active',
    expiryDate: { $gt: new Date() },
  });

  for (const sub of activeSubs) {
    const groupId = getSubscriptionGroupId(sub);
    if (!groupId) continue;
    const inGroup = await isGroupMember(bot, groupId, sub.telegramId);
    if (!inGroup) {
      await safeSend(bot, sub.telegramId,
        `⚠️ *Rejoining Required*\n\nYou have an active subscription but aren't in the premium group.\n\nPlease contact support to get rejoined: /support`,
        { parse_mode: 'Markdown' }
      );
      logger.info(`Sent support rejoin instruction to active user ${sub.telegramId}`);
    }
  }

  // Expired users still in group
  const [expiredSubs, adminAccounts] = await Promise.all([
    Subscription.find({ status: 'expired' })
      .select('telegramId planCategory premiumGroupId expiryDate')
      .lean(),
    User.find({ role: { $in: ['admin', 'superadmin'] } }).select('telegramId').lean(),
  ]);

  const adminIdSet = new Set(adminAccounts.map((user) => String(user.telegramId)));
  const removedUsers = [];

  const processedKeys = new Set();

  for (const expiredSub of expiredSubs) {
    const telegramId = expiredSub.telegramId;
    if (adminIdSet.has(String(telegramId))) continue;

    const groupId = getSubscriptionGroupId(expiredSub);
    if (!groupId) continue;
    const dedupeKey = `${telegramId}:${groupId}`;
    if (processedKeys.has(dedupeKey)) continue;
    processedKeys.add(dedupeKey);

    const hasActiveForGroup = await Subscription.exists({
      telegramId,
      status: 'active',
      expiryDate: { $gt: new Date() },
      $or: [
        { premiumGroupId: String(groupId) },
        {
          premiumGroupId: { $in: [null, ''] },
          planCategory: normalizePlanCategory(expiredSub.planCategory || 'movie'),
        },
      ],
    });
    if (hasActiveForGroup) continue;

    const inGroup = await isGroupMember(bot, groupId, telegramId);
    if (inGroup) {
      await banFromGroup(bot, groupId, telegramId);
      removedUsers.push({
        telegramId: String(telegramId),
        groupName: getGroupLabelForMonitorLog(groupId, expiredSub?.planCategory),
      });
    }
  }

  if (removedUsers.length) {
    const lines = removedUsers.map((item) => `${item.telegramId} | ${item.groupName}`).join('\n');
    await logToChannel(
      bot,
      `🚫 *Expired Users Removed by Monitor*\n\n\`\`\`\nUser_id | Group Name\n${lines}\n\`\`\``
    );
  }
};

// ── 5. DAILY SUMMARY ─────────────────────────────────────────────────────────
// Runs daily at 23:59
// Posts a full activity summary to the log channel
const dailySummaryJob = async (bot) => {
  logger.info('[CRON] Running dailySummaryJob...');
  try {
    const summary = await buildDailySummary();
    const today = new Date().toLocaleDateString('en-GB');

    await logToChannel(bot,
      `📊 *Daily Activity Summary — ${today}*\n\n` +
      `👤 New Users: ${summary.newUsers}\n` +
      `📩 Requests Received: ${summary.requestsReceived}\n` +
      `✅ Approvals: ${summary.approvals}\n` +
      `🔄 Renewals: ${summary.renewals}\n` +
      `❌ Expired Today: ${summary.expiredToday}\n`
    );

    await sendDailyBackupToLogChannel(bot, summary);
  } catch (err) {
    logger.error(`dailySummaryJob error: ${err.message}`);
  }
};

// ── 6. OFFER EXPIRY CHECKER ───────────────────────────────────────────────────
// Runs daily at 00:05
// Deactivates offers whose validTill has passed
const offerExpiryChecker = async () => {
  logger.info('[CRON] Running offerExpiryChecker...');
  const result = await Offer.updateMany(
    { isActive: true, validTill: { $lt: new Date() } },
    { isActive: false }
  );
  if (result.modifiedCount > 0) {
    logger.info(`offerExpiryChecker: ${result.modifiedCount} offers deactivated`);
  }
};

// ── 8. PENDING REQUEST REMINDER ────────────────────────────────────────────
// Runs every 2 hours
// Reminds users that their premium request is still pending review
const pendingRequestReminderJob = async (bot) => {
  logger.info('[CRON] Running pendingRequestReminderJob...');

  const now = new Date();
  const minAgeCutoff = new Date(now.getTime() - PENDING_REQUEST_REMINDER_AFTER_HOURS * 60 * 60 * 1000);
  const repeatCutoff = new Date(now.getTime() - PENDING_REQUEST_REMINDER_REPEAT_HOURS * 60 * 60 * 1000);

  const pendingRequests = await Request.find({
    status: 'pending',
    requestDate: { $lte: minAgeCutoff },
    $or: [
      { pendingReminderAt: null },
      { pendingReminderAt: { $lte: repeatCutoff } },
    ],
  })
    .select('_id telegramId requestCategory requestDate pendingReminderAt')
    .sort({ requestDate: 1 })
    .lean();

  let remindersSent = 0;

  for (const request of pendingRequests) {
    const sent = await safeSend(
      bot,
      request.telegramId,
      `⏳ *Request Update*\n\n` +
      `Aapka *${getRequestCategoryLabel(request.requestCategory)}* request abhi review me hai.\n` +
      `Admin team jaldi verify karegi.\n\n` +
      `Agar urgent ho to /support use karein.`,
      { parse_mode: 'Markdown' }
    );

    if (!sent) continue;

    remindersSent += 1;
    await Request.findByIdAndUpdate(request._id, { pendingReminderAt: new Date() });
  }

  if (remindersSent > 0) {
    logger.info(`pendingRequestReminderJob: ${remindersSent} reminders sent`);
  }
};

// ── 7. INVITE LINK EXPIRY NOTIFIER ───────────────────────────────────────────
// Runs every 15 minutes
// If invite link expired and user still not in group, notify user once and guide next step.
const inviteLinkExpiryNotifier = async (bot) => {
  logger.info('[CRON] Running inviteLinkExpiryNotifier...');
  const now = new Date();

  const subsWithInvite = await Subscription.find({
    status: 'active',
    inviteLink: { $ne: null },
    inviteLinkIssuedAt: { $ne: null },
  }).select('telegramId planName inviteLink inviteLinkIssuedAt inviteLinkTtlMinutes');

  for (const sub of subsWithInvite) {
    const ttlMinutes = Math.max(1, parseInt(sub.inviteLinkTtlMinutes || process.env.INVITE_LINK_TTL_MINUTES || '10', 10));
    const expiresAt = new Date(sub.inviteLinkIssuedAt.getTime() + ttlMinutes * 60 * 1000);
    if (now <= expiresAt) continue;

    const fullSub = await Subscription.findById(sub._id);
    const groupId = getSubscriptionGroupId(fullSub);
    if (!groupId) continue;

    const inGroup = await isGroupMember(bot, groupId, sub.telegramId);
    if (inGroup) {
      await Subscription.findByIdAndUpdate(sub._id, {
        inviteLink: null,
        inviteLinkIssuedAt: null,
        inviteLinkTtlMinutes: null,
      });
      continue;
    }

    const sent = await safeSend(
      bot,
      sub.telegramId,
      `⌛ *Your invite link has expired.*\n\n` +
      `Naya invite link lene ke liye support se contact karein\n\n` +
      `/support command use karein.`,
      { parse_mode: 'Markdown' }
    );

    if (sent) {
      await Subscription.findByIdAndUpdate(sub._id, {
        inviteLink: null,
        inviteLinkIssuedAt: null,
        inviteLinkTtlMinutes: null,
      });

      await logToChannel(
        bot,
        `⚠️ *Expired Invite link Notice*\n` +
        `UserID: \`${sub.telegramId}\`\n` +
        `Plan subscribed: *${sub.planName || 'N/A'}*`
      );

      logger.info(`Expired invite notice sent to ${sub.telegramId}`);
    }
  }
};

// ── INIT: Register all cron schedules ─────────────────────────────────────────
const initCronJobs = (bot) => {
  logCronTimeSnapshot();
  const cronOptions = { timezone: CRON_TIMEZONE };

  for (const schedule of REMINDER_CRON_SCHEDULES) {
    cron.schedule(schedule, () => runAsLeader('reminderScheduler', () => reminderScheduler(bot)), cronOptions);
  }
  for (const schedule of EXPIRY_ENFORCEMENT_CRON_SCHEDULES) {
    cron.schedule(schedule, () => runAsLeader('expiryEnforcementHandler', () => expiryEnforcementHandler(bot)), cronOptions);
  }
  cron.schedule('0 10 * * *', () => runAsLeader('inactiveUserDetector', () => inactiveUserDetector(bot)), cronOptions); // 10:00 AM
  cron.schedule('0 11 * * *', () => runAsLeader('membershipMonitor', () => membershipMonitor(bot)), cronOptions);    // 11:00 AM
  cron.schedule('59 23 * * *', () => runAsLeader('dailySummaryJob', () => dailySummaryJob(bot)), cronOptions);     // 23:59
  cron.schedule('5 0 * * *', () => runAsLeader('offerExpiryChecker', () => offerExpiryChecker()), cronOptions);       // 00:05
  cron.schedule('*/15 * * * *', () => runAsLeader('inviteLinkExpiryNotifier', () => inviteLinkExpiryNotifier(bot)), cronOptions); // every 15 min
  cron.schedule('0 */2 * * *', () => runAsLeader('pendingRequestReminderJob', () => pendingRequestReminderJob(bot)), cronOptions); // every 2 hours
  cron.schedule(CLEANUP_CRON_SCHEDULE, () => runAsLeader('cleanupRetentionJob', () => cleanupRetentionJob()), cronOptions); // retention cleanup

  logger.info(`Reminder schedules initialized: ${REMINDER_CRON_SCHEDULES.join(' | ')}`);
  logger.info(`Expiry enforcement schedules initialized: ${EXPIRY_ENFORCEMENT_CRON_SCHEDULES.join(' | ')}`);
  logger.info(`Cleanup schedule initialized: ${CLEANUP_CRON_SCHEDULE}`);
  logger.info(`Cron leader lock: ${CRON_LEADER_LOCK_ENABLED ? `enabled (${CRON_INSTANCE_ID})` : 'disabled'}`);
  logger.info(`All cron jobs initialized. Timezone: ${CRON_TIMEZONE}`);
};

module.exports = {
  initCronJobs,
  logToChannel,
  reminderScheduler,
  expiryEnforcementHandler,
  inactiveUserDetector,
  membershipMonitor,
  dailySummaryJob,
  offerExpiryChecker,
  inviteLinkExpiryNotifier,
  pendingRequestReminderJob,
  cleanupRetentionJob,
  getLastCleanupReport,
};
