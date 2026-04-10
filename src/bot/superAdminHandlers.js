// src/bot/superAdminHandlers.js
// Super admin: plan/offer/admin management, broadcast, reports, stats, planstats

const User = require('../models/User');
const Plan = require('../models/Plan');
const Subscription = require('../models/Subscription');
const Request = require('../models/Request');
const AdminLog = require('../models/AdminLog');
const mongoose = require('mongoose');
const SellerWithdrawalRequest = require('../models/SellerWithdrawalRequest');
const SellerPayoutLedger = require('../models/SellerPayoutLedger');
const UserOffer = require('../models/UserOffer');
const { normalizePlanCategory, PLAN_CATEGORY } = require('../utils/premiumGroups');
const {
  addAdmin, removeAdmin, createPlan, updatePlan, deletePlan,
  getAllPlans, getActivePlans, createOffer, deleteOffer, getActiveOffers
} = require('../services/adminService');
const {
  getSalesReport,
  getSalesUserBreakdown,
  getTodayExpiryList,
  syncUserStatusFromSubscriptions,
} = require('../services/subscriptionService');
const { getGrowthStats, getCategoryWiseStats, getPlanPerformance } = require('../services/analyticsService');
const {
  getSellerWithdrawalRequests,
  getPendingSellerWithdrawalRequests,
  approveSellerWithdrawal,
  rejectSellerWithdrawal,
} = require('../services/referralService');
const { logToChannel, cleanupRetentionJob, getLastCleanupReport } = require('../services/cronService');
const { safeSend, scheduleDeleteMessage } = require('../utils/telegramUtils');
const { formatDate, startOfToday, endOfToday, startOfWeek, startOfMonth } = require('../utils/dateUtils');
const {
  startBroadcastSession,
  getBroadcastSession,
  consumeBroadcastSession,
  clearBroadcastSession,
} = require('../utils/broadcastSession');
const logger = require('../utils/logger');
const { escapeMarkdown } = require('../utils/markdownEscape');

const persistedReportMessagesByChat = new Map();
const ADMIN_ACTION_CONFIRM_TTL_SECONDS = Math.max(10, parseInt(process.env.ADMIN_ACTION_CONFIRM_TTL_SECONDS || '45', 10));
const pendingSuperAdminCommandConfirmations = new Map();
const isLikelyTelegramGroupId = (value) => /^-?\d+$/.test(String(value || '').trim());

const getSuperAdminIds = () => {
  return String(process.env.SUPER_ADMIN_IDS || process.env.SUPER_ADMIN_ID || '')
    .split(',')
    .map(id => parseInt(id.trim(), 10))
    .filter(Boolean);
};

const requireSuperAdmin = async (ctx, next) => {
  const superAdminIds = getSuperAdminIds();
  if (!superAdminIds.includes(ctx.from.id)) {
    return ctx.reply('⛔ Super Admin access required.');
  }
  return next();
};

const getChatIdFromCtx = (ctx) => {
  return ctx.chat?.id
    || ctx.callbackQuery?.message?.chat?.id
    || ctx.message?.chat?.id
    || null;
};

const rememberPersistedReportMessage = (chatId, messageId) => {
  if (!chatId || !messageId) return;
  const key = String(chatId);
  const existing = persistedReportMessagesByChat.get(key) || [];
  existing.push(messageId);
  persistedReportMessagesByChat.set(key, existing);
};

const clearPersistedReportMessages = async (ctx) => {
  const chatId = getChatIdFromCtx(ctx);
  if (!chatId) return;

  const key = String(chatId);
  const messageIds = persistedReportMessagesByChat.get(key);
  if (!messageIds?.length) return;

  for (const messageId of messageIds) {
    scheduleDeleteMessage(ctx.telegram, chatId, messageId);
  }
  persistedReportMessagesByChat.delete(key);
};

const requireSuperAdminActionConfirmation = async (ctx, commandName, commandArgs, summaryText) => {
  const argsText = String(commandArgs || '').trim().replace(/\s+/g, ' ');
  const key = `${ctx.from?.id || 0}:${commandName}:${argsText}`;
  const now = Date.now();
  const existing = pendingSuperAdminCommandConfirmations.get(key);

  if (existing && existing > now) {
    pendingSuperAdminCommandConfirmations.delete(key);
    return true;
  }

  pendingSuperAdminCommandConfirmations.set(key, now + (ADMIN_ACTION_CONFIRM_TTL_SECONDS * 1000));
  await ctx.reply(
    `⚠️ *Confirmation Required*\n\n` +
    `${summaryText}\n\n` +
    `Please run the same command again within *${ADMIN_ACTION_CONFIRM_TTL_SECONDS} seconds* to confirm.`,
    { parse_mode: 'Markdown' }
  );
  return false;
};

const inspectPremiumGroupConfig = () => {
  const categoryMap = {
    movie: process.env.MOVIE_PREMIUM_GROUP_ID,
    desi: process.env.DESI_PREMIUM_GROUP_ID,
    non_desi: process.env.NON_DESI_PREMIUM_GROUP_ID,
  };
  const fallback = process.env.PREMIUM_GROUP_ID;

  const warnings = [];
  const errors = [];

  Object.entries(categoryMap).forEach(([category, groupId]) => {
    if (groupId && !isLikelyTelegramGroupId(groupId)) {
      errors.push(`${category}: invalid ID format (${groupId})`);
    }
  });
  if (fallback && !isLikelyTelegramGroupId(fallback)) {
    errors.push(`fallback: invalid ID format (${fallback})`);
  }

  const configured = Object.entries(categoryMap).filter(([, v]) => Boolean(v));
  const byGroup = new Map();
  configured.forEach(([category, groupId]) => {
    const key = String(groupId);
    byGroup.set(key, [...(byGroup.get(key) || []), category]);
  });
  for (const [groupId, categories] of byGroup.entries()) {
    if (categories.length > 1) {
      errors.push(`duplicate mapping: ${categories.join(', ')} -> ${groupId}`);
    }
  }

  const missingCategories = Object.entries(categoryMap)
    .filter(([, v]) => !v)
    .map(([k]) => k);

  if (missingCategories.length && fallback) {
    warnings.push(`fallback PREMIUM_GROUP_ID is active for: ${missingCategories.join(', ')}`);
  }
  if (missingCategories.length && !fallback) {
    warnings.push(`missing category group IDs (no fallback): ${missingCategories.join(', ')}`);
  }
  if (Object.values(categoryMap).every(Boolean) && fallback) {
    warnings.push('fallback PREMIUM_GROUP_ID is set but not required');
  }

  return { categoryMap, fallback, warnings, errors };
};

const registerSuperAdminHandlers = (bot) => {

  bot.use(async (ctx, next) => {
    if (ctx.chat?.type !== 'private') return next();
    if (!isSuperAdminUser(ctx.from?.id)) return next();

    const text = String(ctx.message?.text || '').trim();
    if (text.startsWith('/')) {
      await clearPersistedReportMessages(ctx);
    }

    return next();
  });

  const formatUptime = (secondsRaw) => {
    const totalSeconds = Math.max(0, Math.floor(Number(secondsRaw) || 0));
    const days = Math.floor(totalSeconds / 86400);
    const hours = Math.floor((totalSeconds % 86400) / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;
    if (days > 0) return `${days}d ${hours}h ${minutes}m ${seconds}s`;
    if (hours > 0) return `${hours}h ${minutes}m ${seconds}s`;
    return `${minutes}m ${seconds}s`;
  };

  const isSuperAdminUser = (telegramId) => {
    const superAdminIds = getSuperAdminIds();
    return superAdminIds.includes(telegramId);
  };

  // ── Admin management ───────────────────────────────────────────────────────
  bot.command('addadmin', requireSuperAdmin, async (ctx) => {
    const id = parseInt(ctx.message.text.split(' ')[1]);
    if (!id) return ctx.reply('Usage: /addadmin <telegramId>');
    try {
      const user = await addAdmin(id);
      await AdminLog.create({ adminId: ctx.from.id, actionType: 'add_admin', targetUserId: id, details: {} });
      await logToChannel(bot, `👑 Admin Added: \`${id}\` (@${escapeMarkdown(user.username || 'N/A')})`);
      await ctx.reply(`✅ *${escapeMarkdown(user.name)}* is now an Admin.`, { parse_mode: 'Markdown' });
    } catch (err) { await ctx.reply(`❌ ${err.message}`); }
  });

  bot.command('removeadmin', requireSuperAdmin, async (ctx) => {
    const id = parseInt(ctx.message.text.split(' ')[1]);
    if (!id) return ctx.reply('Usage: /removeadmin <telegramId>');
    try {
      const user = await removeAdmin(id);
      await AdminLog.create({ adminId: ctx.from.id, actionType: 'remove_admin', targetUserId: id, details: {} });
      await ctx.reply(`✅ *${escapeMarkdown(user.name)}* is no longer an Admin.`, { parse_mode: 'Markdown' });
    } catch (err) { await ctx.reply(`❌ ${err.message}`); }
  });

  bot.command('admins', requireSuperAdmin, async (ctx) => {
    const admins = await User.find({ role: 'admin' });
    if (!admins.length) return ctx.reply('No admins found.');
    let msg = '👑 *Admin List*\n\n';
    admins.forEach((a, i) => {
      msg += `${i + 1}. ${escapeMarkdown(a.name)} — \`${a.telegramId}\`${a.username ? ' (@' + escapeMarkdown(a.username) + ')' : ''}\n`;
    });
    await ctx.reply(msg, { parse_mode: 'Markdown' });
  });

  // ── /configcheck — validate premium group mapping/env health ──────────────
  bot.command('configcheck', requireSuperAdmin, async (ctx) => {
    const report = inspectPremiumGroupConfig();

    let msg = `🧪 *Config Check*\n\n`;
    msg += `*Category Group Mapping*\n`;
    msg += `• movie: \`${report.categoryMap.movie || 'not_set'}\`\n`;
    msg += `• desi: \`${report.categoryMap.desi || 'not_set'}\`\n`;
    msg += `• non\\_desi: \`${report.categoryMap.non_desi || 'not_set'}\`\n`;
    msg += `• fallback PREMIUM\\_GROUP\\_ID: \`${report.fallback || 'not_set'}\`\n`;

    msg += `\n*Validation*\n`;
    if (!report.errors.length && !report.warnings.length) {
      msg += `✅ No issues detected.`;
    } else {
      if (report.errors.length) {
        msg += `❌ *Errors*\n`;
        report.errors.forEach((line) => { msg += `- ${escapeMarkdown(line)}\n`; });
      }
      if (report.warnings.length) {
        msg += `${report.errors.length ? '\n' : ''}⚠️ *Warnings*\n`;
        report.warnings.forEach((line) => { msg += `- ${escapeMarkdown(line)}\n`; });
      }
    }

    await ctx.reply(msg, { parse_mode: 'Markdown' });
  });

  // ── /cleanupdryrun + /cleanuprun + /cleanupreport ────────────────────────
  bot.command('cleanupdryrun', requireSuperAdmin, async (ctx) => {
    try {
      const report = await cleanupRetentionJob({ dryRun: true });
      await ctx.reply(
        `🧪 *Cleanup Dry-Run*\n\n` +
        `Stale pending requests: *${report.stalePendingCount}*\n` +
        `Old admin logs: *${report.oldLogsCount}*\n` +
        `Old daily summaries: *${report.oldSummariesCount}*\n` +
        `Invite metadata rows: *${report.inviteCleanupCount}*\n` +
        `Old log files: *${report.removedFiles}*`,
        { parse_mode: 'Markdown' }
      );
    } catch (err) {
      logger.error(`cleanupdryrun command error: ${err.message}`);
      await ctx.reply('❌ Cleanup dry-run failed.');
    }
  });

  bot.command('cleanuprun', requireSuperAdmin, async (ctx) => {
    try {
      const confirmed = await requireSuperAdminActionConfirmation(
        ctx,
        'cleanuprun',
        '',
        'This will execute retention cleanup immediately and permanently delete old data/files.'
      );
      if (!confirmed) return;

      const report = await cleanupRetentionJob({ dryRun: false });
      await ctx.reply(
        `🧹 *Cleanup Executed*\n\n` +
        `Pending requests reset: *${report.stalePendingCount}*\n` +
        `Admin logs deleted: *${report.oldLogsCount}*\n` +
        `Daily summaries deleted: *${report.oldSummariesCount}*\n` +
        `Invite metadata cleared: *${report.inviteCleanupCount}*\n` +
        `Log files deleted: *${report.removedFiles}*`,
        { parse_mode: 'Markdown' }
      );
    } catch (err) {
      logger.error(`cleanuprun command error: ${err.message}`);
      await ctx.reply('❌ Cleanup execution failed.');
    }
  });

  bot.command('cleanupreport', requireSuperAdmin, async (ctx) => {
    const report = getLastCleanupReport();
    if (!report) {
      return ctx.reply('ℹ️ No cleanup report available yet. Run /cleanupdryrun or wait for scheduled cleanup.');
    }
    await ctx.reply(
      `📄 *Last Cleanup Report*\n\n` +
      `Ran at: *${new Date(report.ranAt).toLocaleString('en-IN')}*\n` +
      `Mode: *${report.dryRun ? 'dry-run' : 'apply'}*\n` +
      `Pending: *${report.stalePendingCount}*\n` +
      `Admin logs: *${report.oldLogsCount}*\n` +
      `Daily summaries: *${report.oldSummariesCount}*\n` +
      `Invite metadata: *${report.inviteCleanupCount}*\n` +
      `Log files: *${report.removedFiles}*`,
      { parse_mode: 'Markdown' }
    );
  });

  // ── Plan management ────────────────────────────────────────────────────────
  // /createplan Name|days|price|category
  bot.command('createplan', requireSuperAdmin, async (ctx) => {
    const text = ctx.message.text.replace('/createplan', '').trim();
    const [name, days, price, rawCategory] = text.split('|').map(s => s.trim());
    if (!name || !days || !rawCategory) return ctx.reply('Usage: `/createplan Name|days|price|category`', { parse_mode: 'Markdown' });
    try {
      const normalizedCategory = normalizePlanCategory(rawCategory);
      const allowedCategories = new Set([
        PLAN_CATEGORY.MOVIE,
        PLAN_CATEGORY.DESI,
        PLAN_CATEGORY.NON_DESI,
      ]);
      if (!allowedCategories.has(normalizedCategory)) {
        return ctx.reply('❌ Invalid category. Use: movie, desi, non_desi');
      }

      const plan = await createPlan({
        name,
        durationDays: parseInt(days),
        price: price ? parseFloat(price) : 0,
        category: normalizedCategory,
      });
      await AdminLog.create({ adminId: ctx.from.id, actionType: 'create_plan', details: { planId: plan._id, name } });
      await ctx.reply(`✅ Plan created: *${escapeMarkdown(plan.name)}* (${plan.durationDays} days)\nCategory: *${escapeMarkdown(plan.category)}*\nID: \`${plan._id}\``, { parse_mode: 'Markdown' });
    } catch (err) { await ctx.reply(`❌ ${err.message}`); }
  });

  // /editplan id|field|value
  bot.command('editplan', requireSuperAdmin, async (ctx) => {
    const text = ctx.message.text.replace('/editplan', '').trim();
    const [planId, field, value] = text.split('|').map(s => s.trim());
    if (!planId || !field || !value) return ctx.reply('Usage: `/editplan id|field|value`', { parse_mode: 'Markdown' });
    try {
      const editableFields = new Set(['name', 'durationDays', 'price', 'category', 'isActive']);
      if (!editableFields.has(field)) {
        return ctx.reply('❌ Invalid field. Allowed: name, durationDays, price, category, isActive');
      }

      let parsedValue = value;
      if (field === 'durationDays' || field === 'price') {
        parsedValue = Number(value);
        if (!Number.isFinite(parsedValue)) {
          return ctx.reply(`❌ ${field} must be a valid number.`);
        }
      } else if (field === 'isActive') {
        if (!['true', 'false'].includes(String(value).toLowerCase())) {
          return ctx.reply('❌ isActive must be true or false.');
        }
        parsedValue = String(value).toLowerCase() === 'true';
      } else if (field === 'category') {
        parsedValue = normalizePlanCategory(value);
      }

      const updates = { [field]: parsedValue };
      const plan = await updatePlan(planId, updates);
      await AdminLog.create({ adminId: ctx.from.id, actionType: 'edit_plan', details: { planId, field, value } });
      await ctx.reply(`✅ Plan *${escapeMarkdown(plan.name)}* updated.`, { parse_mode: 'Markdown' });
    } catch (err) { await ctx.reply(`❌ ${err.message}`); }
  });

  bot.command('deleteplan', requireSuperAdmin, async (ctx) => {
    const planId = ctx.message.text.split(' ')[1];
    if (!planId) return ctx.reply('Usage: /deleteplan <planId>');
    try {
      const plan = await Plan.findById(planId).select('name durationDays category').lean();
      if (!plan) return ctx.reply(`❌ Plan not found: \`${planId}\``, { parse_mode: 'Markdown' });

      const confirmed = await requireSuperAdminActionConfirmation(
        ctx,
        'deleteplan',
        planId,
        `This will permanently delete plan *${escapeMarkdown(plan.name)}* (${plan.durationDays}d, ${escapeMarkdown(String(plan.category || 'movie'))}).`
      );
      if (!confirmed) return;

      await deletePlan(planId);
      await AdminLog.create({ adminId: ctx.from.id, actionType: 'delete_plan', details: { planId } });
      await ctx.reply('✅ Plan deleted.');
    } catch (err) { await ctx.reply(`❌ ${err.message}`); }
  });

  bot.command('pauseplan', requireSuperAdmin, async (ctx) => {
    const planId = ctx.message.text.split(' ')[1];
    if (!planId) return ctx.reply('Usage: /pauseplan <planId>');
    try {
      const plan = await Plan.findByIdAndUpdate(planId, [{ $set: { isActive: { $not: '$isActive' } } }], { new: true });
      await ctx.reply(`✅ Plan *${escapeMarkdown(plan.name)}* is now ${plan.isActive ? '✅ Active' : '⏸ Paused'}.`, { parse_mode: 'Markdown' });
    } catch (err) { await ctx.reply(`❌ ${err.message}`); }
  });

  bot.command('resumeplan', requireSuperAdmin, async (ctx) => {
    const planId = ctx.message.text.split(' ')[1];
    if (!planId) return ctx.reply('Usage: /resumeplan <planId>');

    try {
      const plan = await Plan.findById(planId);
      if (!plan) {
        return ctx.reply('❌ Plan not found.');
      }

      if (plan.isActive) {
        return ctx.reply(`ℹ️ Plan *${escapeMarkdown(plan.name)}* is already active.`, { parse_mode: 'Markdown' });
      }

      plan.isActive = true;
      await plan.save();

      await AdminLog.create({
        adminId: ctx.from.id,
        actionType: 'edit_plan',
        details: {
          planId: plan._id,
          field: 'isActive',
          value: true,
          command: 'resumeplan',
        },
      });

      await ctx.reply(`✅ Plan *${escapeMarkdown(plan.name)}* resumed and set to ✅ Active.`, { parse_mode: 'Markdown' });
    } catch (err) {
      await ctx.reply(`❌ ${err.message}`);
    }
  });

  bot.command('listplans', requireSuperAdmin, async (ctx) => {
    const plans = await getAllPlans();
    if (!plans.length) return ctx.reply('No plans found.');
    const grouped = {
      movie: [],
      desi: [],
      non_desi: [],
    };

    plans.forEach((plan) => {
      const category = normalizePlanCategory(plan.category || 'movie');
      if (!grouped[category]) grouped[category] = [];
      grouped[category].push(plan);
    });

    let msg = '📋 *All Plans (Category-wise)*\n\n';
    const orderedCategories = ['movie', 'desi', 'non_desi'];

    for (const category of orderedCategories) {
      const items = grouped[category] || [];
      msg += `*${category.toUpperCase()}*\n`;
      if (!items.length) {
        msg += `No plans\n\n`;
        continue;
      }

      items.forEach((p, i) => {
        msg += `${i + 1}. *${escapeMarkdown(p.name)}* — ${p.durationDays} days — ₹${p.price} — ${p.isActive ? '✅' : '⏸'}\n   \`${p._id}\`\n`;
      });
      msg += `\n`;
    }

    await ctx.reply(msg, { parse_mode: 'Markdown' });
  });

  // ── Offer management ───────────────────────────────────────────────────────
  // /createoffer plan_id|discount|DD/MM/YYYY
  bot.command('createoffer', requireSuperAdmin, async (ctx) => {
    const text = String(ctx.message?.text || '').replace('/createoffer', '').trim();
    const [planId, discountRaw, dateStr] = text.split('|').map((s) => String(s || '').trim());

    if (!planId || !discountRaw || !dateStr) {
      return ctx.reply('Usage: `/createoffer plan_id|discount|DD/MM/YYYY`', { parse_mode: 'Markdown' });
    }

    const discountPercent = Number(discountRaw);
    if (!Number.isFinite(discountPercent) || discountPercent < 0 || discountPercent > 100) {
      return ctx.reply('❌ Invalid discount. Use a number between 0 and 100.');
    }

    const [d, m, y] = dateStr.split('/');
    const validTill = new Date(`${y}-${m}-${d}`);
    if (!d || !m || !y || Number.isNaN(validTill.getTime())) {
      return ctx.reply('❌ Invalid date format. Use DD/MM/YYYY.');
    }

    try {
      const plan = await Plan.findById(planId);
      if (!plan) {
        return ctx.reply(`❌ Plan not found: \`${planId}\``, { parse_mode: 'Markdown' });
      }

      const offer = await createOffer({
        planId: plan._id,
        title: `${discountPercent}% OFF on ${plan.name}`,
        description: `Plan specific offer for ${plan.name}`,
        validTill,
        discountPercent,
        createdBy: ctx.from.id,
      });

      await AdminLog.create({
        adminId: ctx.from.id,
        actionType: 'create_offer',
        details: {
          offerId: offer._id,
          planId: plan._id,
          planName: plan.name,
          discountPercent,
        },
      });

      await ctx.reply(
        `✅ Plan offer created\n` +
        `Plan: *${escapeMarkdown(plan.name)}*\n` +
        `Discount: *${discountPercent}%*\n` +
        `Valid Till: *${escapeMarkdown(formatDate(validTill))}*\n` +
        `Offer ID: \`${offer._id}\``,
        { parse_mode: 'Markdown' }
      );
    } catch (err) {
      await ctx.reply(`❌ ${err.message}`);
    }
  });

  // /addoffer Title|Desc|DD/MM/YYYY|discount%
  bot.command('addoffer', requireSuperAdmin, async (ctx) => {
    const text = ctx.message.text.replace('/addoffer', '').trim();
    const [title, description, dateStr, discount] = text.split('|').map(s => s.trim());
    if (!title || !description || !dateStr) {
      return ctx.reply('Usage: `/addoffer Title|Description|DD/MM/YYYY|discountPercent`', { parse_mode: 'Markdown' });
    }
    try {
      const [d, m, y] = dateStr.split('/');
      const offer = await createOffer({
        title, description,
        validTill: new Date(`${y}-${m}-${d}`),
        discountPercent: discount ? parseInt(discount) : 0,
        createdBy: ctx.from.id,
      });
      await AdminLog.create({ adminId: ctx.from.id, actionType: 'create_offer', details: { offerId: offer._id, title } });
      await ctx.reply(`✅ Offer created: *${escapeMarkdown(offer.title)}*`, { parse_mode: 'Markdown' });
    } catch (err) { await ctx.reply(`❌ ${err.message}`); }
  });

  bot.command('deleteoffer', requireSuperAdmin, async (ctx) => {
    const offerId = ctx.message.text.split(' ')[1];
    if (!offerId) return ctx.reply('Usage: /deleteoffer <offerId>');
    try {
      await deleteOffer(offerId);
      await ctx.reply('✅ Offer deleted.');
    } catch (err) { await ctx.reply(`❌ ${err.message}`); }
  });

  bot.command('listoffers', requireSuperAdmin, async (ctx) => {
    const offers = await getActiveOffers();
    if (!offers.length) return ctx.reply('No active offers.');
    let msg = '🎁 *Active Offers*\n\n';
    offers.forEach((o, i) => {
      msg += `${i + 1}. *${escapeMarkdown(o.title)}*\n${escapeMarkdown(o.description)}\nValid till: ${escapeMarkdown(formatDate(o.validTill))}\n\`${o._id}\`\n\n`;
    });
    await ctx.reply(msg, { parse_mode: 'Markdown' });
  });

  // ── /removeprivateoffers all|<telegramId> ────────────────────────────────
  bot.command('removeprivateoffers', requireSuperAdmin, async (ctx) => {
    try {
      const raw = String(ctx.message?.text || '').replace('/removeprivateoffers', '').trim();
      if (!raw) {
        return ctx.reply(
          'Usage: `/removeprivateoffers all` or `/removeprivateoffers <telegramId>`',
          { parse_mode: 'Markdown' }
        );
      }

      if (raw.toLowerCase() === 'all') {
        const result = await UserOffer.deleteMany({ isActive: true, isUsed: false });

        await AdminLog.create({
          adminId: ctx.from.id,
          actionType: 'delete_offer',
          targetUserId: null,
          details: {
            userSpecific: true,
            scope: 'all',
            deletedCount: Number(result?.deletedCount || 0),
            command: 'removeprivateoffers',
          },
        });

        return ctx.reply(
          `✅ Removed *${Number(result?.deletedCount || 0)}* private offer(s) from all users.`,
          { parse_mode: 'Markdown' }
        );
      }

      const targetId = parseInt(raw, 10);
      if (!targetId) {
        return ctx.reply(
          '❌ Invalid input. Use `all` or a valid telegramId.',
          { parse_mode: 'Markdown' }
        );
      }

      const result = await UserOffer.deleteMany({ targetTelegramId: targetId });

      await AdminLog.create({
        adminId: ctx.from.id,
        actionType: 'delete_offer',
        targetUserId: targetId,
        details: {
          userSpecific: true,
          scope: 'user',
          deletedCount: Number(result?.deletedCount || 0),
          command: 'removeprivateoffers',
        },
      });

      await ctx.reply(
        `✅ Removed *${Number(result?.deletedCount || 0)}* private offer(s) for user \`${targetId}\`.`,
        { parse_mode: 'Markdown' }
      );
    } catch (err) {
      logger.error(`removeprivateoffers command error: ${err.message}`);
      await ctx.reply('❌ Failed to remove private offers. Please try again.');
    }
  });

  // ── /listprivateoffers ────────────────────────────────────────────────────
  bot.command('listprivateoffers', requireSuperAdmin, async (ctx) => {
    try {
      const offers = await UserOffer.find({ isUsed: false })
        .sort({ targetTelegramId: 1, createdAt: -1 })
        .lean();

      if (!offers.length) {
        return ctx.reply('No non-redeemed private offers found.');
      }

      const lines = ['id|offers|admin_id'];
      offers.forEach((offer) => {
        const offerLabel = offer?.discountPercent > 0
          ? `${offer.discountPercent}%`
          : (offer?.title || 'offer');
        lines.push(`${offer.targetTelegramId}|${offerLabel}|${offer.createdBy}`);
      });

      await ctx.reply(`\`${lines.join('\n')}\``, { parse_mode: 'Markdown' });
    } catch (err) {
      logger.error(`listprivateoffers command error: ${err.message}`);
      await ctx.reply('❌ Failed to fetch private offers list.');
    }
  });

  // ── /reset [all|requests|withdrawals] — clear pending approvals ───────────
  bot.command('reset', requireSuperAdmin, async (ctx) => {
    try {
      const scopeRaw = String(ctx.message?.text || '').split(/\s+/)[1] || 'all';
      const scope = scopeRaw.trim().toLowerCase();
      const allowedScopes = new Set(['all', 'requests', 'withdrawals']);
      if (!allowedScopes.has(scope)) {
        return ctx.reply(
          'Usage: `/reset [all|requests|withdrawals]`\nExample: `/reset all`',
          { parse_mode: 'Markdown' }
        );
      }

      const confirmed = await requireSuperAdminActionConfirmation(
        ctx,
        'reset',
        scope,
        `This will reset pending approvals for scope *${escapeMarkdown(scope)}* (user join requests and/or seller withdrawals).`
      );
      if (!confirmed) return;

      const now = new Date();
      let resetRequestsCount = 0;
      let resetWithdrawalsCount = 0;
      let statusSyncCount = 0;

      if (scope === 'all' || scope === 'requests') {
        const pendingRequests = await Request.find({ status: { $in: ['pending', 'processing'] } })
          .select('telegramId')
          .lean();

        const affectedTelegramIds = [...new Set(
          pendingRequests
            .map((row) => Number(row.telegramId))
            .filter((id) => Number.isFinite(id))
        )];

        const requestResetResult = await Request.updateMany(
          { status: { $in: ['pending', 'processing'] } },
          {
            $set: {
              status: 'rejected',
              actionDate: now,
              actionBy: ctx.from.id,
              pendingReminderAt: null,
              processingBy: null,
              processingAt: null,
              processingToken: null,
            },
          }
        );
        resetRequestsCount = Number(requestResetResult?.modifiedCount || 0);

        for (const telegramId of affectedTelegramIds) {
          try {
            await syncUserStatusFromSubscriptions(telegramId);
            statusSyncCount += 1;
          } catch (syncErr) {
            logger.warn(`reset command status sync failed for ${telegramId}: ${syncErr.message}`);
          }
        }
      }

      if (scope === 'all' || scope === 'withdrawals') {
        const withdrawalResetResult = await SellerWithdrawalRequest.updateMany(
          { status: 'pending' },
          {
            $set: {
              status: 'rejected',
              reviewedAt: now,
              reviewedBy: ctx.from.id,
              note: 'Reset by superadmin via /reset',
            },
          }
        );
        resetWithdrawalsCount = Number(withdrawalResetResult?.modifiedCount || 0);
      }

      await AdminLog.create({
        adminId: ctx.from.id,
        actionType: 'reset_pending',
        details: {
          scope,
          resetRequestsCount,
          resetWithdrawalsCount,
          statusSyncCount,
        },
      });

      await ctx.reply(
        `✅ Pending approvals reset completed.\n\n` +
        `Scope: *${escapeMarkdown(scope)}*\n` +
        `User join requests reset: *${resetRequestsCount}*\n` +
        `Seller withdrawals reset: *${resetWithdrawalsCount}*\n` +
        `User status synced: *${statusSyncCount}*`,
        { parse_mode: 'Markdown' }
      );
    } catch (err) {
      logger.error(`reset command error: ${err.message}`);
      await ctx.reply('❌ Failed to reset pending approvals. Please try again.');
    }
  });

  // ── /broadcast ─────────────────────────────────────────────────────────────
  bot.command('broadcast', requireSuperAdmin, async (ctx) => {
    await ctx.reply('📢 *Broadcast — Choose Target:*', {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [{ text: '👥 All Users', callback_data: 'bc_all' }, { text: '✅ Active', callback_data: 'bc_active' }],
          [{ text: '❌ Expired', callback_data: 'bc_expired' }, { text: '🆕 New (last 3 days)', callback_data: 'bc_new' }],
        ],
      },
    });
  });

  bot.action(/^bc_(all|active|expired|new)$/, requireSuperAdmin, async (ctx) => {
    await ctx.answerCbQuery();
    startBroadcastSession(ctx.from.id, { target: ctx.match[1] });
    await ctx.reply(
      `📝 Send your broadcast content for *${ctx.match[1]}* users.\n` +
      `You can send text or media with caption.\n` +
      `Send /bcancel to abort.`,
      { parse_mode: 'Markdown' }
    );
  });

  // ── /reports ───────────────────────────────────────────────────────────────
  bot.command('reports', requireSuperAdmin, async (ctx) => {
    await ctx.reply('📊 *Reports — Choose Type:*', {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [{ text: '📅 Daily Sales', callback_data: 'report_daily' }, { text: '📆 Weekly', callback_data: 'report_weekly' }],
          [{ text: '🗓 Monthly', callback_data: 'report_monthly' }, { text: '📋 Today Expiry', callback_data: 'report_expiry' }],
          [{ text: '✅ Active Users', callback_data: 'report_active' }, { text: '❌ Expired Users', callback_data: 'report_expired' }],
        ],
      },
    });
  });

  bot.action(/^report_(daily|weekly|monthly|expiry|active|expired)$/, requireSuperAdmin, async (ctx) => {
    await ctx.answerCbQuery('Generating...');
    const type = ctx.match[1];
    try {
      let message = '';
      if (type === 'daily' || type === 'weekly' || type === 'monthly') {
        let start = startOfToday();
        let end = endOfToday();
        let title = 'Daily Sales Report';

        if (type === 'weekly') {
          start = startOfWeek();
          end = new Date();
          title = 'Weekly Sales Report';
        } else if (type === 'monthly') {
          start = startOfMonth();
          end = new Date();
          title = 'Monthly Sales Report';
        }

        const userRows = await getSalesUserBreakdown(start, end);
        const chunks = buildCategoryWiseSalesReportMessages(title, userRows, 3500);
        await sendPersistedMarkdownChunks(ctx, chunks, 3500);
        return;
      } else if (type === 'expiry') {
        const list = await getTodayExpiryList();
        message = `📋 *Today's Expiry List* (${list.length})\n\n`;
        list.forEach((s, i) => { message += `${i + 1}. \`${s.telegramId}\` — ${s.planName}\n`; });
        if (!list.length) message += 'No expirations today.';
      } else if (type === 'active') {
        const c = await Subscription.countDocuments({ status: 'active', expiryDate: { $gt: new Date() } });
        message = `✅ *Active Subscriptions:* ${c}`;
      } else if (type === 'expired') {
        const c = await Subscription.countDocuments({ status: 'expired' });
        message = `❌ *Total Expired:* ${c}`;
      }
      await sendPersistedMarkdownChunks(ctx, [message], 3500);
    } catch (err) {
      await ctx.reply('❌ Error generating report.');
      logger.error(`report error: ${err.message}`);
    }
  });

  // ── /report <Nd|Nm> — custom CSV report (e.g. 1d, 7d, 28d, 1m) ─────────
  bot.command('report', requireSuperAdmin, async (ctx) => {
    try {
      const token = (ctx.message.text.split(' ')[1] || '').trim().toLowerCase();
      const parsed = parseReportDuration(token);
      if (!parsed) {
        return ctx.reply('Usage: `/report <Nd|Nm>`\nExamples: `/report 1d`, `/report 7d`, `/report 28d`, `/report 1m`', { parse_mode: 'Markdown' });
      }

      const { value, unit } = parsed;
      const endDate = new Date();
      const startDate = new Date(endDate);
      if (unit === 'd') startDate.setDate(startDate.getDate() - value);
      else startDate.setMonth(startDate.getMonth() - value);

      const [
        requestsReceived,
        approvals,
        rejections,
        subsCreated,
        renewals,
        newUsers,
        activeNow,
        expiredNow,
        blockedNow,
        salesByPlan,
        subscriptionRows,
      ] = await Promise.all([
        Request.countDocuments({ createdAt: { $gte: startDate, $lte: endDate } }),
        Request.countDocuments({ status: 'approved', actionDate: { $gte: startDate, $lte: endDate } }),
        Request.countDocuments({ status: 'rejected', actionDate: { $gte: startDate, $lte: endDate } }),
        Subscription.countDocuments({ createdAt: { $gte: startDate, $lte: endDate } }),
        Subscription.countDocuments({
          createdAt: { $gte: startDate, $lte: endDate },
          isRenewal: true,
        }),
        User.countDocuments({ createdAt: { $gte: startDate, $lte: endDate } }),
        Subscription.countDocuments({ status: 'active', expiryDate: { $gt: endDate } }),
        Subscription.countDocuments({ status: 'expired' }),
        User.countDocuments({ isBlocked: true }),
        getSalesReport(startDate, endDate),
        Subscription.find({
          createdAt: { $gte: startDate, $lte: endDate },
        })
          .select('telegramId planName startDate expiryDate approvedBy status')
          .sort({ createdAt: -1 })
          .lean(),
      ]);

      const label = `${value}${unit}`;
      const generatedAt = new Date();
      const rows = [
        ['Section', 'Metric', 'Value'],
        ['Meta', 'Range', label],
        ['Meta', 'StartDate', startDate.toISOString()],
        ['Meta', 'EndDate', endDate.toISOString()],
        ['Meta', 'GeneratedAt', generatedAt.toISOString()],
        ['Summary', 'RequestsReceived', requestsReceived],
        ['Summary', 'Approvals', approvals],
        ['Summary', 'Rejections', rejections],
        ['Summary', 'SubscriptionsCreated', subsCreated],
        ['Summary', 'Renewals', renewals],
        ['Summary', 'NewUsers', newUsers],
        ['Snapshot', 'ActiveSubscriptionsNow', activeNow],
        ['Snapshot', 'ExpiredSubscriptionsNow', expiredNow],
        ['Snapshot', 'BlockedUsersNow', blockedNow],
      ];

      rows.push([]);
      rows.push(['PlanSales', 'PlanName', 'Count', 'Revenue']);
      if (salesByPlan.length) {
        salesByPlan.forEach((row) => {
          rows.push(['PlanSales', row.planName, row.count, Number(row.totalRevenue || 0).toFixed(2)]);
        });
      } else {
        rows.push(['PlanSales', 'NoData', 0, '0.00']);
      }

      rows.push([]);
      rows.push(['UserSubscriptions', 'UserID', 'Plan', 'StartDate', 'ExpiryDate', 'ApprovedByAdminId', 'Status']);
      if (subscriptionRows.length) {
        subscriptionRows.forEach((sub) => {
          rows.push([
            'UserSubscriptions',
            sub.telegramId,
            sub.planName,
            sub.startDate ? new Date(sub.startDate).toISOString() : '',
            sub.expiryDate ? new Date(sub.expiryDate).toISOString() : '',
            sub.approvedBy ?? '',
            sub.status || '',
          ]);
        });
      } else {
        rows.push(['UserSubscriptions', 'NoData', '', '', '', '', '']);
      }

      const csv = rows.map(toCsvRow).join('\n');
      const fileName = `report_${label}_${generatedAt.toISOString().slice(0, 10)}.csv`;

      await ctx.replyWithDocument(
        {
          source: Buffer.from(csv, 'utf8'),
          filename: fileName,
        },
        {
          caption: `📄 CSV report generated for ${label}`,
        }
      );
    } catch (err) {
      logger.error(`custom report csv error: ${err.message}`);
      await ctx.reply('❌ Failed to generate CSV report.');
    }
  });

  // ── /sellerpayoutscsv [Nd|Nm|all] — seller payouts CSV export ───────────
  bot.command('sellerpayoutscsv', requireSuperAdmin, async (ctx) => {
    try {
      const token = (ctx.message.text.split(' ')[1] || '').trim().toLowerCase();

      let startDate = null;
      let label = 'all';

      if (token && token !== 'all') {
        const parsed = parseReportDuration(token);
        if (!parsed) {
          return ctx.reply('Usage: `/sellerpayoutscsv [Nd|Nm|all]`\nExamples: `/sellerpayoutscsv 7d`, `/sellerpayoutscsv 1m`, `/sellerpayoutscsv all`', { parse_mode: 'Markdown' });
        }

        const { value, unit } = parsed;
        label = `${value}${unit}`;
        startDate = new Date();
        if (unit === 'd') startDate.setDate(startDate.getDate() - value);
        else startDate.setMonth(startDate.getMonth() - value);
      }

      const withdrawalQuery = startDate
        ? { requestedAt: { $gte: startDate } }
        : {};
      const ledgerQuery = startDate
        ? { createdAt: { $gte: startDate } }
        : {};

      const [withdrawals, ledgerRows] = await Promise.all([
        SellerWithdrawalRequest.find(withdrawalQuery)
          .sort({ requestedAt: -1 })
          .lean(),
        SellerPayoutLedger.find(ledgerQuery)
          .sort({ createdAt: -1 })
          .lean(),
      ]);

      const generatedAt = new Date();
      const rows = [
        ['Section', 'Metric', 'Value'],
        ['Meta', 'Range', label],
        ['Meta', 'GeneratedAt', generatedAt.toISOString()],
        ['Summary', 'WithdrawalRows', withdrawals.length],
        ['Summary', 'LedgerRows', ledgerRows.length],
      ];

      rows.push([]);
      rows.push(['Withdrawals', 'RequestId', 'SellerId', 'Status', 'Amount', 'UPI', 'RequestedAt', 'ReviewedAt', 'ReviewedBy', 'Note']);
      if (withdrawals.length) {
        withdrawals.forEach((item) => {
          rows.push([
            'Withdrawals',
            String(item._id),
            item.sellerTelegramId,
            item.status,
            Number(item.amount || 0).toFixed(2),
            item.upiId || '',
            item.requestedAt ? new Date(item.requestedAt).toISOString() : '',
            item.reviewedAt ? new Date(item.reviewedAt).toISOString() : '',
            item.reviewedBy ?? '',
            item.note || '',
          ]);
        });
      } else {
        rows.push(['Withdrawals', 'NoData', '', '', '', '', '', '', '', '']);
      }

      rows.push([]);
      rows.push(['Ledger', 'EntryId', 'SellerId', 'EntryType', 'Source', 'Amount', 'BalanceAfter', 'RelatedUserId', 'RelatedRequestId', 'CreatedAt', 'CreatedBy', 'Note']);
      if (ledgerRows.length) {
        ledgerRows.forEach((entry) => {
          rows.push([
            'Ledger',
            String(entry._id),
            entry.sellerTelegramId,
            entry.entryType,
            entry.source,
            Number(entry.amount || 0).toFixed(2),
            Number(entry.balanceAfter || 0).toFixed(2),
            entry.relatedUserTelegramId ?? '',
            entry.relatedWithdrawalRequestId ? String(entry.relatedWithdrawalRequestId) : '',
            entry.createdAt ? new Date(entry.createdAt).toISOString() : '',
            entry.createdBy ?? '',
            entry.note || '',
          ]);
        });
      } else {
        rows.push(['Ledger', 'NoData', '', '', '', '', '', '', '', '', '', '']);
      }

      const csv = rows.map(toCsvRow).join('\n');
      const fileName = `seller_payouts_${label}_${generatedAt.toISOString().slice(0, 10)}.csv`;

      await ctx.replyWithDocument(
        {
          source: Buffer.from(csv, 'utf8'),
          filename: fileName,
        },
        {
          caption: `📄 Seller payouts CSV generated for ${label}`,
        }
      );
    } catch (err) {
      logger.error(`sellerpayoutscsv error: ${err.message}`);
      await ctx.reply('❌ Failed to generate seller payouts CSV report.');
    }
  });

  // ── /sellerwithdrawalscsv [Nd|Nm|all] [status] — withdrawals-only CSV ──
  bot.command('sellerwithdrawalscsv', requireSuperAdmin, async (ctx) => {
    try {
      const parts = String(ctx.message?.text || '').trim().split(/\s+/);
      const arg1 = String(parts[1] || '').trim().toLowerCase();
      const arg2 = String(parts[2] || '').trim().toLowerCase();

      const allowedStatuses = new Set(['pending', 'approved', 'rejected', 'all']);
      let status = 'all';
      let startDate = null;
      let label = 'all';

      if (arg1) {
        if (allowedStatuses.has(arg1)) {
          status = arg1;
        } else if (arg1 !== 'all') {
          const parsed = parseReportDuration(arg1);
          if (!parsed) {
            return ctx.reply('Usage: `/sellerwithdrawalscsv [Nd|Nm|all] [pending|approved|rejected|all]`\nExamples: `/sellerwithdrawalscsv 7d`, `/sellerwithdrawalscsv 1m pending`, `/sellerwithdrawalscsv all approved`', { parse_mode: 'Markdown' });
          }

          const { value, unit } = parsed;
          label = `${value}${unit}`;
          startDate = new Date();
          if (unit === 'd') startDate.setDate(startDate.getDate() - value);
          else startDate.setMonth(startDate.getMonth() - value);
        }
      }

      if (arg2) {
        if (!allowedStatuses.has(arg2)) {
          return ctx.reply('Usage: `/sellerwithdrawalscsv [Nd|Nm|all] [pending|approved|rejected|all]`\nExamples: `/sellerwithdrawalscsv 7d`, `/sellerwithdrawalscsv 1m pending`, `/sellerwithdrawalscsv all approved`', { parse_mode: 'Markdown' });
        }
        status = arg2;
      }

      const query = {
        ...(startDate ? { requestedAt: { $gte: startDate } } : {}),
        ...(status !== 'all' ? { status } : {}),
      };

      const withdrawals = await SellerWithdrawalRequest.find(query)
        .sort({ requestedAt: -1 })
        .lean();

      const generatedAt = new Date();
      const rows = [
        ['Section', 'Metric', 'Value'],
        ['Meta', 'Range', label],
        ['Meta', 'StatusFilter', status],
        ['Meta', 'GeneratedAt', generatedAt.toISOString()],
        ['Summary', 'WithdrawalRows', withdrawals.length],
      ];

      rows.push([]);
      rows.push(['Withdrawals', 'RequestId', 'SellerId', 'Status', 'Amount', 'UPI', 'RequestedAt', 'ReviewedAt', 'ReviewedBy', 'Note']);
      if (withdrawals.length) {
        withdrawals.forEach((item) => {
          rows.push([
            'Withdrawals',
            String(item._id),
            item.sellerTelegramId,
            item.status,
            Number(item.amount || 0).toFixed(2),
            item.upiId || '',
            item.requestedAt ? new Date(item.requestedAt).toISOString() : '',
            item.reviewedAt ? new Date(item.reviewedAt).toISOString() : '',
            item.reviewedBy ?? '',
            item.note || '',
          ]);
        });
      } else {
        rows.push(['Withdrawals', 'NoData', '', '', '', '', '', '', '', '']);
      }

      const csv = rows.map(toCsvRow).join('\n');
      const fileName = `seller_withdrawals_${label}_${status}_${generatedAt.toISOString().slice(0, 10)}.csv`;

      await ctx.replyWithDocument(
        {
          source: Buffer.from(csv, 'utf8'),
          filename: fileName,
        },
        {
          caption: `📄 Seller withdrawals CSV generated for ${label} (${status})`,
        }
      );
    } catch (err) {
      logger.error(`sellerwithdrawalscsv error: ${err.message}`);
      await ctx.reply('❌ Failed to generate seller withdrawals CSV report.');
    }
  });

  // ── /stats — growth dashboard ──────────────────────────────────────────────
  bot.command('stats', requireSuperAdmin, async (ctx) => {
    try {
      const [s, categoryStats] = await Promise.all([
        getGrowthStats(),
        getCategoryWiseStats(),
      ]);

      const categoryLabel = {
        movie: 'Movie',
        desi: 'Desi',
        non_desi: 'Non-Desi',
      };

      let categorySection = '\n\n📂 *Category-wise*\n';
      categoryStats.forEach((row) => {
        const label = categoryLabel[row.category] || row.category;
        categorySection +=
          `\n• *${label}*` +
          `\n  Active: *${row.activeSubscriptions}* | Pending: *${row.pendingRequests}*` +
          `\n  Approved Today: *${row.approvalsToday}* | Renewals Today: *${row.renewalsToday}*\n`;
      });

      await ctx.reply(
        `📈 *Growth Dashboard*\n\n` +
        `👥 Total Users: *${s.total}*\n` +
        `✅ Active: *${s.active}*\n` +
        `❌ Expired: *${s.expired}*\n` +
        `🚫 Blocked: *${s.blocked}*\n` +
        `🆕 New Today: *${s.newToday}*\n` +
        `🔄 Renewals Today: *${s.renewalsToday}*` +
        categorySection,
        { parse_mode: 'Markdown' }
      );
    } catch (err) {
      await ctx.reply('❌ Error fetching stats.');
    }
  });

  // ── /categorystats — category-wise CSV snapshot ───────────────────────────
  bot.command('categorystats', requireSuperAdmin, async (ctx) => {
    try {
      const categoryStats = await getCategoryWiseStats();
      const generatedAt = new Date();

      const categoryLabel = {
        movie: 'Movie',
        desi: 'Desi',
        non_desi: 'Non-Desi',
      };

      const rows = [
        ['Section', 'Category', 'ActiveSubscriptions', 'PendingRequests', 'ApprovalsToday', 'RenewalsToday', 'GeneratedAt'],
      ];

      let totals = {
        activeSubscriptions: 0,
        pendingRequests: 0,
        approvalsToday: 0,
        renewalsToday: 0,
      };

      categoryStats.forEach((row) => {
        rows.push([
          'CategoryStats',
          categoryLabel[row.category] || row.category,
          row.activeSubscriptions,
          row.pendingRequests,
          row.approvalsToday,
          row.renewalsToday,
          generatedAt.toISOString(),
        ]);

        totals.activeSubscriptions += Number(row.activeSubscriptions || 0);
        totals.pendingRequests += Number(row.pendingRequests || 0);
        totals.approvalsToday += Number(row.approvalsToday || 0);
        totals.renewalsToday += Number(row.renewalsToday || 0);
      });

      rows.push([
        'Totals',
        'All Categories',
        totals.activeSubscriptions,
        totals.pendingRequests,
        totals.approvalsToday,
        totals.renewalsToday,
        generatedAt.toISOString(),
      ]);

      const csv = rows.map(toCsvRow).join('\n');
      const fileName = `category_stats_${generatedAt.toISOString().slice(0, 10)}.csv`;

      await ctx.replyWithDocument(
        {
          source: Buffer.from(csv, 'utf8'),
          filename: fileName,
        },
        {
          caption: '📄 Category-wise stats CSV generated.',
        }
      );
    } catch (err) {
      logger.error(`categorystats error: ${err.message}`);
      await ctx.reply('❌ Failed to generate category-wise stats CSV.');
    }
  });

  // ── /planstats — plan performance ─────────────────────────────────────────
  bot.command('planstats', requireSuperAdmin, async (ctx) => {
    try {
      const data = await getPlanPerformance();
      if (!data.length) return ctx.reply('No active subscriptions found.');

      let msg = '📊 *Plan Performance*\n\n';
      data.forEach(row => {
        msg += `📋 *${row.planName}* (${row.durationDays} days): *${row.count}* active users\n`;
      });
      await ctx.reply(msg, { parse_mode: 'Markdown' });
    } catch (err) {
      await ctx.reply('❌ Error fetching plan stats.');
    }
  });

  // ── /health — runtime snapshot ────────────────────────────────────────────
  bot.command('health', requireSuperAdmin, async (ctx) => {
    try {
      const dbStateMap = {
        0: 'disconnected',
        1: 'connected',
        2: 'connecting',
        3: 'disconnecting',
      };
      const readyState = mongoose?.connection?.readyState ?? 0;
      const dbState = dbStateMap[readyState] || 'unknown';
      const dbHost = mongoose?.connection?.host || 'N/A';
      const dbName = mongoose?.connection?.name || 'N/A';

      let botUsername = 'N/A';
      let botId = 'N/A';
      try {
        const me = await bot.telegram.getMe();
        botUsername = me?.username ? `@${me.username}` : 'N/A';
        botId = me?.id || 'N/A';
      } catch (_) { }

      const memory = process.memoryUsage();
      const rssBytes = Number(memory.rss || 0);
      const heapUsedBytes = Number(memory.heapUsed || 0);
      const heapTotalBytes = Number(memory.heapTotal || 0);

      const rssMb = (rssBytes / (1024 * 1024)).toFixed(1);
      const heapUsedMb = (heapUsedBytes / (1024 * 1024)).toFixed(1);
      const heapTotalMb = (heapTotalBytes / (1024 * 1024)).toFixed(1);
      const heapUtilization = heapTotalBytes > 0
        ? heapUsedBytes / heapTotalBytes
        : 0;
      const uptimeSeconds = Math.floor(process.uptime());

      const highHeapUtil = heapUtilization >= 0.92;
      const highHeapAbs = heapUsedBytes >= 150 * 1024 * 1024;
      const highRssAbs = rssBytes >= 500 * 1024 * 1024;
      const warmupWindow = uptimeSeconds < 120;
      const memoryPressure = highRssAbs || (highHeapUtil && highHeapAbs);

      const cronTimezone = process.env.CRON_TIMEZONE || 'Asia/Kolkata';
      const reminderSchedules = process.env.REMINDER_CRON_SCHEDULES || '15 9 * * *,0 20 * * *,15 23 * * *';
      const expirySchedules = process.env.EXPIRY_ENFORCEMENT_CRON_SCHEDULES || '0 8 * * *,0 14 * * *,30 20 * * *';

      let healthIcon = '🟢';
      let healthLabel = 'HEALTHY';
      let healthReason = 'All core systems operational';

      if (![1, 2, 3].includes(readyState)) {
        healthIcon = '🔴';
        healthLabel = 'CRITICAL';
        healthReason = 'Database disconnected';
      } else if (readyState !== 1 || memoryPressure || botUsername === 'N/A') {
        healthIcon = '🟡';
        healthLabel = 'DEGRADED';
        if (readyState !== 1) healthReason = `Database ${dbState}`;
        else if (memoryPressure) healthReason = highRssAbs ? 'High RSS memory usage' : 'High heap memory pressure';
        else healthReason = 'Bot identity check failed';
      } else if (warmupWindow && highHeapUtil) {
        healthIcon = '🟡';
        healthLabel = 'DEGRADED';
        healthReason = 'Startup warm-up (heap filling; monitor for 2 mins)';
      }

      const msg =
        `🩺 *System Health Snapshot*\n\n` +
        `${healthIcon} *${healthLabel}* — ${healthReason}\n\n` +
        `🤖 Bot: *${botUsername}* (ID: \`${botId}\`)\n` +
        `🟢 Process: PID \`${process.pid}\`\n` +
        `⏱ Uptime: *${formatUptime(process.uptime())}*\n` +
        `🧠 Memory: RSS *${rssMb} MB* | Heap *${heapUsedMb}/${heapTotalMb} MB*\n\n` +
        `🗄 DB State: *${dbState.toUpperCase()}* (code: \`${readyState}\`)\n` +
        `🧭 DB Host: \`${dbHost}\`\n` +
        `📚 DB Name: \`${dbName}\`\n\n` +
        `🕒 Cron TZ: *${cronTimezone}*\n` +
        `⏰ Reminder Schedules: \`${reminderSchedules}\`\n` +
        `🧯 Expiry Schedules: \`${expirySchedules}\`\n` +
        `🌐 Server Time: ${new Date().toLocaleString('en-IN')}`;

      await ctx.reply(msg, { parse_mode: 'Markdown' });
    } catch (err) {
      logger.error(`health command error: ${err.message}`);
      await ctx.reply('❌ Unable to fetch health snapshot right now.');
    }
  });

  // ── /sellerstats [limit] — seller summary list ───────────────────────────
  bot.command('sellerstats', requireSuperAdmin, async (ctx) => {
    try {
      const limitArg = parseInt((ctx.message.text.split(' ')[1] || '20').trim(), 10);
      const limit = Number.isFinite(limitArg) ? Math.min(Math.max(limitArg, 1), 100) : 20;

      const sellers = await User.find({ isSeller: true })
        .select('telegramId name username sellerCode sellerStats')
        .sort({ 'sellerStats.availableBalance': -1, 'sellerStats.qualifiedReferrals': -1, createdAt: -1 })
        .limit(limit)
        .lean();

      if (!sellers.length) return ctx.reply('ℹ️ No registered sellers found.');

      let msg = `🛍 *Seller Status List* (Top ${sellers.length})\n\n`;
      sellers.forEach((seller, index) => {
        const stats = seller.sellerStats || {};
        msg += `${index + 1}. Seller ID: \`${seller.telegramId}\`\n`;
        msg += `   Name: ${seller.name || 'N/A'}${seller.username ? ` (@${seller.username})` : ''}\n`;
        msg += `   Seller Code: \`${seller.sellerCode || 'N/A'}\`\n`;
        msg += `   Referrals: *${Number(stats.totalReferrals || 0)}*\n`;
        msg += `   Qualified: *${Number(stats.qualifiedReferrals || 0)}*\n`;
        msg += `   Available Amount: *₹${Number(stats.availableBalance || 0).toFixed(2)}*\n`;
        msg += `   Lifetime Earned: *₹${Number(stats.lifetimeEarnings || 0).toFixed(2)}*\n\n`;
      });

      msg += 'Usage: `/sellerstats` or `/sellerstats 50`';
      await ctx.reply(msg, { parse_mode: 'Markdown' });
    } catch (err) {
      logger.error(`sellerstats error: ${err.message}`);
      await ctx.reply('❌ Unable to fetch seller stats right now.');
    }
  });

  // ── /referralstats [limit] — user referral leaderboard ───────────────────
  bot.command('referralstats', requireSuperAdmin, async (ctx) => {
    try {
      const limitArg = parseInt((ctx.message.text.split(' ')[1] || '20').trim(), 10);
      const limit = Number.isFinite(limitArg) ? Math.min(Math.max(limitArg, 1), 100) : 20;

      const referralCounts = await User.aggregate([
        { $match: { referredBy: { $ne: null } } },
        { $group: { _id: '$referredBy', referralCount: { $sum: 1 } } },
        { $sort: { referralCount: -1, _id: 1 } },
        { $limit: limit },
      ]);

      if (!referralCounts.length) return ctx.reply('ℹ️ No referral data found yet.');

      const referrerIds = referralCounts.map((item) => Number(item._id)).filter(Boolean);
      const referrerUsers = await User.find({ telegramId: { $in: referrerIds } })
        .select('telegramId name username')
        .lean();
      const referrerMap = new Map(referrerUsers.map((item) => [Number(item.telegramId), item]));

      let msg = `🤝 *Referral Leaderboard* (Top ${referralCounts.length})\n\n`;
      referralCounts.forEach((item, index) => {
        const userId = Number(item._id);
        const user = referrerMap.get(userId);
        const safeUserName = escapeMarkdown(user?.name || 'N/A');
        const safeUserUsername = user?.username ? ` (@${escapeMarkdown(user.username)})` : '';
        msg += `${index + 1}. User ID: \`${userId}\`\n`;
        msg += `   Name: ${safeUserName}${safeUserUsername}\n`;
        msg += `   Referrals: *${Number(item.referralCount || 0)}*\n\n`;
      });

      msg += 'Usage: `/referralstats` or `/referralstats 50`';
      await ctx.reply(msg, { parse_mode: 'Markdown' });
    } catch (err) {
      logger.error(`referralstats error: ${err.message}`);
      await ctx.reply('❌ Unable to fetch referral stats right now.');
    }
  });

  // ── Seller withdrawal management ──────────────────────────────────────────
  bot.command('sellerwithdrawals', requireSuperAdmin, async (ctx) => {
    const parts = String(ctx.message?.text || '').trim().split(/\s+/);
    const statusArgRaw = String(parts[1] || 'pending').toLowerCase();
    const allowedStatuses = new Set(['pending', 'approved', 'rejected', 'all']);
    const status = allowedStatuses.has(statusArgRaw) ? statusArgRaw : 'pending';
    const sellerId = parts[2] ? parseInt(parts[2], 10) : null;

    if (parts[2] && !sellerId) {
      return ctx.reply('Usage: /sellerwithdrawals [pending|approved|rejected|all] [sellerTelegramId]');
    }

    const items = status === 'pending' && !sellerId
      ? await getPendingSellerWithdrawalRequests(20)
      : await getSellerWithdrawalRequests({ status, limit: 20, sellerTelegramId: sellerId });

    if (!items.length) return ctx.reply(`✅ No ${status} seller withdrawals found.`);

    let msg = `💸 *Seller Withdrawals — ${status.toUpperCase()}*\n`;
    if (sellerId) msg += `Seller Filter: \`${sellerId}\`\n`;
    msg += '\n';

    items.forEach((item, i) => {
      msg += `${i + 1}. ID: \`${item._id}\`\n`;
      msg += `   Seller: \`${item.sellerTelegramId}\`\n`;
      msg += `   Status: *${String(item.status || '').toUpperCase()}*\n`;
      msg += `   UPI: \`${item.upiId || 'N/A'}\`\n`;
      msg += `   Amount: *₹${Number(item.amount).toFixed(2)}*\n`;
      msg += `   Requested: ${new Date(item.requestedAt).toLocaleString('en-IN')}\n\n`;
    });

    msg += 'Approve: `/approvesellerwd <requestId>`\n';
    msg += 'Reject: `/rejectsellerwd <requestId> | reason`\n\n';
    msg += 'Filter usage: `/sellerwithdrawals pending` or `/sellerwithdrawals approved 123456789`';
    await ctx.reply(msg, { parse_mode: 'Markdown' });
  });

  bot.command('approvesellerwd', requireSuperAdmin, async (ctx) => {
    const requestId = (ctx.message.text.split(' ')[1] || '').trim();
    if (!requestId) return ctx.reply('Usage: /approvesellerwd <requestId>');

    try {
      const request = await approveSellerWithdrawal(requestId, ctx.from.id);
      await safeSend(
        bot,
        request.sellerTelegramId,
        `✅ *Seller Withdrawal Approved*\n\n` +
        `Request ID: \`${request._id}\`\n` +
        `Amount: *₹${Number(request.amount).toFixed(2)}*\n\n` +
        `Payout will be processed shortly.`,
        { parse_mode: 'Markdown' }
      );

      await ctx.reply(`✅ Approved withdrawal \`${request._id}\` for ₹${Number(request.amount).toFixed(2)}.`, { parse_mode: 'Markdown' });
    } catch (err) {
      await ctx.reply(`❌ ${err.message}`);
    }
  });

  bot.command('rejectsellerwd', requireSuperAdmin, async (ctx) => {
    const raw = ctx.message.text.replace('/rejectsellerwd', '').trim();
    const [requestIdPart, ...rest] = raw.split('|');
    const requestId = (requestIdPart || '').trim();
    const reason = rest.join('|').trim();

    if (!requestId) return ctx.reply('Usage: /rejectsellerwd <requestId> | reason');

    try {
      const request = await rejectSellerWithdrawal(requestId, ctx.from.id, reason);
      await safeSend(
        bot,
        request.sellerTelegramId,
        `❌ *Seller Withdrawal Rejected*\n\n` +
        `Request ID: \`${request._id}\`\n` +
        `Amount: *₹${Number(request.amount).toFixed(2)}*\n` +
        `${request.note ? `Reason: ${request.note}\n` : ''}`,
        { parse_mode: 'Markdown' }
      );

      await ctx.reply(`✅ Rejected withdrawal \`${request._id}\`.`, { parse_mode: 'Markdown' });
    } catch (err) {
      await ctx.reply(`❌ ${err.message}`);
    }
  });

  // ── /revokeseller <telegramId> — revoke seller program access ───────────
  bot.command('revokeseller', requireSuperAdmin, async (ctx) => {
    const targetId = parseInt((ctx.message.text.split(' ')[1] || '').trim(), 10);
    if (!targetId) return ctx.reply('Usage: /revokeseller <telegramId>');

    try {
      const user = await User.findOne({ telegramId: targetId });
      if (!user) return ctx.reply(`❌ User \`${targetId}\` not found.`, { parse_mode: 'Markdown' });

      if (!user.isSeller && !user.sellerCode) {
        return ctx.reply(`ℹ️ User \`${targetId}\` is not an active seller.`, { parse_mode: 'Markdown' });
      }

      const confirmed = await requireSuperAdminActionConfirmation(
        ctx,
        'revokeseller',
        String(targetId),
        `This will revoke seller access for user \`${targetId}\` and reject all pending seller withdrawals for that seller.`
      );
      if (!confirmed) return;

      const previousSellerCode = user.sellerCode || null;

      const pendingRequests = await SellerWithdrawalRequest.find({
        sellerTelegramId: targetId,
        status: 'pending',
      }).select('_id').lean();

      const pendingRequestIds = pendingRequests.map((request) => String(request._id));
      const rejectedPendingCount = pendingRequestIds.length;

      if (rejectedPendingCount > 0) {
        await SellerWithdrawalRequest.updateMany(
          { _id: { $in: pendingRequestIds } },
          {
            $set: {
              status: 'rejected',
              reviewedAt: new Date(),
              reviewedBy: ctx.from.id,
              note: 'Auto-rejected due to seller revocation',
            },
          }
        );
      }

      await User.findByIdAndUpdate(user._id, {
        $set: { isSeller: false },
        $unset: {
          sellerCode: '',
          'meta.sellerRegisteredAt': '',
        },
      });

      await AdminLog.create({
        adminId: ctx.from.id,
        actionType: 'revoke_seller',
        targetUserId: targetId,
        details: {
          previousSellerCode,
          rejectedPendingCount,
          rejectedPendingRequestIds: pendingRequestIds,
        },
      });

      await safeSend(
        bot,
        targetId,
        `⛔ *Seller Program Revoked*\n\n` +
        `Your seller access has been revoked by admin.\n` +
        (rejectedPendingCount > 0
          ? `Pending withdrawals rejected: *${rejectedPendingCount}*.\n`
          : '') +
        `If this is unexpected, contact support.`,
        { parse_mode: 'Markdown' }
      );

      await logToChannel(
        bot,
        `⛔ *Seller Revoked*\n` +
        `User: \`${targetId}\`\n` +
        `By: ${ctx.from.username ? '@' + escapeMarkdown(ctx.from.username) : escapeMarkdown(String(ctx.from.id))}` +
        (rejectedPendingCount > 0 ? `\nPending WD Rejected: ${rejectedPendingCount}` : '') +
        `${previousSellerCode ? `\nPrevious Code: \`${previousSellerCode}\`` : ''}`
      );

      await ctx.reply(
        `✅ Seller revoked for \`${targetId}\`.` +
        (rejectedPendingCount > 0 ? `\n❌ Rejected pending withdrawals: *${rejectedPendingCount}*` : ''),
        { parse_mode: 'Markdown' }
      );
    } catch (err) {
      logger.error(`revokeseller error: ${err.message}`);
      await ctx.reply(`❌ ${err.message}`);
    }
  });

  // ── /resetsellerid <telegramId> — set sellerReferredBy to 0 ──────────────
  bot.command('resetsellerid', requireSuperAdmin, async (ctx) => {
    const targetId = parseInt((ctx.message.text.split(' ')[1] || '').trim(), 10);
    if (!targetId) return ctx.reply('Usage: /resetsellerid <telegramId>');

    try {
      const user = await User.findOneAndUpdate(
        { telegramId: targetId },
        {
          $set: {
            sellerReferredBy: 0,
            sellerCommissionApplied: false,
          },
        },
        { new: true }
      );

      if (!user) {
        return ctx.reply(`❌ User \`${targetId}\` not found.`, { parse_mode: 'Markdown' });
      }

      await AdminLog.create({
        adminId: ctx.from.id,
        actionType: 'revoke_seller',
        targetUserId: targetId,
        details: {
          command: 'resetsellerid',
          sellerReferredBy: 0,
        },
      });

      await ctx.reply(
        `✅ Seller referral ID reset for user \`${targetId}\`.\n` +
        `New sellerReferredBy: *0*`,
        { parse_mode: 'Markdown' }
      );
    } catch (err) {
      logger.error(`resetsellerid command error: ${err.message}`);
      await ctx.reply('❌ Failed to reset seller ID. Please try again.');
    }
  });

  bot.action(/^swd_(approve|reject)_(.+)$/, async (ctx) => {
    const action = ctx.match[1];
    const requestId = ctx.match[2];

    if (!isSuperAdminUser(ctx.from.id)) {
      return ctx.answerCbQuery('⛔ Super Admin only', { show_alert: true });
    }

    // answerCbQuery may only be called once per callback; errors are sent as a chat message below.
    await ctx.answerCbQuery(action === 'approve' ? 'Approving...' : 'Rejecting...').catch(() => { });

    try {
      let request;
      if (action === 'approve') {
        request = await approveSellerWithdrawal(requestId, ctx.from.id);
        await safeSend(
          bot,
          request.sellerTelegramId,
          `✅ *Seller Withdrawal Approved*\n\n` +
          `Request ID: \`${request._id}\`\n` +
          `Amount: *₹${Number(request.amount).toFixed(2)}*\n\n` +
          `Payout will be processed shortly.`,
          { parse_mode: 'Markdown' }
        );
      } else {
        request = await rejectSellerWithdrawal(requestId, ctx.from.id, 'Rejected from inline action');
        await safeSend(
          bot,
          request.sellerTelegramId,
          `❌ *Seller Withdrawal Rejected*\n\n` +
          `Request ID: \`${request._id}\`\n` +
          `Amount: *₹${Number(request.amount).toFixed(2)}*\n` +
          `Reason: ${request.note}`,
          { parse_mode: 'Markdown' }
        );
      }

      try {
        const who = ctx.from.username
          ? `@${escapeMarkdown(ctx.from.username)}`
          : escapeMarkdown(String(ctx.from.id));
        await ctx.editMessageText(
          `${ctx.callbackQuery.message.text}\n\n` +
          `${action === 'approve' ? '✅ *APPROVED*' : '❌ *REJECTED*'} by ${who}`,
          { parse_mode: 'Markdown' }
        );
      } catch (_) { }
    } catch (err) {
      const text = `❌ Withdrawal action failed: ${String(err?.message || err)}`;
      await ctx.reply(text).catch(() => { });
      logger.error(`swd_${action} error: ${err?.message || err}`);
    }
  });

  // ── /adminlogs — recent admin activity ────────────────────────────────────
  bot.command('adminlogs', requireSuperAdmin, async (ctx) => {
    const logs = await AdminLog.find({}).sort({ timestamp: -1 }).limit(15);
    if (!logs.length) return ctx.reply('No admin logs found.');
    let msg = '📋 *Recent Admin Actions*\n\n';
    logs.forEach(l => {
      msg += `• \`${l.actionType}\` by \`${l.adminId}\``;
      if (l.targetUserId) msg += ` → \`${l.targetUserId}\``;
      msg += `\n  ${new Date(l.timestamp).toLocaleString('en-GB')}\n`;
    });
    await ctx.reply(msg, { parse_mode: 'Markdown' });
  });

  // ── /bcancel ───────────────────────────────────────────────────────────────
  bot.command('bcancel', requireSuperAdmin, async (ctx) => {
    if (clearBroadcastSession(ctx.from.id)) {
      await ctx.reply('✅ Cancelled.');
    }
  });

  const isBlockedBotError = (err) => {
    return err?.code === 403 || err?.response?.error_code === 403;
  };

  const sendBroadcastPayload = async (botInstance, targetTelegramId, sourceChatId, sourceMessage) => {
    if (!sourceMessage?.message_id || !sourceChatId) return { ok: false, blocked: false };

    try {
      await botInstance.telegram.copyMessage(targetTelegramId, sourceChatId, sourceMessage.message_id, {
        protect_content: true,
      });
      return { ok: true, blocked: false };
    } catch (err) {
      if (isBlockedBotError(err)) {
        return { ok: false, blocked: true };
      }

      const fallbackText = String(sourceMessage?.text || sourceMessage?.caption || '').trim();
      if (!fallbackText) return { ok: false, blocked: false };

      try {
        await botInstance.telegram.sendMessage(targetTelegramId, fallbackText, { protect_content: true });
        return { ok: true, blocked: false };
      } catch (fallbackErr) {
        if (isBlockedBotError(fallbackErr)) {
          return { ok: false, blocked: true };
        }
        logger.warn(`broadcast fallback send failed for ${targetTelegramId}: ${fallbackErr.message}`);
        return { ok: false, blocked: false };
      }
    }
  };

  // ── Message handler: broadcast payload capture (text or media) ────────────
  bot.on('message', async (ctx, next) => {
    const session = getBroadcastSession(ctx.from.id);
    if (!session || session.action !== 'broadcast') return next();

    const commandText = String(ctx.message?.text || '').trim();
    if (commandText.startsWith('/')) return next();

    consumeBroadcastSession(ctx.from.id);

    const { target } = session;
    const adminId = ctx.from.id;
    const adminChatId = ctx.chat?.id;
    const sourceChatId = ctx.chat?.id;
    const sourceMessage = ctx.message;

    await ctx.reply('📤 Broadcast started. I will send completion summary once done.', { parse_mode: 'Markdown' });

    setImmediate(async () => {
      try {
        let filter = { role: 'user', isBlocked: false };
        if (target === 'active') filter.status = 'active';
        else if (target === 'expired') filter.status = 'expired';
        else if (target === 'new') {
          const threeDaysAgo = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000);
          filter.createdAt = { $gte: threeDaysAgo };
        }

        const users = await User.find(filter).select('telegramId').lean();

        let sent = 0;
        let failed = 0;
        const blockedUserIds = [];

        for (const user of users) {
          const result = await sendBroadcastPayload(bot, user.telegramId, sourceChatId, sourceMessage);
          if (result.ok) {
            sent += 1;
          } else {
            failed += 1;
            if (result.blocked) blockedUserIds.push(user.telegramId);
          }
          await new Promise((resolve) => setTimeout(resolve, 50));
        }

        let summary =
          `📢 *Broadcast Complete*\n` +
          `🎯 Target: ${target}\n` +
          `👥 Total: ${users.length}\n` +
          `✅ Sent: ${sent}\n` +
          `❌ Failed: ${failed}`;

        if (blockedUserIds.length) {
          const blockedLines = blockedUserIds.map((id) => `• \`${id}\``).join('\n');
          summary += `\n\n🚫 *Blocked Users* (${blockedUserIds.length})\n${blockedLines}`;
        }

        const chunks = splitMarkdownByLength(summary, 3500);
        for (const chunk of chunks) {
          await bot.telegram.sendMessage(adminChatId, chunk, { parse_mode: 'Markdown' }).catch(() => { });
        }

        await logToChannel(
          bot,
          `📢 *Broadcast Complete*\n` +
          `Target: ${target}\n` +
          `Total: ${users.length}\n` +
          `Sent: ${sent}\n` +
          `Failed: ${failed}` +
          (blockedUserIds.length
            ? `\nBlocked Users (${blockedUserIds.length}):\n${blockedUserIds.map((id) => `\`${id}\``).join(', ')}`
            : '')
        );

        await AdminLog.create({
          adminId,
          actionType: 'broadcast',
          details: {
            target,
            total: users.length,
            sent,
            failed,
            blockedUsersCount: blockedUserIds.length,
            blockedUserIds,
            messageType: sourceMessage?.photo ? 'photo'
              : sourceMessage?.video ? 'video'
                : sourceMessage?.document ? 'document'
                  : sourceMessage?.animation ? 'animation'
                    : sourceMessage?.audio ? 'audio'
                      : sourceMessage?.voice ? 'voice'
                        : sourceMessage?.sticker ? 'sticker'
                          : 'text',
            preview: String(sourceMessage?.text || sourceMessage?.caption || '[media]').substring(0, 100),
          },
        });
      } catch (err) {
        logger.error(`broadcast background job error: ${err.message}`);
        await bot.telegram.sendMessage(adminChatId, '❌ Broadcast failed due to an internal error.').catch(() => { });
      }
    });

    return;
  });
};

const formatSalesReport = (title, data) => {
  if (!data.length) return `${title}\n\nNo data found.`;
  let msg = `*${title}*\n\n`;
  let totalSubs = 0, totalRevenue = 0;
  data.forEach(r => {
    msg += `📋 *${r.planName}*: ${r.count} subs — ₹${r.totalRevenue.toFixed(2)}\n`;
    totalSubs += r.count;
    totalRevenue += r.totalRevenue;
  });
  msg += `\n📊 Total: ${totalSubs} — ₹${totalRevenue.toFixed(2)}`;
  return msg;
};

const splitMarkdownByLength = (text, maxLength = 3500) => {
  const normalized = String(text || '').replace(/\r\n/g, '\n');
  if (!normalized) return [];

  const chunks = [];
  let current = '';

  const pushCurrent = () => {
    const trimmed = current.trimEnd();
    if (trimmed) chunks.push(trimmed);
    current = '';
  };

  const appendLine = (line) => {
    const candidate = current ? `${current}${line}\n` : `${line}\n`;
    if (candidate.length <= maxLength) {
      current = candidate;
      return;
    }

    if (current) {
      pushCurrent();
      appendLine(line);
      return;
    }

    let remaining = line;
    while (remaining.length > maxLength - 1) {
      chunks.push(remaining.slice(0, maxLength - 1));
      remaining = remaining.slice(maxLength - 1);
    }
    current = `${remaining}\n`;
  };

  normalized.split('\n').forEach(appendLine);
  if (current.trim()) pushCurrent();
  return chunks;
};

const sendPersistedMarkdownChunks = async (ctx, messages, maxLength = 3500) => {
  const chatId = getChatIdFromCtx(ctx);
  if (!chatId) {
    for (const text of messages) {
      await ctx.reply(text, { parse_mode: 'Markdown' });
    }
    return;
  }

  for (const messageText of messages) {
    const parts = splitMarkdownByLength(messageText, maxLength);
    for (const part of parts) {
      const sent = await ctx.telegram.sendMessage(chatId, part, { parse_mode: 'Markdown' });
      rememberPersistedReportMessage(chatId, sent?.message_id);
    }
  }
};

const buildMarkdownChunks = (header, lines, maxLength = 3500) => {
  const chunks = [];
  let current = header;

  lines.forEach((line) => {
    const next = `${current}${line}\n`;
    if (next.length > maxLength && current !== header) {
      chunks.push(current.trimEnd());
      current = `${header}${line}\n`;
    } else {
      current = next;
    }
  });

  if (current.trim()) {
    chunks.push(current.trimEnd());
  }
  return chunks;
};

const buildCategoryWiseSalesReportMessages = (title, rows, maxLength = 3500) => {
  const categoryOrder = ['movie', 'desi', 'non_desi'];
  const categoryLabel = {
    movie: 'Movie',
    desi: 'Desi',
    non_desi: 'Non-Desi',
  };

  const grouped = {};
  categoryOrder.forEach((key) => {
    grouped[key] = [];
  });

  rows.forEach((row) => {
    const key = categoryOrder.includes(row.planCategory) ? row.planCategory : 'movie';
    grouped[key].push(row);
  });

  const chunks = [];
  chunks.push(`📊 *${title}*`);

  let grandCount = 0;
  let grandRevenue = 0;
  const categorySummaries = [];

  categoryOrder.forEach((key) => {
    const items = grouped[key] || [];
    let categoryRevenue = 0;
    const sectionLines = [];
    sectionLines.push(`*${categoryLabel[key]} List*`);

    if (!items.length) {
      sectionLines.push('_No sales_');
    } else {
      items.forEach((row, index) => {
        const price = Number(row.planPrice || 0);
        categoryRevenue += price;
        const safePlanName = escapeTelegramMarkdown(String(row.planName || 'Plan'));
        sectionLines.push(`${index + 1}. \`${row.telegramId}\` | ${safePlanName} | ₹${price.toFixed(2)}`);
      });
    }

    sectionLines.push(`Subtotal: *${items.length}* sales | *₹${categoryRevenue.toFixed(2)}*`);

    const sectionHeader = `📊 *${title}*\n\n`;
    const sectionChunks = buildMarkdownChunks(sectionHeader, sectionLines, maxLength);
    chunks.push(...sectionChunks);

    grandCount += items.length;
    grandRevenue += categoryRevenue;
    categorySummaries.push({ key, count: items.length, revenue: categoryRevenue });
  });

  let summarySection = '*Summary*\n';
  categorySummaries.forEach((row) => {
    summarySection += `${categoryLabel[row.key]}: *${row.count}* sales | *₹${row.revenue.toFixed(2)}*\n`;
  });
  summarySection += `\nTotal Sales: *${grandCount}*\n`;
  summarySection += `Total Revenue: *₹${grandRevenue.toFixed(2)}*`;
  chunks.push(summarySection.trimEnd());

  return chunks;
};

const chunkMarkdownSections = (sections, maxLength = 3500) => {
  const chunks = [];
  let current = '';

  sections.forEach((section) => {
    const block = `${section}\n\n`;
    if ((current + block).length > maxLength && current.length > 0) {
      chunks.push(current.trimEnd());
      current = block;
    } else {
      current += block;
    }
  });

  if (current.trim()) {
    chunks.push(current.trimEnd());
  }

  return chunks;
};

const escapeTelegramMarkdown = (value) => {
  return String(value || '').replace(/([_*\[\]()`])/g, '\\$1');
};

const parseReportDuration = (token) => {
  const match = String(token || '').match(/^(\d+)([dm])$/i);
  if (!match) return null;

  const value = parseInt(match[1], 10);
  const unit = match[2].toLowerCase();
  if (!value || value <= 0) return null;
  if (unit === 'd' && value > 365) return null;
  if (unit === 'm' && value > 24) return null;
  return { value, unit };
};

const toCsvCell = (value) => {
  const raw = value === null || typeof value === 'undefined' ? '' : String(value);
  if (/[",\n]/.test(raw)) {
    return `"${raw.replace(/"/g, '""')}"`;
  }
  return raw;
};

const toCsvRow = (columns) => {
  return columns.map(toCsvCell).join(',');
};

module.exports = { registerSuperAdminHandlers };
