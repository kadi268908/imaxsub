// src/bot/adminHandlers.js
// Admin handlers:
//   • approve/reject access requests (inline buttons in log channel)
//   • /user <id> — full user profile
//   • /history <id> — subscription / purchase history for a user
//   • /plans — admin utilities
const crypto = require('crypto');
const User = require('../models/User');
const Request = require('../models/Request');
const Plan = require('../models/Plan');
const Offer = require('../models/Offer');
const Subscription = require('../models/Subscription');
const AdminLog = require('../models/AdminLog');
const UserOffer = require('../models/UserOffer');
const DmWordFilter = require('../models/DmWordFilter');
const { createSubscription, syncUserStatusFromSubscriptions } = require('../services/subscriptionService');
const { approveRequest, rejectRequest, getActivePlans } = require('../services/adminService');
const { awardReferralBonus, awardSellerCommission } = require('../services/referralService');
const { SUPPORT_CONTACT } = require('../services/supportService');
const { formatDate, daysRemaining, addDays, startOfToday } = require('../utils/dateUtils');
const { logToChannel } = require('../services/cronService');
const { generateInviteLink, revokeInviteLink, isGroupMember, safeSend, safeSendAndPin, banFromGroup, unbanFromGroup } = require('../utils/telegramUtils');
const { PLAN_CATEGORY, normalizePlanCategory, getGroupIdForCategory, getAllPremiumGroupIds } = require('../utils/premiumGroups');
const logger = require('../utils/logger');
const { isTransactionUnsupportedError } = require('../utils/mongoErrors');
const ADMIN_ACTION_CONFIRM_TTL_SECONDS = Math.max(10, parseInt(process.env.ADMIN_ACTION_CONFIRM_TTL_SECONDS || '45', 10));
/** @type Map<string, { actionType: string, adminTelegramId: number, expiresAt: number, payload: object }> */
const pendingAdminButtonConfirmations = new Map();
const approvalRequestLocks = new Set();
const withStyle = (button, style) => ({ ...button, style });

const getCategoryShortLabel = (category) => {
  const normalized = normalizePlanCategory(category);
  if (normalized === PLAN_CATEGORY.MOVIE) return 'Movie';
  if (normalized === PLAN_CATEGORY.DESI) return 'Desi';
  if (normalized === PLAN_CATEGORY.NON_DESI) return 'Non Desi';
  return normalized;
};

const getSuperAdminIds = () => {
  return String(process.env.SUPER_ADMIN_IDS || process.env.SUPER_ADMIN_ID || '')
    .split(',')
    .map(id => parseInt(id.trim(), 10))
    .filter(Boolean);
};

const requireAdmin = async (ctx, next) => {
  const user = await User.findOne({ telegramId: ctx.from.id });
  if (!user || !['admin', 'superadmin'].includes(user.role)) {
    return ctx.reply('⛔ Access denied. Admins only.');
  }
  ctx.adminUser = user;
  return next();
};

const getSubscriptionGroupId = (subscription) => {
  if (subscription?.premiumGroupId) return String(subscription.premiumGroupId);
  return getGroupIdForCategory(subscription?.planCategory || subscription?.planId?.category || 'movie');
};

const clearStoredInviteLink = async (subscriptionId) => {
  if (!subscriptionId) return;
  await Subscription.findByIdAndUpdate(subscriptionId, {
    inviteLink: null,
    inviteLinkIssuedAt: null,
    inviteLinkTtlMinutes: null,
  });
};

const revokeSubscriptionInviteLink = async (bot, subscription) => {
  if (!subscription?.inviteLink) return;
  const groupId = getSubscriptionGroupId(subscription);
  if (!groupId) {
    await clearStoredInviteLink(subscription._id);
    return;
  }

  await revokeInviteLink(bot, groupId, subscription.inviteLink);
  await clearStoredInviteLink(subscription._id);
};

const VALID_PLAN_CATEGORIES = new Set(Object.values(PLAN_CATEGORY));

const parseCategoryInput = (value) => {
  if (!value) return null;
  const normalized = String(value).toLowerCase().replace(/[-\s]/g, '_');
  if (!VALID_PLAN_CATEGORIES.has(normalized)) return null;
  return normalized;
};

const getActiveSubscriptions = async (telegramId) => {
  return Subscription.find({
    telegramId,
    status: 'active',
  }).sort({ expiryDate: -1, createdAt: -1 });
};

const resolveSubscriptionForAdminAction = async (telegramId, categoryInput = null) => {
  const subscriptions = await getActiveSubscriptions(telegramId);
  if (!subscriptions.length) {
    return { error: 'none' };
  }

  const normalizedCategory = categoryInput ? parseCategoryInput(categoryInput) : null;
  if (categoryInput && !normalizedCategory) {
    return { error: 'invalid_category' };
  }

  if (normalizedCategory) {
    const matched = subscriptions.find(
      (sub) => normalizePlanCategory(sub.planCategory || sub.planId?.category || 'movie') === normalizedCategory
    );
    if (!matched) {
      return { error: 'category_not_found', normalizedCategory, subscriptions };
    }
    return { subscription: matched, normalizedCategory, subscriptions };
  }

  if (subscriptions.length > 1) {
    return { error: 'ambiguous', subscriptions };
  }

  return {
    subscription: subscriptions[0],
    normalizedCategory: normalizePlanCategory(subscriptions[0].planCategory || subscriptions[0].planId?.category || 'movie'),
    subscriptions,
  };
};

const promptAdminButtonConfirmation = async (ctx, { actionType, payload, summaryMarkdown }) => {
  const token = crypto.randomBytes(16).toString('hex');
  const expiresAt = Date.now() + (ADMIN_ACTION_CONFIRM_TTL_SECONDS * 1000);
  pendingAdminButtonConfirmations.set(token, {
    actionType,
    adminTelegramId: ctx.from.id,
    expiresAt,
    payload,
  });
  await ctx.reply(
    `⚠️ *Confirmation Required*\n\n${summaryMarkdown}\n\n` +
    `Tap *Confirm* to proceed or *Cancel* to dismiss. (${ADMIN_ACTION_CONFIRM_TTL_SECONDS}s)`,
    {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [[
          withStyle({ text: '✅ Confirm', callback_data: `adm_conf_${token}` }, 'success'),
          withStyle({ text: '❌ Cancel', callback_data: `adm_can_${token}` }, 'danger'),
        ]],
      },
    }
  );
};

const formatSubscriptionCategoryList = (subscriptions) => {
  return subscriptions
    .map((sub) => {
      const category = normalizePlanCategory(sub.planCategory || sub.planId?.category || 'movie');
      return `- ${category}: ${sub.planName} (expires ${formatDate(sub.expiryDate)})`;
    })
    .join('\n');
};

const getDiscountedPrice = (price, discountPercent) => {
  const base = Number(price || 0);
  const discount = Number(discountPercent || 0);
  if (!base || discount <= 0) return base;
  return Math.ceil(Math.max(0, base - (base * discount / 100)));
};

const escapeMarkdown = (value) => {
  return String(value ?? '').replace(/([\\_*`\[])/g, '\\$1');
};

const isMessageNotModifiedError = (err) => {
  const message = err?.response?.description || err?.description || err?.message || '';
  return String(message).toLowerCase().includes('message is not modified');
};

const isParseEntityError = (err) => {
  const message = err?.response?.description || err?.description || err?.message || '';
  return String(message).toLowerCase().includes("can't parse entities");
};

const markdownToPlainText = (text) => {
  return String(text || '')
    .replace(/\\([\\_*`\[])/g, '$1')
    .replace(/`/g, '');
};

const replyMarkdownOrPlain = async (ctx, text, extra = {}) => {
  try {
    await ctx.reply(text, { parse_mode: 'Markdown', ...extra });
  } catch (err) {
    if (!isParseEntityError(err)) throw err;
    await ctx.reply(markdownToPlainText(text), extra);
  }
};

const REJECT_REASON_PROMPT_MD = '⚠️ *Select rejection reason below:*';
const REJECT_REASON_PROMPT_PLAIN = '⚠️ Select rejection reason below:';

const stripRejectReasonPromptFromText = (text) => {
  return String(text || '')
    .replace(/\n*\s*⚠️\s*\*?Select rejection reason below:\*?\s*/gi, '\n')
    .trimEnd();
};

const appendRejectReasonPromptOnce = (text) => {
  const cleaned = stripRejectReasonPromptFromText(text);
  return `${cleaned}\n\n${REJECT_REASON_PROMPT_MD}`;
};

/** After approve: append status to log message and remove plan/reject buttons (works for photo, document, or text). */
const applyApprovalTransformToLogMessage = async (ctx, { planName, allRenewal }) => {
  const msg = ctx.callbackQuery?.message;
  if (!msg) return;

  const approver = ctx.from.username
    ? `@${escapeMarkdown(ctx.from.username)}`
    : `\`${ctx.from.id}\``;
  const safePlan = escapeMarkdown(planName);
  const footerMd = `\n\n✅ *APPROVED* by ${approver} — *${safePlan}*${allRenewal ? '\n📌 _Renewal approval_' : ''}`;
  const footerPlain = `\n\n✅ APPROVED by ${ctx.from.username ? `@${ctx.from.username}` : ctx.from.id} — ${planName}${allRenewal ? ' [RENEWAL]' : ''}`;

  const hasPhoto = Array.isArray(msg.photo) && msg.photo.length > 0;
  const hasDocument = Boolean(msg.document);

  if (hasPhoto || hasDocument) {
    const base = stripRejectReasonPromptFromText(String(msg.caption || ''));
    let nextCaption = base + footerMd;
    if (nextCaption.length > 1024) {
      const reserve = Math.min(footerPlain.length + 10, 200);
      nextCaption = `${base.slice(0, Math.max(0, 1024 - reserve))}…${footerPlain}`;
    }
    try {
      await ctx.editMessageCaption(nextCaption, { parse_mode: 'Markdown' });
    } catch (err) {
      if (isMessageNotModifiedError(err)) return;
      try {
        const plainCap = base.length > 1024 - footerPlain.length
          ? `${base.slice(0, Math.max(0, 1024 - footerPlain.length - 2))}…${footerPlain}`
          : base + footerPlain;
        await ctx.editMessageCaption(plainCap);
      } catch (_) { /* ignore */ }
    }
    try {
      await ctx.editMessageReplyMarkup({ inline_keyboard: [] });
    } catch (err) {
      if (!isMessageNotModifiedError(err)) { /* ignore */ }
    }
    return;
  }

  if (msg.text !== undefined) {
    const base = stripRejectReasonPromptFromText(String(msg.text || ''));
    try {
      await ctx.editMessageText(base + footerMd, {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [] },
      });
    } catch (err) {
      if (isMessageNotModifiedError(err)) return;
      try {
        await ctx.editMessageText(base + footerPlain, {
          reply_markup: { inline_keyboard: [] },
        });
      } catch (_) { /* ignore */ }
    }
  }
};

/** After reject: append status + reason, remove all inline buttons (photo, document, or text). */
const applyRejectTransformToLogMessage = async (ctx, { reasonLabel }) => {
  const msg = ctx.callbackQuery?.message;
  if (!msg) return;

  const safeReason = escapeMarkdown(reasonLabel);
  const approver = ctx.from.username
    ? `@${escapeMarkdown(ctx.from.username)}`
    : `\`${ctx.from.id}\``;
  const footerMd = `\n\n❌ *REJECTED* by ${approver}\nReason: *${safeReason}*`;
  const footerPlain = `\n\n❌ REJECTED by ${ctx.from.username ? `@${ctx.from.username}` : ctx.from.id}\nReason: ${reasonLabel}`;

  const hasPhoto = Array.isArray(msg.photo) && msg.photo.length > 0;
  const hasDocument = Boolean(msg.document);

  if (hasPhoto || hasDocument) {
    const base = stripRejectReasonPromptFromText(String(msg.caption || ''));
    let nextCaption = base + footerMd;
    if (nextCaption.length > 1024) {
      const reserve = Math.min(footerPlain.length + 10, 220);
      nextCaption = `${base.slice(0, Math.max(0, 1024 - reserve))}…${footerPlain}`;
    }
    try {
      await ctx.editMessageCaption(nextCaption, { parse_mode: 'Markdown' });
    } catch (err) {
      if (isMessageNotModifiedError(err)) return;
      try {
        const plainCap = base.length > 1024 - footerPlain.length
          ? `${base.slice(0, Math.max(0, 1024 - footerPlain.length - 2))}…${footerPlain}`
          : base + footerPlain;
        await ctx.editMessageCaption(plainCap);
      } catch (_) { /* ignore */ }
    }
    try {
      await ctx.editMessageReplyMarkup({ inline_keyboard: [] });
    } catch (err) {
      if (!isMessageNotModifiedError(err)) { /* ignore */ }
    }
    return;
  }

  if (msg.text !== undefined) {
    const base = stripRejectReasonPromptFromText(String(msg.text || ''));
    try {
      await ctx.editMessageText(base + footerMd, {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [] },
      });
    } catch (err) {
      if (isMessageNotModifiedError(err)) return;
      try {
        await ctx.editMessageText(base + footerPlain, {
          reply_markup: { inline_keyboard: [] },
        });
      } catch (_) { /* ignore */ }
    }
  }
};

const toTelegramContactUrl = (contact) => {
  const value = String(contact || '').trim();
  if (!value) return 'https://t.me/ImaxSupport1Bot';
  if (/^https?:\/\//i.test(value)) return value;
  const username = value.startsWith('@') ? value.slice(1) : value;
  return `https://t.me/${username}`;
};

const SUPPORT_CONTACT_URL = toTelegramContactUrl(SUPPORT_CONTACT);

const normalizeFilterPhrase = (value) => String(value || '').trim().toLowerCase();

const parseFilterPhrase = (text = '', command = 'filter') => {
  const escapedCommand = String(command || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const raw = String(text || '').replace(new RegExp(`^\\/${escapedCommand}\\b`, 'i'), '').trim();
  if (!raw) return '';

  const quoted = raw.match(/^"([\s\S]+)"$/);
  if (quoted) return quoted[1].trim();
  return raw;
};

const extractFilterResponseFromReply = (replyMessage) => {
  if (!replyMessage) {
    return { error: 'missing_reply' };
  }

  if (replyMessage.sticker?.file_id) {
    return {
      responseType: 'sticker',
      responseStickerFileId: replyMessage.sticker.file_id,
      responseText: null,
      responsePhotoFileId: null,
      responseCaption: null,
    };
  }

  if (Array.isArray(replyMessage.photo) && replyMessage.photo.length > 0) {
    const bestPhoto = replyMessage.photo[replyMessage.photo.length - 1];
    return {
      responseType: 'photo',
      responsePhotoFileId: bestPhoto.file_id,
      responseCaption: String(replyMessage.caption || '').trim() || null,
      responseText: null,
      responseStickerFileId: null,
    };
  }

  const text = String(replyMessage.text || '').trim();
  if (text) {
    return {
      responseType: 'text',
      responseText: text,
      responsePhotoFileId: null,
      responseStickerFileId: null,
      responseCaption: null,
    };
  }

  return { error: 'unsupported_reply' };
};

const registerAdminHandlers = (bot) => {

  const REJECT_REASON_OPTIONS = {
    old_ss: 'Old Screenshot',
    fake_ss: 'Fake Screenshot',
    wrong_plan: 'Wrong Plan selected',
    others: 'Others',
  };

  const runRevokePlanConfirmed = async (ctx, { targetId, categoryInput }) => {
    try {
      const resolved = await resolveSubscriptionForAdminAction(targetId, categoryInput);
      if (resolved.error === 'none') {
        return ctx.reply('ℹ️ No active subscription found (state changed).');
      }
      if (resolved.error === 'invalid_category') {
        return ctx.reply('❌ Invalid category. Use movie, desi, or non_desi.');
      }
      if (resolved.error === 'category_not_found') {
        return ctx.reply(
          `❌ No active subscription found in category *${resolved.normalizedCategory}*.`,
          { parse_mode: 'Markdown' }
        );
      }
      if (resolved.error === 'ambiguous') {
        return ctx.reply(
          `⚠️ Multiple active subscriptions found. Please pass category.\n\n` +
          `Usage: /revokeplan <telegramId> [category]\n\n` +
          `${formatSubscriptionCategoryList(resolved.subscriptions)}`
        );
      }

      const activeSub = resolved.subscription;
      const resolvedCategory = normalizePlanCategory(activeSub.planCategory || activeSub.planId?.category || 'movie');
      const revokedAt = new Date();
      activeSub.status = 'cancelled';
      activeSub.expiryDate = revokedAt;
      await activeSub.save();

      const remainingActiveSubs = await Subscription.countDocuments({
        telegramId: targetId,
        status: 'active',
      });

      await User.findOneAndUpdate(
        { telegramId: targetId },
        {
          status: remainingActiveSubs > 0 ? 'active' : 'inactive',
          lastInteraction: new Date(),
        }
      );

      const currentGroupId = getSubscriptionGroupId(activeSub);
      await revokeSubscriptionInviteLink(bot, activeSub);
      if (currentGroupId) {
        await banFromGroup(bot, currentGroupId, targetId);
      }

      await safeSend(
        bot,
        targetId,
        `⚠️ *Your subscription has been revoked by admin.*\n\n` +
        `Agar koi issue hai to *Contact Support* button ya /support use karein.\n\n`,
        { parse_mode: 'Markdown' }
      );

      await AdminLog.create({
        adminId: ctx.from.id,
        actionType: 'manual_expire',
        targetUserId: targetId,
        details: {
          reason: 'Revoke incorrect approval',
          revokedSubscriptionId: activeSub._id,
          previousPlan: activeSub.planName,
          category: resolvedCategory,
        },
      });

      await ctx.reply(
        `✅ Plan revoked for user \`${targetId}\`.\nCategory: *${resolvedCategory}*\nPrevious plan: *${activeSub.planName}*\nUser removed from premium group.`,
        { parse_mode: 'Markdown' }
      );
    } catch (err) {
      logger.error(`revokeplan confirmed error: ${err.message}`);
      await ctx.reply('❌ Failed to revoke plan. Please try again.');
    }
  };

  const runModifyPlanConfirmed = async (ctx, payload) => {
    const {
      targetId,
      planId,
      subscriptionId,
      resetExpiryFromToday,
    } = payload;
    try {
      const plan = await Plan.findById(planId);
      const activeSub = await Subscription.findOne({
        _id: subscriptionId,
        telegramId: targetId,
        status: 'active',
      }).populate('planId');

      if (!plan || !activeSub) {
        return ctx.reply('ℹ️ Subscription or plan changed; please run /modifyplan again.');
      }

      const targetCategory = normalizePlanCategory(activeSub.planCategory || activeSub.planId?.category || 'movie');
      const planCategory = normalizePlanCategory(plan.category || 'movie');
      if (planCategory !== targetCategory) {
        return ctx.reply(
          `❌ Plan category mismatch.\n` +
          `You are editing *${escapeMarkdown(targetCategory)}* subscription but selected plan is *${escapeMarkdown(planCategory)}*.\n\n` +
          `Pick a plan in the same category, or use a different tool to move users between categories.`,
          { parse_mode: 'Markdown' }
        );
      }

      const oldGroupId = getSubscriptionGroupId(activeSub);
      const previousPlan = activeSub.planName;
      const now = new Date();

      let newExpiry;
      let durationDaysToStore;
      if (resetExpiryFromToday) {
        newExpiry = new Date(now.getTime() + (plan.durationDays * 24 * 60 * 60 * 1000));
        durationDaysToStore = plan.durationDays;
      } else {
        newExpiry = activeSub.expiryDate ? new Date(activeSub.expiryDate) : null;
        if (!newExpiry || newExpiry.getTime() <= now.getTime()) {
          return ctx.reply(
            'ℹ️ This subscription is already expired or has no valid expiry.\n' +
            'Use the same command with `|reset` last to start a fresh period from today.'
          );
        }
        const msPerDay = 24 * 60 * 60 * 1000;
        durationDaysToStore = Math.max(1, Math.ceil((newExpiry.getTime() - now.getTime()) / msPerDay));
      }

      const newPlanCategory = planCategory;
      const newGroupId = getGroupIdForCategory(newPlanCategory);
      if (!newGroupId) {
        return ctx.reply(`❌ Premium group not configured for category: ${newPlanCategory}`);
      }

      activeSub.planId = plan._id;
      activeSub.planName = plan.name;
      activeSub.planCategory = newPlanCategory;
      activeSub.premiumGroupId = newGroupId;
      activeSub.durationDays = durationDaysToStore;
      activeSub.expiryDate = newExpiry;
      activeSub.status = 'active';
      activeSub.approvedBy = ctx.from.id;
      activeSub.isRenewal = false;
      if (resetExpiryFromToday) {
        activeSub.startDate = now;
        activeSub.reminderFlags = { day7: false, day3: false, day1: false, day0: false };
      }
      await activeSub.save();

      await User.findOneAndUpdate(
        { telegramId: targetId },
        { status: 'active', lastInteraction: new Date() }
      );

      if (oldGroupId && String(oldGroupId) !== String(newGroupId)) {
        await revokeSubscriptionInviteLink(bot, activeSub);
        await banFromGroup(bot, oldGroupId, targetId);
      }

      const alreadyInGroup = await isGroupMember(bot, newGroupId, targetId);
      const extra = { parse_mode: 'Markdown' };
      let userMsg =
        `✅ *Your subscription plan has been updated by admin.*\n\n` +
        `📋 New Plan: *${plan.name}*\n` +
        `📅 Remaining / term: *${durationDaysToStore} days*\n` +
        `⏰ Expires on: *${formatDate(newExpiry)}*`;

      let shouldPinInviteMessage = false;
      if (!alreadyInGroup) {
        await revokeSubscriptionInviteLink(bot, activeSub);
        await unbanFromGroup(bot, newGroupId, targetId);
        const inviteLink = await generateInviteLink(bot, newGroupId, targetId, newExpiry);
        if (inviteLink) {
          extra.reply_markup = {
            inline_keyboard: [[{ text: '🔗 Join Premium Group', url: inviteLink, style: 'success' }]],
          };
          userMsg += `\n\nGroup join karne ke liye niche button par click karein.`;
          shouldPinInviteMessage = true;
          await Subscription.findByIdAndUpdate(activeSub._id, {
            inviteLink,
            inviteLinkIssuedAt: new Date(),
            inviteLinkTtlMinutes: Math.max(1, parseInt(process.env.INVITE_LINK_TTL_MINUTES || '10', 10)),
          });
        }
      } else {
        userMsg += `\n\n✅ Aap already premium group me ho. Isliye naya invite link nahi bheja gaya.`;
        extra.reply_markup = {
          inline_keyboard: [[{ text: '🎫 Support Chat', url: SUPPORT_CONTACT_URL, style: 'primary' }]],
        };
      }

      await safeSendAndPin(bot, targetId, userMsg, extra, { pin: shouldPinInviteMessage });

      await AdminLog.create({
        adminId: ctx.from.id,
        actionType: 'edit_plan',
        targetUserId: targetId,
        details: {
          reason: 'Correct wrong selected plan',
          subscriptionId: activeSub._id,
          previousPlan,
          previousCategory: targetCategory,
          newPlan: plan.name,
          newCategory: newPlanCategory,
          planTemplateDurationDays: plan.durationDays,
          storedDurationDays: durationDaysToStore,
          expiryMode: resetExpiryFromToday ? 'reset_from_today' : 'preserve_existing',
          newExpiry: newExpiry.toISOString(),
        },
      });

      await ctx.reply(
        `✅ Plan updated for user \`${targetId}\`.\n` +
        `*${escapeMarkdown(previousPlan)}* → *${escapeMarkdown(plan.name)}*\n` +
        `Category: *${escapeMarkdown(newPlanCategory)}*\n` +
        `${resetExpiryFromToday ? 'Expiry' : 'Expiry (unchanged)'}: *${escapeMarkdown(formatDate(newExpiry))}*\n` +
        `Mode: *${resetExpiryFromToday ? 'reset from today' : 'preserve expiry'}*`,
        { parse_mode: 'Markdown' }
      );
    } catch (err) {
      logger.error(`modifyplan confirmed error: ${err.message}`);
      await ctx.reply('❌ Failed to modify plan. Please try again.');
    }
  };

  bot.action(/^adm_conf_([a-f0-9]{32})$/i, requireAdmin, async (ctx) => {
    const token = ctx.match[1];
    const entry = pendingAdminButtonConfirmations.get(token);
    if (!entry) {
      await ctx.answerCbQuery('ℹ️ Expired or already used', { show_alert: true });
      return;
    }
    if (ctx.from.id !== entry.adminTelegramId) {
      return ctx.answerCbQuery('⛔ Not your confirmation', { show_alert: true });
    }
    if (Date.now() > entry.expiresAt) {
      pendingAdminButtonConfirmations.delete(token);
      await ctx.answerCbQuery('⏱ Confirmation expired', { show_alert: true });
      try { await ctx.editMessageReplyMarkup({ inline_keyboard: [] }); } catch (_) { }
      return;
    }
    pendingAdminButtonConfirmations.delete(token);
    await ctx.answerCbQuery('Processing...');
    try {
      await ctx.editMessageReplyMarkup({ inline_keyboard: [] });
    } catch (_) { }
    if (entry.actionType === 'revokeplan') {
      return runRevokePlanConfirmed(ctx, entry.payload);
    }
    if (entry.actionType === 'modifyplan') {
      return runModifyPlanConfirmed(ctx, entry.payload);
    }
  });

  bot.action(/^adm_can_([a-f0-9]{32})$/i, requireAdmin, async (ctx) => {
    const token = ctx.match[1];
    const entry = pendingAdminButtonConfirmations.get(token);
    if (!entry) {
      return ctx.answerCbQuery('ℹ️ Already dismissed', { show_alert: false });
    }
    if (ctx.from.id !== entry.adminTelegramId) {
      return ctx.answerCbQuery('⛔ Not your confirmation', { show_alert: true });
    }
    pendingAdminButtonConfirmations.delete(token);
    await ctx.answerCbQuery('Cancelled');
    try {
      await ctx.editMessageReplyMarkup({ inline_keyboard: [] });
    } catch (_) { }
  });

  // ── /filter <phrase> — reply-based DM trigger/response mapping ────────────
  bot.command('filter', requireAdmin, async (ctx) => {
    if (ctx.chat?.type !== 'private') {
      return ctx.reply('❌ This command only works in DM/private chat.');
    }

    const phrase = parseFilterPhrase(ctx.message?.text || '');
    const normalizedPhrase = normalizeFilterPhrase(phrase);
    const responsePayload = extractFilterResponseFromReply(ctx.message?.reply_to_message);

    if (!normalizedPhrase) {
      return ctx.reply('Usage: reply to a message/photo/sticker with /filter "Any Word"');
    }

    if (responsePayload.error === 'missing_reply') {
      return ctx.reply('❌ Reply required. Reply to text/photo/sticker and send /filter "Any Word"');
    }

    if (responsePayload.error === 'unsupported_reply') {
      return ctx.reply('❌ Unsupported reply type. Use text/emoji, photo (with or without caption), or sticker.');
    }

    try {
      const existing = await DmWordFilter.findOne({ normalizedPhrase });
      let saved;

      if (existing) {
        existing.phrase = phrase;
        existing.createdBy = ctx.from.id;
        existing.responseType = responsePayload.responseType;
        existing.responseText = responsePayload.responseText;
        existing.responsePhotoFileId = responsePayload.responsePhotoFileId;
        existing.responseStickerFileId = responsePayload.responseStickerFileId;
        existing.responseCaption = responsePayload.responseCaption;
        saved = await existing.save();
      } else {
        saved = await DmWordFilter.create({
          phrase,
          normalizedPhrase,
          createdBy: ctx.from.id,
          ...responsePayload,
        });
      }

      await AdminLog.create({
        adminId: ctx.from.id,
        actionType: 'add_filter_word',
        details: {
          phrase: saved.phrase,
          responseType: saved.responseType,
          mode: existing ? 'updated' : 'created',
        },
      });

      await ctx.reply(
        `✅ DM filter ${existing ? 'updated' : 'added'}: "${saved.phrase}"\n` +
        `Response type: ${saved.responseType}`
      );
    } catch (err) {
      logger.error(`filter command error: ${err.message}`);
      await ctx.reply('❌ Failed to add filter. Please try again.');
    }
  });

  // ── /unfilter <phrase> — remove DM text filter phrase (admins + superadmins) ──
  bot.command('unfilter', requireAdmin, async (ctx) => {
    if (ctx.chat?.type !== 'private') {
      return ctx.reply('❌ This command only works in DM/private chat.');
    }

    const phrase = parseFilterPhrase(ctx.message?.text || '', 'unfilter');
    const normalizedPhrase = normalizeFilterPhrase(phrase);

    if (!normalizedPhrase) {
      return ctx.reply('Usage: /unfilter "Any Word"');
    }

    try {
      const removed = await DmWordFilter.findOneAndDelete({ normalizedPhrase });
      if (!removed) {
        return ctx.reply(`ℹ️ Filter not found: "${phrase}"`);
      }

      await AdminLog.create({
        adminId: ctx.from.id,
        actionType: 'remove_filter_word',
        details: { phrase: removed.phrase },
      });

      await ctx.reply(`✅ DM filter removed: "${removed.phrase}"`);
    } catch (err) {
      logger.error(`unfilter command error: ${err.message}`);
      await ctx.reply('❌ Failed to remove filter. Please try again.');
    }
  });

  // ── /filters — list all DM filters (admins + superadmins) ────────────────
  bot.command('filters', requireAdmin, async (ctx) => {
    if (ctx.chat?.type !== 'private') {
      return ctx.reply('❌ This command only works in DM/private chat.');
    }

    try {
      const filters = await DmWordFilter.find({})
        .select('phrase responseType createdAt')
        .sort({ createdAt: -1 })
        .lean();

      if (!filters.length) {
        return ctx.reply('ℹ️ No filters found.');
      }

      let msg = `🧩 *DM Filters* (${filters.length})\n\n`;
      filters.forEach((item, index) => {
        msg += `${index + 1}. "${item.phrase}" → *${item.responseType}*\n`;
      });

      msg += `\nRemove with: /unfilter "Any Word"`;
      await ctx.reply(msg, { parse_mode: 'Markdown' });
    } catch (err) {
      logger.error(`filters command error: ${err.message}`);
      await ctx.reply('❌ Failed to fetch filters. Please try again.');
    }
  });

  // ── Approve subscription request ───────────────────────────────────────────
  // callback_data: approve_<requestId>_<planId|days>
  bot.action(/^approve_(.+)_(.+)$/, async (ctx) => {
    const [, requestId] = ctx.match;
    if (approvalRequestLocks.has(requestId)) {
      return ctx.answerCbQuery('⏳ Request already being processed', { show_alert: true });
    }
    approvalRequestLocks.add(requestId);

    await ctx.answerCbQuery('Processing...');
    let acquiredProcessingLock = false;
    let processingToken = null;
    try {
      const [, , planOrDays] = ctx.match;

      const adminUser = await User.findOne({ telegramId: ctx.from.id });
      if (!adminUser || !['admin', 'superadmin'].includes(adminUser.role)) {
        return ctx.answerCbQuery('⛔ Admins only', { show_alert: true });
      }

      processingToken = `apr_${ctx.from.id}_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
      const request = await Request.findOneAndUpdate(
        {
          _id: requestId,
          status: 'pending',
        },
        {
          $set: {
            status: 'processing',
            processingBy: ctx.from.id,
            processingAt: new Date(),
            processingToken,
            processingError: null,
          },
        },
        { new: true }
      );

      if (!request) {
        const current = await Request.findById(requestId).select('status processingBy').lean();
        if (!current) return ctx.answerCbQuery('❌ Request not found', { show_alert: true });
        if (current.status === 'processing') {
          const by = current.processingBy ? ` by ${current.processingBy}` : '';
          return ctx.answerCbQuery(`⏳ Already being processed${by}`, { show_alert: true });
        }
        return ctx.answerCbQuery('ℹ️ Already processed', { show_alert: true });
      }
      acquiredProcessingLock = true;

      const requestCategory = normalizePlanCategory(request.requestCategory || 'movie');

      // Resolve plan by _id or durationDays
      let plan = await Plan.findById(planOrDays).catch(() => null);
      if (!plan) {
        const days = parseInt(planOrDays);
        plan = await Plan.findOne({ durationDays: days, isActive: true, category: requestCategory });
        if (!plan) {
          plan = await Plan.create({ name: `${days} Days Plan`, durationDays: days, price: 0, category: requestCategory });
        }
      }

      const resolvedPlanCategory = normalizePlanCategory(plan.category || requestCategory);
      if (resolvedPlanCategory !== requestCategory) {
        return ctx.answerCbQuery('❌ Plan category mismatch for this request', { show_alert: true });
      }

      const targetCategories = [resolvedPlanCategory];
      const categoryGroupPairs = targetCategories.map((category) => ({
        category,
        groupId: getGroupIdForCategory(category),
      }));

      if (categoryGroupPairs.some((item) => !item.groupId)) {
        return ctx.answerCbQuery('❌ Premium group not configured for one or more categories', { show_alert: true });
      }

      const subscriptions = [];
      const session = await Request.startSession();
      let usedTransaction = false;
      try {
        await session.withTransaction(async () => {
          for (const item of categoryGroupPairs) {
            const sub = await createSubscription(request.telegramId, plan, ctx.from.id, {
              planCategory: item.category,
              premiumGroupId: item.groupId,
              session,
            });
            subscriptions.push({ ...item, subscription: sub });
          }
          await approveRequest(requestId, ctx.from.id, plan._id, {
            session,
            expectedStatuses: ['processing'],
            expectedProcessingToken: processingToken,
          });
        });
        usedTransaction = true;
      } catch (txErr) {
        if (!isTransactionUnsupportedError(txErr)) {
          throw txErr;
        }
        logger.warn(
          `approve flow: transactions unavailable, using sequential path for request ${requestId}: ${txErr.message}`
        );
      } finally {
        await session.endSession();
      }

      if (!usedTransaction) {
        subscriptions.length = 0;
        for (const item of categoryGroupPairs) {
          const sub = await createSubscription(request.telegramId, plan, ctx.from.id, {
            planCategory: item.category,
            premiumGroupId: item.groupId,
          });
          subscriptions.push({ ...item, subscription: sub });
        }
        await approveRequest(requestId, ctx.from.id, plan._id, {
          expectedStatuses: ['processing'],
          expectedProcessingToken: processingToken,
        });
      }

      const allRenewal = subscriptions.every((item) => item.subscription?.isRenewal);
      let appliedOffer = request?.appliedUserOffer || {};
      let appliedDiscountPercent = Number(appliedOffer.discountPercent || 0);

      if (appliedOffer?.offerId && appliedDiscountPercent > 0) {
        const [publicOfferDoc, privateOfferDoc] = await Promise.all([
          Offer.findById(appliedOffer.offerId).select('planId').lean().catch(() => null),
          UserOffer.findById(appliedOffer.offerId).select('_id planId').lean().catch(() => null),
        ]);

        // Plan-specific public offer should apply only when selected plan matches.
        if (publicOfferDoc) {
          const publicOfferPlanId = publicOfferDoc.planId ? String(publicOfferDoc.planId) : null;
          if (publicOfferPlanId && publicOfferPlanId !== String(plan._id)) {
            appliedOffer = {};
            appliedDiscountPercent = 0;
          }
        } else if (privateOfferDoc) {
          const privateOfferPlanId = privateOfferDoc.planId ? String(privateOfferDoc.planId) : null;
          if (privateOfferPlanId && privateOfferPlanId !== String(plan._id)) {
            // Offer was consumed earlier but selected plan does not match; restore it.
            await UserOffer.findByIdAndUpdate(privateOfferDoc._id, {
              $set: {
                isUsed: false,
                usedAt: null,
                usedByRequestId: null,
              },
            });
            await Request.findByIdAndUpdate(requestId, {
              appliedUserOffer: {
                offerId: null,
                title: null,
                discountPercent: 0,
              },
            });
            appliedOffer = {};
            appliedDiscountPercent = 0;
          }
        } else if (!privateOfferDoc) {
          // If the referenced offer no longer exists, ignore stale discount safely.
          appliedOffer = {};
          appliedDiscountPercent = 0;
        }
      }

      if (appliedDiscountPercent <= 0) {
        const consumedPlanSpecificPrivateOffer = await UserOffer.findOneAndUpdate(
          {
            targetTelegramId: request.telegramId,
            isActive: true,
            isUsed: false,
            validTill: { $gt: new Date() },
            planId: plan._id,
          },
          {
            $set: {
              isUsed: true,
              usedAt: new Date(),
              usedByRequestId: request._id,
            },
          },
          {
            sort: { createdAt: -1 },
            new: true,
          }
        );

        if (consumedPlanSpecificPrivateOffer) {
          appliedOffer = {
            offerId: consumedPlanSpecificPrivateOffer._id,
            title: consumedPlanSpecificPrivateOffer.title,
            discountPercent: Number(consumedPlanSpecificPrivateOffer.discountPercent || 0),
          };
          appliedDiscountPercent = Number(consumedPlanSpecificPrivateOffer.discountPercent || 0);
          await Request.findByIdAndUpdate(requestId, { appliedUserOffer: appliedOffer });
        }
      }

      if (appliedDiscountPercent <= 0) {
        const now = new Date();
        const [planSpecificPublicOffer, globalPublicOffer] = await Promise.all([
          Offer.findOne({
            isActive: true,
            validTill: { $gt: now },
            discountPercent: { $gt: 0 },
            planId: plan._id,
          })
            .sort({ discountPercent: -1, createdAt: -1 })
            .lean(),
          Offer.findOne({
            isActive: true,
            validTill: { $gt: now },
            discountPercent: { $gt: 0 },
            planId: null,
          })
            .sort({ discountPercent: -1, createdAt: -1 })
            .lean(),
        ]);

        const selectedPublicOffer = planSpecificPublicOffer || globalPublicOffer;
        if (selectedPublicOffer) {
          appliedOffer = {
            offerId: selectedPublicOffer._id,
            title: selectedPublicOffer.title,
            discountPercent: Number(selectedPublicOffer.discountPercent || 0),
          };
          appliedDiscountPercent = Number(selectedPublicOffer.discountPercent || 0);
          await Request.findByIdAndUpdate(requestId, { appliedUserOffer: appliedOffer });
        }
      }

      const payableAmount = appliedDiscountPercent > 0
        ? getDiscountedPrice(plan.price, appliedDiscountPercent)
        : Number(plan.price || 0);
      const maxExpiry = subscriptions
        .map((item) => item.subscription?.expiryDate)
        .filter(Boolean)
        .sort((left, right) => new Date(right).getTime() - new Date(left).getTime())[0];

      let userMessage;
      const extra = { parse_mode: 'Markdown' };
      const inviteButtons = [];
      const alreadyInGroupCategories = [];

      for (const item of subscriptions) {
        const alreadyInGroup = await isGroupMember(bot, item.groupId, request.telegramId);
        if (alreadyInGroup) {
          alreadyInGroupCategories.push(item.category);
          continue;
        }

        await revokeSubscriptionInviteLink(bot, item.subscription);
        await unbanFromGroup(bot, item.groupId, request.telegramId);
        const inviteLink = await generateInviteLink(
          bot, item.groupId, request.telegramId, item.subscription.expiryDate
        );

        if (inviteLink) {
          await Subscription.findByIdAndUpdate(item.subscription._id, {
            inviteLink,
            inviteLinkIssuedAt: new Date(),
            inviteLinkTtlMinutes: Math.max(1, parseInt(process.env.INVITE_LINK_TTL_MINUTES || '10', 10)),
          });
          inviteButtons.push({
            text: `🔗 Join ${getCategoryShortLabel(item.category)} Group`,
            url: inviteLink,
            style: 'success',
          });
        }
      }

      if (allRenewal && inviteButtons.length === 0) {
        userMessage =
          `🎉 *Subscription Renewed!*\n\n` +
          `📋 Plan: *${plan.name}*\n` +
          (Number(plan.price || 0) > 0 && appliedDiscountPercent > 0
            ? `💰 Price: ~₹${Number(plan.price || 0).toFixed(2)}~ → *₹${payableAmount.toFixed(2)}*\n`
            : '') +
          `➕ Extended by: *${plan.durationDays} days*\n` +
          `📅 New Expiry: *${formatDate(maxExpiry)}*\n\n` +
          (alreadyInGroupCategories.length > 0
            ? `✅ Aap already premium group me ho. Isliye naya invite link nahi bheja gaya.\n\n`
            : `Apka premium renew ho gaya hai. \n\n`) +
          `Thank you! 🙏`;

        if (alreadyInGroupCategories.length > 0) {
          extra.reply_markup = {
            inline_keyboard: [[{ text: '🎫 Support Chat', url: SUPPORT_CONTACT_URL, style: 'primary' }]],
          };
        }
      } else {
        if (inviteButtons.length > 0) {
          userMessage =
            `🎉 *Access Approved!*\n\n` +
            `📋 Plan: *${plan.name}*\n` +
            (Number(plan.price || 0) > 0 && appliedDiscountPercent > 0
              ? `💰 Price: ~₹${Number(plan.price || 0).toFixed(2)}~ → *₹${payableAmount.toFixed(2)}*\n`
              : '') +
            `📅 Valid for: *${plan.durationDays} days*\n` +
            `⏰ Expires on: *${formatDate(maxExpiry)}*\n\n` +
            `🔗 *Premium Group join kijiye niche diye gai button pe click karke*\n\n` +
            `⚠️ Yeh single-use link hai. Kripya iss link ko share na kare nahi to aap ban ho shakte hain.\n\n` +
            `YE link sirf ${Math.max(1, parseInt(process.env.INVITE_LINK_TTL_MINUTES || '10', 10))} min k liye valid hai.\n\n` +
            `Thank you for joining! 🙏\n\n` +
            `📌 Iss bot ko block nahi kijiyega nahi to aage aane waale offers miss ho jayenge.`;

          extra.reply_markup = {
            inline_keyboard: inviteButtons.map((button) => ([button])),
          };
        } else {
          userMessage =
            `🎉 *Access Approved!*\n\n` +
            `📋 Plan: *${plan.name}*\n` +
            (Number(plan.price || 0) > 0 && appliedDiscountPercent > 0
              ? `💰 Price: ~₹${Number(plan.price || 0).toFixed(2)}~ → *₹${payableAmount.toFixed(2)}*\n`
              : '') +
            `📅 Valid for: *${plan.durationDays} days*\n` +
            `⏰ Expires on: *${formatDate(maxExpiry)}*\n\n` +
            `✅ Aap already premium group me ho. Isliye naya invite link nahi bheja gaya.\n\n` +
            `Koi issue ho to support se contact karein.`;

          extra.reply_markup = {
            inline_keyboard: [[{ text: '🎫 Support Chat', url: SUPPORT_CONTACT_URL, style: 'primary' }]],
          };
        }
      }

      const shouldPinInviteMessage = inviteButtons.length > 0;

      await safeSendAndPin(bot, request.telegramId, userMessage, extra, { pin: shouldPinInviteMessage });
      await awardReferralBonus(bot, request.telegramId);
      await awardSellerCommission(bot, request.telegramId, payableAmount);

      await applyApprovalTransformToLogMessage(ctx, { planName: plan.name, allRenewal });

      await logToChannel(bot,
        `✅ *Subscription ${allRenewal ? 'Renewed' : 'Approved'}*\n` +
        `User: \`${request.telegramId}\`\n` +
        `Category: ${resolvedPlanCategory}\n` +
        `Group(s): ${subscriptions.map((item) => `\`${item.groupId}\``).join(', ')}\n` +
        (alreadyInGroupCategories.length > 0
          ? `Invite: skipped (already in premium group) for ${alreadyInGroupCategories.map((category) => getCategoryShortLabel(category)).join(', ')}\n`
          : '') +
        `Plan: ${plan.name} (${plan.durationDays}d)\n` +
        (Number(plan.price || 0) > 0 && appliedDiscountPercent > 0
          ? `Price: ₹${Number(plan.price || 0).toFixed(2)} -> ₹${payableAmount.toFixed(2)} (${appliedDiscountPercent}% OFF${appliedOffer?.title ? `, ${appliedOffer.title}` : ''})\n`
          : '') +
        `Expires: ${formatDate(maxExpiry)}\n` +
        `By: ${ctx.from.username ? '@' + ctx.from.username : ctx.from.id}`
      );

      await AdminLog.create({
        adminId: ctx.from.id,
        actionType: 'approve_request',
        targetUserId: request.telegramId,
        details: {
          plan: plan.name,
          durationDays: plan.durationDays,
          category: resolvedPlanCategory,
          premiumGroupIds: subscriptions.map((item) => item.groupId),
          atomicCategories: subscriptions.map((item) => item.category),
          isRenewal: allRenewal,
          planPrice: Number(plan.price || 0),
          payableAmount,
          appliedDiscountPercent,
          appliedOfferTitle: appliedOffer?.title || null,
        },
      });

    } catch (err) {
      if (acquiredProcessingLock && processingToken) {
        await Request.findOneAndUpdate(
          {
            _id: requestId,
            status: 'processing',
            processingToken,
          },
          {
            $set: {
              status: 'pending',
              processingError: String(err?.message || err).slice(0, 500),
            },
            $unset: {
              processingBy: '',
              processingAt: '',
              processingToken: '',
            },
          }
        ).catch(() => { });
      }
      logger.error(`approve error: ${err.message}`);
      await ctx.answerCbQuery('❌ Error processing', { show_alert: true });
    } finally {
      approvalRequestLocks.delete(requestId);
    }
  });

  // ── Reject request (step 1: choose reason) ───────────────────────────────
  const buildRejectReasonKeyboard = (requestId) => ({
    inline_keyboard: [
      [withStyle({ text: '🧾 Old Screenshot', callback_data: `reject_reason_${requestId}_old_ss` }, 'primary')],
      [withStyle({ text: '🚫 Fake Screenshot', callback_data: `reject_reason_${requestId}_fake_ss` }, 'danger')],
      [withStyle({ text: '📦 Wrong Plan selected', callback_data: `reject_reason_${requestId}_wrong_plan` }, 'primary')],
      [withStyle({ text: '📝 Others', callback_data: `reject_reason_${requestId}_others` }, 'primary')],
      [withStyle({ text: '⬅️ Back', callback_data: `reject_reason_back_${requestId}` }, 'success')],
    ],
  });

  const buildApprovalSelectionKeyboard = async (requestId, requestCategory = PLAN_CATEGORY.MOVIE) => {
    const normalizedCategory = normalizePlanCategory(requestCategory);
    const plans = await Plan.find({ isActive: true, category: normalizedCategory }).sort({ durationDays: 1 });
    const inlineKeyboard = [];

    for (let index = 0; index < plans.length; index += 2) {
      inlineKeyboard.push(
        plans.slice(index, index + 2).map((plan) => ({
          ...withStyle({
            text: `${plan.name} (${plan.durationDays}d${plan.price ? ` · ₹${plan.price}` : ''})`,
            callback_data: `approve_${requestId}_${plan._id}`,
          }, 'success'),
        }))
      );
    }

    inlineKeyboard.push([withStyle({ text: '❌ Reject', callback_data: `reject_${requestId}` }, 'danger')]);
    return { inline_keyboard: inlineKeyboard };
  };

  bot.action(/^reject_([a-f0-9]{24})$/i, async (ctx) => {
    await ctx.answerCbQuery('Select rejection reason');

    const [, requestId] = ctx.match;
    try {
      const adminUser = await User.findOne({ telegramId: ctx.from.id });
      if (!adminUser || !['admin', 'superadmin'].includes(adminUser.role)) return;

      const request = await Request.findById(requestId);
      if (!request || request.status !== 'pending') {
        return ctx.answerCbQuery('ℹ️ Already processed', { show_alert: true });
      }

      const msg = ctx.callbackQuery.message;
      const kb = buildRejectReasonKeyboard(requestId);
      const hasMedia = (Array.isArray(msg.photo) && msg.photo.length > 0) || Boolean(msg.document);

      if (hasMedia) {
        const cap = appendRejectReasonPromptOnce(msg.caption || '');
        try {
          await ctx.editMessageCaption(cap, { parse_mode: 'Markdown', reply_markup: kb });
        } catch (err) {
          logger.warn(`reject reason caption edit: ${err.message}`);
          await ctx.editMessageReplyMarkup(kb);
        }
      } else {
        try {
          await ctx.editMessageText(
            appendRejectReasonPromptOnce(msg.text || ''),
            { parse_mode: 'Markdown', reply_markup: kb }
          );
        } catch (err) {
          logger.warn(`reject reason text edit: ${err.message}`);
          await ctx.editMessageReplyMarkup(kb);
        }
      }
    } catch (err) {
      logger.error(`reject reason selector error: ${err.message}`);
    }
  });

  bot.action(/^reject_reason_back_([a-f0-9]{24})$/i, async (ctx) => {
    await ctx.answerCbQuery('Back');
    try {
      const [, requestId] = ctx.match;
      const adminUser = await User.findOne({ telegramId: ctx.from.id });
      if (!adminUser || !['admin', 'superadmin'].includes(adminUser.role)) return;

      const request = await Request.findById(requestId);
      if (!request || request.status !== 'pending') {
        return ctx.answerCbQuery('ℹ️ Already processed', { show_alert: true });
      }

      const msg = ctx.callbackQuery.message;
      const approvalKb = await buildApprovalSelectionKeyboard(requestId, request.requestCategory);
      const hasMedia = (Array.isArray(msg.photo) && msg.photo.length > 0) || Boolean(msg.document);

      if (hasMedia) {
        const cap = stripRejectReasonPromptFromText(msg.caption || '');
        await ctx.editMessageCaption(cap, { parse_mode: 'Markdown', reply_markup: approvalKb });
      } else {
        await ctx.editMessageText(
          stripRejectReasonPromptFromText(msg.text || ''),
          { parse_mode: 'Markdown', reply_markup: approvalKb }
        );
      }
    } catch (err) {
      logger.error(`reject reason back error: ${err.message}`);
      await ctx.answerCbQuery('❌ Unable to go back', { show_alert: true });
    }
  });

  // ── Reject request (step 2: apply selected reason) ───────────────────────
  bot.action(/^reject_reason_([a-f0-9]{24})_(old_ss|fake_ss|wrong_plan|others)$/i, async (ctx) => {
    await ctx.answerCbQuery('Rejecting...');
    try {
      const [, requestId, reasonCodeRaw] = ctx.match;
      const reasonCode = String(reasonCodeRaw || '').toLowerCase();
      const reasonLabel = REJECT_REASON_OPTIONS[reasonCode] || 'Reason not specified';

      const adminUser = await User.findOne({ telegramId: ctx.from.id });
      if (!adminUser || !['admin', 'superadmin'].includes(adminUser.role)) return;

      const request = await Request.findById(requestId);
      if (!request || request.status !== 'pending') {
        return ctx.answerCbQuery('ℹ️ Already processed', { show_alert: true });
      }

      await rejectRequest(requestId, ctx.from.id);

      await safeSend(
        bot,
        request.telegramId,
        `❌ *Request Rejected*\n\n` +
        `Reason: *${escapeMarkdown(reasonLabel)}*\n\n` +
        `If there is any issue, please contact support.`,
        {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [[
              withStyle({ text: '📩 Contact Support', url: SUPPORT_CONTACT_URL }, 'primary'),
            ]],
          },
        }
      );

      await applyRejectTransformToLogMessage(ctx, { reasonLabel });

      await logToChannel(
        bot,
        `❌ *Request Rejected*\nUser: \`${request.telegramId}\`\n` +
        `Reason: *${escapeMarkdown(reasonLabel)}*\n` +
        `By: ${ctx.from.username ? '@' + ctx.from.username : ctx.from.id}`
      );

      await AdminLog.create({
        adminId: ctx.from.id,
        actionType: 'reject_request',
        targetUserId: request.telegramId,
        details: {
          reasonCode,
          reasonLabel,
        },
      });
    } catch (err) {
      logger.error(`reject with reason error: ${err.message}`);
      await ctx.answerCbQuery('❌ Error processing', { show_alert: true });
    }
  });

  // ── /user <telegramId> — user search panel ─────────────────────────────────
  bot.command('user', requireAdmin, async (ctx) => {
    const parts = ctx.message.text.split(' ');
    if (parts.length < 2) return ctx.reply('Usage: /user <telegramId>');

    const targetId = parseInt(parts[1]);
    const user = await User.findOne({ telegramId: targetId });
    if (!user) return ctx.reply(`❌ User \`${targetId}\` not found.`, { parse_mode: 'Markdown' });

    const activeSubs = await Subscription.find({
      telegramId: targetId,
      status: 'active',
      expiryDate: { $gt: new Date() },
    }).sort({ expiryDate: 1 });
    const totalSubs = await Subscription.countDocuments({ telegramId: targetId });

    let msg = `👤 *User Profile*\n\n`;
    msg += `Name: *${escapeMarkdown(user.name)}*\n`;
    msg += `Username: ${user.username ? escapeMarkdown('@' + user.username) : 'N/A'}\n`;
    msg += `Telegram ID: \`${user.telegramId}\`\n`;
    msg += `Status: *${escapeMarkdown(user.status)}*\n`;
    msg += `Role: ${escapeMarkdown(user.role)}\n`;
    msg += `Joined: ${formatDate(user.joinDate)}\n`;
    msg += `Blocked: ${user.isBlocked ? '🚫 Yes' : '✅ No'}\n`;
    msg += `Total Subscriptions: *${totalSubs}*\n`;

    if (activeSubs.length) {
      msg += `\n📋 *Active Subscriptions:*\n`;
      for (let index = 0; index < activeSubs.length; index += 1) {
        const sub = activeSubs[index];
        const category = normalizePlanCategory(sub.planCategory || sub.planId?.category || 'movie');
        const groupId = getSubscriptionGroupId(sub);
        let inGroupStatus = 'Unknown';

        if (groupId) {
          try {
            const inGroup = await isGroupMember(bot, groupId, targetId);
            inGroupStatus = inGroup ? 'Yes' : 'No';
          } catch (_) {
            inGroupStatus = 'Unknown';
          }
        }

        msg += `${index + 1}. *${escapeMarkdown(category)}* — ${escapeMarkdown(sub.planName)}\n`;
        msg += `   Status: ${escapeMarkdown(sub.status)}\n`;
        msg += `   Joined Group: *${inGroupStatus}*\n`;
        msg += `   Expires: ${formatDate(sub.expiryDate)} (Days left: *${daysRemaining(sub.expiryDate)}*)\n`;
      }
    } else {
      msg += `\n❌ No active subscription\n`;
    }

    if (user.referredBy) msg += `\n🤝 Referred by: \`${user.referredBy}\`\n`;

    await replyMarkdownOrPlain(ctx, msg);
  });

  // ── /diagnose <telegramId> — incident diagnostics snapshot ─────────────────
  bot.command('diagnose', requireAdmin, async (ctx) => {
    const parts = String(ctx.message?.text || '').trim().split(/\s+/);
    if (parts.length < 2) return ctx.reply('Usage: /diagnose <telegramId>');

    const targetId = parseInt(parts[1], 10);
    if (!Number.isFinite(targetId)) {
      return ctx.reply('❌ Invalid telegramId. Usage: /diagnose <telegramId>');
    }

    try {
      const user = await User.findOne({ telegramId: targetId }).lean();
      if (!user) {
        return ctx.reply(`❌ User \`${targetId}\` not found.`, { parse_mode: 'Markdown' });
      }

      const now = new Date();
      const [activeSubs, pendingRequests, latestInviteSub] = await Promise.all([
        Subscription.find({
          telegramId: targetId,
          status: 'active',
          expiryDate: { $gt: now },
        }).sort({ expiryDate: 1 }).lean(),
        Request.find({
          telegramId: targetId,
          status: 'pending',
        }).sort({ createdAt: -1 }).limit(10).lean(),
        Subscription.findOne({
          telegramId: targetId,
          inviteLinkIssuedAt: { $ne: null },
        }).sort({ inviteLinkIssuedAt: -1 }).lean(),
      ]);

      const categories = [PLAN_CATEGORY.MOVIE, PLAN_CATEGORY.DESI, PLAN_CATEGORY.NON_DESI];
      let msg = `🩺 *Incident Diagnose*\n\n`;
      msg += `User: *${escapeMarkdown(user.name || 'N/A')}*\n`;
      msg += `ID: \`${targetId}\`\n`;
      msg += `Username: ${user.username ? escapeMarkdown('@' + user.username) : 'N/A'}\n`;
      msg += `Status: *${escapeMarkdown(user.status || 'unknown')}*\n\n`;

      msg += `🗺 *Category → Group Mapping*\n`;
      for (const category of categories) {
        const groupId = getGroupIdForCategory(category);
        const configuredEnvName = category === PLAN_CATEGORY.MOVIE
          ? 'MOVIE_PREMIUM_GROUP_ID'
          : (category === PLAN_CATEGORY.DESI ? 'DESI_PREMIUM_GROUP_ID' : 'NON_DESI_PREMIUM_GROUP_ID');
        const directConfigured = process.env[configuredEnvName];
        const viaFallback = !directConfigured && !!process.env.PREMIUM_GROUP_ID;
        let joined = 'n/a';
        if (groupId) {
          try {
            const inGroup = await isGroupMember(bot, groupId, targetId);
            joined = inGroup ? 'yes' : 'no';
          } catch (_) {
            joined = 'unknown';
          }
        }

        msg += `• *${escapeMarkdown(category)}* → \`${groupId || 'not_configured'}\``;
        if (viaFallback) msg += ` _(fallback: PREMIUM_GROUP_ID)_`;
        msg += ` | joined: *${escapeMarkdown(joined)}*\n`;
      }

      msg += `\n📦 *Active Subscriptions* (${activeSubs.length})\n`;
      if (!activeSubs.length) {
        msg += `_none_\n`;
      } else {
        activeSubs.forEach((sub, index) => {
          const category = normalizePlanCategory(sub.planCategory || sub.planId?.category || 'movie');
          const resolvedGroup = getSubscriptionGroupId(sub);
          msg += `${index + 1}. *${escapeMarkdown(category)}* — ${escapeMarkdown(sub.planName || 'Plan')}\n`;
          msg += `   SubID: \`${sub._id}\` | Group: \`${resolvedGroup || 'n/a'}\`\n`;
          msg += `   Expires: ${formatDate(sub.expiryDate)} (${daysRemaining(sub.expiryDate)}d left)\n`;
          if (sub.inviteLinkIssuedAt) {
            msg += `   Invite issued: ${formatDate(sub.inviteLinkIssuedAt)}\n`;
          }
        });
      }

      msg += `\n⏳ *Pending Requests* (${pendingRequests.length})\n`;
      if (!pendingRequests.length) {
        msg += `_none_\n`;
      } else {
        pendingRequests.forEach((req, index) => {
          const category = normalizePlanCategory(req.requestCategory || 'movie');
          msg += `${index + 1}. \`${req._id}\` — *${escapeMarkdown(category)}* — ${formatDate(req.createdAt || req.requestDate)}\n`;
        });
      }

      msg += `\n🔗 *Last Invite Issued*\n`;
      if (!latestInviteSub) {
        msg += `_none_\n`;
      } else {
        const category = normalizePlanCategory(latestInviteSub.planCategory || 'movie');
        msg += `Category: *${escapeMarkdown(category)}*\n`;
        msg += `Plan: *${escapeMarkdown(latestInviteSub.planName || 'Plan')}*\n`;
        msg += `Issued At: ${formatDate(latestInviteSub.inviteLinkIssuedAt)}\n`;
        msg += `TTL (min): *${Number(latestInviteSub.inviteLinkTtlMinutes || 0)}*\n`;
        msg += `Group used: \`${getSubscriptionGroupId(latestInviteSub) || 'n/a'}\``;
      }

      await ctx.reply(msg, { parse_mode: 'Markdown' });
    } catch (err) {
      logger.error(`diagnose command error: ${err.message}`);
      await ctx.reply('❌ Failed to generate diagnose report. Please try again.');
    }
  });

  // ── /history <telegramId> — subscription / purchase history ───────────────
  bot.command('history', requireAdmin, async (ctx) => {
    const parts = String(ctx.message?.text || '').trim().split(/\s+/);
    if (parts.length < 2) {
      return ctx.reply('Usage: `/history <telegramId>`', { parse_mode: 'Markdown' });
    }

    const targetId = parseInt(parts[1], 10);
    if (!Number.isFinite(targetId)) {
      return ctx.reply('Usage: `/history <telegramId>` — ID must be a number.', { parse_mode: 'Markdown' });
    }

    try {
      const targetUser = await User.findOne({ telegramId: targetId });
      if (!targetUser) {
        return ctx.reply(`❌ User \`${targetId}\` not found.`, { parse_mode: 'Markdown' });
      }

      const totalCount = await Subscription.countDocuments({ telegramId: targetId });
      const subs = await Subscription.find({ telegramId: targetId })
        .sort({ createdAt: -1 })
        .limit(40)
        .lean();

      const planIds = [...new Set(subs.map((s) => s.planId).filter(Boolean))];
      const plans = planIds.length
        ? await Plan.find({ _id: { $in: planIds } }).select('_id price').lean()
        : [];
      const priceByPlanId = new Map(plans.map((p) => [String(p._id), Number(p.price || 0)]));

      let header = `📜 *Purchase history*\n\n`;
      header += `User: *${escapeMarkdown(targetUser.name)}*\n`;
      header += `Telegram ID: \`${targetId}\`\n`;
      header += `Username: ${targetUser.username ? escapeMarkdown('@' + targetUser.username) : 'N/A'}\n`;
      header += `Total subscription records: *${totalCount}*`;
      if (totalCount > subs.length) {
        header += ` (showing latest *${subs.length}*)`;
      }
      header += `\n\n`;

      if (!subs.length) {
        return ctx.reply(`${header}_No subscription records yet._`, { parse_mode: 'Markdown' });
      }

      const blocks = subs.map((sub, i) => {
        const cat = getCategoryShortLabel(sub.planCategory);
        const price = sub.planId ? priceByPlanId.get(String(sub.planId)) : null;
        const priceLine = Number.isFinite(price) && price > 0 ? ` · Plan price: *₹${price}*` : '';
        const planLabel = escapeMarkdown(String(sub.planName || 'Plan').slice(0, 120));

        let b = `${i + 1}. *${planLabel}* (${escapeMarkdown(cat)})${priceLine}\n`;
        b += `   Sub: \`${sub._id}\` · *${escapeMarkdown(sub.status)}*`;
        if (sub.isRenewal) b += ` · 🔄 Renewal`;
        b += `\n`;
        b += `   ${sub.durationDays}d · Start ${formatDate(sub.startDate)} · Expires ${formatDate(sub.expiryDate)}\n`;
        if (sub.approvedBy) b += `   Approved by: \`${sub.approvedBy}\`\n`;
        b += `   Logged: ${formatDate(sub.createdAt)}\n\n`;
        return b;
      });

      const maxChunk = 3800;
      let chunk = header;
      const continuedHeader = `📜 *Purchase history* _(continued)_ — \`${targetId}\`\n\n`;

      for (const block of blocks) {
        if (chunk.length + block.length > maxChunk) {
          await ctx.reply(chunk, { parse_mode: 'Markdown' });
          chunk = continuedHeader + block;
        } else {
          chunk += block;
        }
      }
      await ctx.reply(chunk, { parse_mode: 'Markdown' });
    } catch (err) {
      logger.error(`history command error: ${err.message}`);
      await ctx.reply(`❌ Failed to load history: ${escapeMarkdown(err.message)}`, { parse_mode: 'Markdown' });
    }
  });

  // ── /legacyadd <planIdOrDays>|<DD/MM/YYYY>|<id1,id2,...> ─────────────────
  bot.command('legacyadd', requireAdmin, async (ctx) => {
    try {
      if (!getSuperAdminIds().includes(ctx.from.id)) {
        return ctx.reply('⛔ Super Admin access required for /legacyadd.');
      }

      const raw = String(ctx.message?.text || '').replace('/legacyadd', '').trim();
      const [planPart, datePart, idsPart] = raw.split('|').map(s => s.trim());

      if (!planPart || !datePart || !idsPart) {
        return ctx.reply(
          'Usage: `/legacyadd <planIdOrDays>|<DD/MM/YYYY>|<id1,id2,id3,...>`',
          { parse_mode: 'Markdown' }
        );
      }

      let plan = await Plan.findById(planPart).catch(() => null);
      if (!plan) {
        const days = parseInt(planPart, 10);
        if (!days) {
          return ctx.reply('❌ Invalid plan value. Use planId or duration in days.');
        }
        plan = await Plan.findOne({ durationDays: days, isActive: true });
        if (!plan) {
          plan = await Plan.create({ name: `${days} Days Plan`, durationDays: days, price: 0 });
        }
      }

      const [d, m, y] = String(datePart).split('/').map(v => parseInt(v, 10));
      const expiryDate = new Date(y, (m || 1) - 1, d || 1, 23, 59, 59, 999);
      if (!d || !m || !y || Number.isNaN(expiryDate.getTime())) {
        return ctx.reply('❌ Invalid date format. Use DD/MM/YYYY.');
      }
      if (expiryDate <= new Date()) {
        return ctx.reply('❌ Expiry date must be in the future.');
      }

      const ids = [...new Set(
        idsPart
          .split(',')
          .map(s => parseInt(s.trim(), 10))
          .filter(Boolean)
      )];

      if (!ids.length) {
        return ctx.reply('❌ No valid user IDs found.');
      }

      if (ids.length > 200) {
        return ctx.reply('❌ Max 200 users per command. Please split into smaller batches.');
      }

      const startDate = new Date(expiryDate.getTime() - (plan.durationDays * 24 * 60 * 60 * 1000));
      const legacyCategory = normalizePlanCategory(plan.category || 'movie');
      const legacyGroupId = getGroupIdForCategory(legacyCategory);
      if (!legacyGroupId) {
        return ctx.reply(`❌ Premium group not configured for category: ${legacyCategory}`);
      }

      let imported = 0;
      let updated = 0;
      let skippedNotInGroup = 0;
      let skippedInvalid = 0;
      const failedIds = [];

      for (const telegramId of ids) {
        try {
          const inGroup = await isGroupMember(bot, legacyGroupId, telegramId);
          if (!inGroup) {
            skippedNotInGroup++;
            continue;
          }

          let user = await User.findOne({ telegramId });
          if (!user) {
            user = await User.create({
              telegramId,
              name: `Legacy User ${telegramId}`,
              username: null,
              role: 'user',
              status: 'active',
            });
          }

          const existingSub = await Subscription.findOne({
            telegramId,
            status: 'active',
          }).sort({ createdAt: -1 });

          if (existingSub) {
            existingSub.planId = plan._id;
            existingSub.planName = plan.name;
            existingSub.planCategory = legacyCategory;
            existingSub.premiumGroupId = legacyGroupId;
            existingSub.durationDays = plan.durationDays;
            existingSub.startDate = startDate;
            existingSub.expiryDate = expiryDate;
            existingSub.status = 'active';
            existingSub.approvedBy = ctx.from.id;
            existingSub.isRenewal = false;
            existingSub.reminderFlags = { day7: false, day3: false, day1: false, day0: false };
            await existingSub.save();
            updated++;
          } else {
            await Subscription.create({
              userId: user._id,
              telegramId,
              planId: plan._id,
              planName: plan.name,
              planCategory: legacyCategory,
              premiumGroupId: legacyGroupId,
              durationDays: plan.durationDays,
              startDate,
              expiryDate,
              status: 'active',
              approvedBy: ctx.from.id,
              isRenewal: false,
            });
            imported++;
          }

          await User.findOneAndUpdate(
            { telegramId },
            { status: 'active', isBlocked: false, lastInteraction: new Date() }
          );
        } catch (e) {
          skippedInvalid++;
          failedIds.push(telegramId);
        }
      }

      await AdminLog.create({
        adminId: ctx.from.id,
        actionType: 'legacy_import',
        details: {
          plan: plan.name,
          durationDays: plan.durationDays,
          expiryDate,
          totalInput: ids.length,
          imported,
          updated,
          skippedNotInGroup,
          skippedInvalid,
          failedIds: failedIds.slice(0, 20),
        },
      });

      await ctx.reply(
        `✅ *Legacy Import Complete*\n\n` +
        `📥 Total IDs: *${ids.length}*\n` +
        `🆕 Imported: *${imported}*\n` +
        `♻️ Updated: *${updated}*\n` +
        `⛔ Not in group: *${skippedNotInGroup}*\n` +
        `⚠️ Failed: *${skippedInvalid}*\n\n` +
        `Plan: *${plan.name}* (${plan.durationDays} days)\n` +
        `Expiry: *${formatDate(expiryDate)}*`,
        { parse_mode: 'Markdown' }
      );
    } catch (err) {
      logger.error(`legacyadd command error: ${err.message}`);
      await ctx.reply('❌ Failed legacy import. Please check command format and try again.');
    }
  });

  // ── /revokeplan <telegramId> [category] — terminate specific plan ─────────
  bot.command('revokeplan', requireAdmin, async (ctx) => {
    try {
      const parts = String(ctx.message?.text || '').trim().split(/\s+/);
      if (parts.length < 2) {
        return ctx.reply('Usage: /revokeplan <telegramId> [movie|desi|non_desi]');
      }

      const targetId = parseInt(parts[1], 10);
      const categoryInput = parts[2] || null;
      if (!targetId) {
        return ctx.reply('❌ Invalid telegramId. Usage: /revokeplan <telegramId> [category]');
      }

      const targetUser = await User.findOne({ telegramId: targetId });
      if (!targetUser) {
        return ctx.reply(`❌ User \`${targetId}\` not found.`, { parse_mode: 'Markdown' });
      }

      const resolved = await resolveSubscriptionForAdminAction(targetId, categoryInput);
      if (resolved.error === 'none') {
        return ctx.reply('ℹ️ No active subscription found for this user.');
      }
      if (resolved.error === 'invalid_category') {
        return ctx.reply('❌ Invalid category. Use movie, desi, or non_desi.');
      }
      if (resolved.error === 'category_not_found') {
        return ctx.reply(
          `❌ No active subscription found in category *${resolved.normalizedCategory}*.`,
          { parse_mode: 'Markdown' }
        );
      }
      if (resolved.error === 'ambiguous') {
        return ctx.reply(
          `⚠️ Multiple active subscriptions found. Please pass category.\n\n` +
          `Usage: /revokeplan <telegramId> [category]\n\n` +
          `${formatSubscriptionCategoryList(resolved.subscriptions)}`
        );
      }

      const activeSub = resolved.subscription;
      const resolvedCategory = normalizePlanCategory(activeSub.planCategory || activeSub.planId?.category || 'movie');
      await promptAdminButtonConfirmation(ctx, {
        actionType: 'revokeplan',
        payload: { targetId, categoryInput },
        summaryMarkdown:
          `This will revoke *${escapeMarkdown(activeSub.planName)}* (${escapeMarkdown(resolvedCategory)}) for user \`${targetId}\` and remove group access.`,
      });
      return;
    } catch (err) {
      logger.error(`revokeplan command error: ${err.message}`);
      await ctx.reply('❌ Failed to revoke plan. Please try again.');
    }
  });

  // ── /modifyplan <telegramId>|<planIdOrDays>|[category]|[reset] — correct plan ───
  // Default: keeps existing expiry (metadata / wrong-plan correction). Optional 4th segment: reset | fromtoday — full renewal from now using plan duration.
  bot.command('modifyplan', requireAdmin, async (ctx) => {
    try {
      const fullText = String(ctx.message?.text || '');
      const raw = fullText.replace(/^\/modifyplan(@\w+)?\s*/i, '').trim();
      const segments = raw.split('|').map((s) => s.trim());
      let categoryPart = segments[2] || null;
      let modePartRaw = segments[3];
      const isModeToken = (t) => ['reset', 'fromtoday'].includes(String(t || '').toLowerCase());
      if (isModeToken(categoryPart) && !segments[3]) {
        modePartRaw = categoryPart;
        categoryPart = null;
      }
      const [idPart, planPart] = segments;
      const modePart = String(modePartRaw || '').toLowerCase();
      const resetExpiryFromToday = modePart === 'reset' || modePart === 'fromtoday';

      if (!idPart || !planPart) {
        return ctx.reply(
          'Usage: `/modifyplan <telegramId>|<planIdOrDays>|[movie|desi|non_desi]|[reset]`\n\n' +
          '• *Default:* keeps current expiry (correct plan label/id only).\n' +
          '• *Last part `reset`*: new expiry = today + plan duration.\n' +
          '• Plan must match subscription category (no cross-category moves here).',
          { parse_mode: 'Markdown' }
        );
      }

      const targetId = parseInt(idPart, 10);
      if (!targetId) {
        return ctx.reply('❌ Invalid telegramId.');
      }

      const targetUser = await User.findOne({ telegramId: targetId });
      if (!targetUser) {
        return ctx.reply(`❌ User \`${targetId}\` not found.`, { parse_mode: 'Markdown' });
      }

      const resolved = await resolveSubscriptionForAdminAction(targetId, categoryPart || null);
      if (resolved.error === 'none') {
        return ctx.reply('ℹ️ No active subscription found for this user to modify.');
      }
      if (resolved.error === 'invalid_category') {
        return ctx.reply('❌ Invalid category. Use movie, desi, or non_desi.');
      }
      if (resolved.error === 'category_not_found') {
        return ctx.reply(
          `❌ No active subscription found in category *${resolved.normalizedCategory}* for modify.`,
          { parse_mode: 'Markdown' }
        );
      }
      if (resolved.error === 'ambiguous') {
        return ctx.reply(
          `⚠️ Multiple active subscriptions found. Please pass category in 3rd argument.\n\n` +
          `Usage: /modifyplan <telegramId>|<planIdOrDays>|[category]|[reset]\n\n` +
          `${formatSubscriptionCategoryList(resolved.subscriptions)}`
        );
      }

      const targetCategory = resolved.normalizedCategory;

      let plan = await Plan.findById(planPart).catch(() => null);
      if (!plan) {
        const daysToken = String(planPart || '').trim();
        if (!/^\d+$/.test(daysToken)) {
          return ctx.reply('❌ Invalid plan ID. Use `/plans` for Mongo `_id`, or a numeric duration in days for that category.', { parse_mode: 'Markdown' });
        }
        const days = parseInt(daysToken, 10);
        if (!days) {
          return ctx.reply('❌ Invalid plan value. Use planId or duration in days.');
        }
        plan = await Plan.findOne({
          durationDays: days,
          isActive: true,
          category: targetCategory,
        });
      }

      if (!plan) {
        return ctx.reply('❌ Plan not found. Use `/plans` to see active plans.', { parse_mode: 'Markdown' });
      }

      const planCategory = normalizePlanCategory(plan.category || 'movie');
      if (planCategory !== targetCategory) {
        return ctx.reply(
          `❌ Plan category mismatch.\n` +
          `You are editing *${escapeMarkdown(targetCategory)}* subscription but selected plan is *${escapeMarkdown(planCategory)}*.\n\n` +
          `Pick a plan in the same category, or use a different tool to move users between categories.`,
          { parse_mode: 'Markdown' }
        );
      }

      const activeSub = resolved.subscription;

      const previousPlan = activeSub.planName;
      const now = new Date();

      let newExpiry;
      if (resetExpiryFromToday) {
        newExpiry = new Date(now.getTime() + (plan.durationDays * 24 * 60 * 60 * 1000));
      } else {
        newExpiry = activeSub.expiryDate ? new Date(activeSub.expiryDate) : null;
        if (!newExpiry || newExpiry.getTime() <= now.getTime()) {
          return ctx.reply(
            'ℹ️ This subscription is already expired or has no valid expiry.\n' +
            'Use the same command with `|reset` last to start a fresh period from today.'
          );
        }
      }

      const newPlanCategory = planCategory;
      const newGroupId = getGroupIdForCategory(newPlanCategory);
      if (!newGroupId) {
        return ctx.reply(`❌ Premium group not configured for category: ${newPlanCategory}`);
      }

      const expirySummary = resetExpiryFromToday
        ? `New expiry: *${escapeMarkdown(formatDate(newExpiry))}* (from today + plan days)`
        : `Keeps expiry: *${escapeMarkdown(formatDate(newExpiry))}*`;
      await promptAdminButtonConfirmation(ctx, {
        actionType: 'modifyplan',
        payload: {
          targetId,
          planId: String(plan._id),
          subscriptionId: String(activeSub._id),
          resetExpiryFromToday,
        },
        summaryMarkdown:
          `Modify plan for user \`${targetId}\`:\n` +
          `*${escapeMarkdown(previousPlan)}* → *${escapeMarkdown(plan.name)}* (${escapeMarkdown(targetCategory)})\n` +
          `${expirySummary}`,
      });
      return;
    } catch (err) {
      logger.error(`modifyplan command error: ${err.message}`);
      await ctx.reply('❌ Failed to modify plan. Please try again.');
    }
  });

  // ── /invite <telegramId> [category] — resend category-wise invite ─────────
  bot.command('invite', requireAdmin, async (ctx) => {
    try {
      const parts = String(ctx.message?.text || '').trim().split(/\s+/);
      if (parts.length < 2) {
        return ctx.reply('Usage: /invite <telegramId> [movie|desi|non_desi]');
      }

      const targetId = parseInt(parts[1], 10);
      const categoryInput = parts[2] || null;
      if (!targetId) {
        return ctx.reply('❌ Invalid telegramId. Usage: /invite <telegramId> [category]');
      }

      const targetUser = await User.findOne({ telegramId: targetId });
      if (!targetUser) {
        return ctx.reply(`❌ User \`${targetId}\` not found.`, { parse_mode: 'Markdown' });
      }

      // Nullify any previous pending request so user can raise a fresh one if needed.
      const pendingResult = await Request.updateMany(
        { telegramId: targetId, status: 'pending' },
        { status: 'rejected', actionDate: new Date(), actionBy: ctx.from.id }
      );

      const resolved = await resolveSubscriptionForAdminAction(targetId, categoryInput);
      if (resolved.error === 'invalid_category') {
        return ctx.reply('❌ Invalid category. Use movie, desi, or non_desi.');
      }
      if (resolved.error === 'category_not_found') {
        return ctx.reply(
          `❌ No active subscription found in category *${resolved.normalizedCategory}* for invite.`,
          { parse_mode: 'Markdown' }
        );
      }
      if (resolved.error === 'ambiguous') {
        return ctx.reply(
          `⚠️ Multiple active subscriptions found. Please pass category.\n\n` +
          `Usage: /invite <telegramId> [category]\n\n` +
          `${formatSubscriptionCategoryList(resolved.subscriptions)}`
        );
      }

      const activeSub = resolved.subscription;
      if (!activeSub || activeSub.expiryDate <= new Date()) {
        await User.findOneAndUpdate(
          { telegramId: targetId },
          { status: 'inactive', lastInteraction: new Date() }
        );

        await safeSend(
          bot,
          targetId,
          `ℹ️ *Please raise a new joining request.*\n\n` +
          `Aapka pehle ka pending request reset kar diya gaya hai.\n` +
          `Kripya /start karke *Premium Access Request* dubara bhejein.`,
          { parse_mode: 'Markdown' }
        );

        await AdminLog.create({
          adminId: ctx.from.id,
          actionType: 'resend_invite',
          targetUserId: targetId,
          details: {
            result: 'no_active_subscription',
            nullifiedPendingRequests: pendingResult?.modifiedCount || 0,
          },
        });

        return ctx.reply(
          `✅ Pending request(s) reset for \`${targetId}\`.\nNo active subscription found, user asked to raise a new request.`,
          { parse_mode: 'Markdown' }
        );
      }

      const inviteGroupId = getSubscriptionGroupId(activeSub);
      if (!inviteGroupId) {
        return ctx.reply('❌ Premium group mapping missing for this user subscription.');
      }

      const alreadyInGroup = await isGroupMember(bot, inviteGroupId, targetId);
      if (alreadyInGroup) {
        await safeSend(
          bot,
          targetId,
          `✅ *Aap already premium group me ho.*\n\n` +
          `Isliye naya invite link generate nahi kiya gaya.\n` +
          `Koi issue ho to support se contact karein.`,
          {
            parse_mode: 'Markdown',
            reply_markup: {
              inline_keyboard: [[{ text: '🎫 Support Chat', url: SUPPORT_CONTACT_URL, style: 'primary' }]],
            },
          }
        );

        await AdminLog.create({
          adminId: ctx.from.id,
          actionType: 'resend_invite',
          targetUserId: targetId,
          details: {
            subscriptionId: activeSub._id,
            category: normalizePlanCategory(activeSub.planCategory || activeSub.planId?.category || 'movie'),
            plan: activeSub.planName,
            expiryDate: activeSub.expiryDate,
            result: 'already_in_group_no_invite',
            nullifiedPendingRequests: pendingResult?.modifiedCount || 0,
          },
        });

        return ctx.reply(
          `ℹ️ User \`${targetId}\` is already in premium group. Invite link not sent.`,
          { parse_mode: 'Markdown' }
        );
      }

      await unbanFromGroup(bot, inviteGroupId, targetId);
      await revokeSubscriptionInviteLink(bot, activeSub);
      const inviteLink = await generateInviteLink(bot, inviteGroupId, targetId, activeSub.expiryDate);
      if (!inviteLink) {
        return ctx.reply('❌ Failed to generate a new invite link. Check bot group admin permissions.');
      }

      await Subscription.findByIdAndUpdate(activeSub._id, {
        inviteLink,
        inviteLinkIssuedAt: new Date(),
        inviteLinkTtlMinutes: Math.max(1, parseInt(process.env.INVITE_LINK_TTL_MINUTES || '10', 10)),
      });

      await safeSend(
        bot,
        targetId,
        `🔗 *New Invite Link Generated*\n\n` +
        `Aapka naya joining link ready hai. Niche button pe click karke group join karein.\n\n` +
        `⏰ Link limited-time aur single-use hai.`,
        {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [[{ text: '🔗 Join Premium Group', url: inviteLink, style: 'success' }]],
          },
        }
      );

      await AdminLog.create({
        adminId: ctx.from.id,
        actionType: 'resend_invite',
        targetUserId: targetId,
        details: {
          subscriptionId: activeSub._id,
          category: normalizePlanCategory(activeSub.planCategory || activeSub.planId?.category || 'movie'),
          plan: activeSub.planName,
          expiryDate: activeSub.expiryDate,
          nullifiedPendingRequests: pendingResult?.modifiedCount || 0,
        },
      });

      const now = new Date();
      await logToChannel(
        bot,
        `✅ *New Invite Link issued:*\n` +
        `For User Id: \`${targetId}\`\n` +
        `By Admin Id: \`${ctx.from.id}\`\n` +
        `Date: ${now.toLocaleDateString('en-GB')}\n` +
        `Time: ${now.toLocaleTimeString('en-IN')}`
      );

      await ctx.reply(
        `✅ New invite link sent to user \`${targetId}\`.\nCategory: *${normalizePlanCategory(activeSub.planCategory || activeSub.planId?.category || 'movie')}*\nPlan: *${activeSub.planName}*\nExpires: *${formatDate(activeSub.expiryDate)}*`,
        { parse_mode: 'Markdown' }
      );
    } catch (err) {
      logger.error(`invite command error: ${err.message}`);
      await ctx.reply('❌ Failed to resend invite. Please try again.');
    }
  });

  // ── /repair <telegramId> — heal user request/invite/subscription state ────
  bot.command('repair', requireAdmin, async (ctx) => {
    try {
      const parts = String(ctx.message?.text || '').trim().split(/\s+/);
      if (parts.length < 2) return ctx.reply('Usage: /repair <telegramId>');

      const targetId = parseInt(parts[1], 10);
      if (!targetId) return ctx.reply('❌ Invalid telegramId. Usage: /repair <telegramId>');

      const targetUser = await User.findOne({ telegramId: targetId });
      if (!targetUser) return ctx.reply(`❌ User \`${targetId}\` not found.`, { parse_mode: 'Markdown' });

      const now = new Date();
      const pendingCutoff = new Date(now.getTime() - 6 * 60 * 60 * 1000);

      const resetRequests = await Request.updateMany(
        {
          telegramId: targetId,
          status: { $in: ['pending', 'processing'] },
          requestDate: { $lt: pendingCutoff },
        },
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

      const activeSubs = await Subscription.find({
        telegramId: targetId,
        status: 'active',
        expiryDate: { $gt: now },
      });

      let staleInviteMetaCleared = 0;
      let freshInvitesIssued = 0;
      const inviteButtons = [];

      for (const sub of activeSubs) {
        const groupId = getSubscriptionGroupId(sub);
        const ttlMinutes = Math.max(1, parseInt(sub.inviteLinkTtlMinutes || process.env.INVITE_LINK_TTL_MINUTES || '10', 10));
        const inviteExpired = sub.inviteLinkIssuedAt
          ? new Date(sub.inviteLinkIssuedAt).getTime() + ttlMinutes * 60 * 1000 < now.getTime()
          : false;

        if (inviteExpired || sub.status !== 'active') {
          await Subscription.findByIdAndUpdate(sub._id, {
            inviteLink: null,
            inviteLinkIssuedAt: null,
            inviteLinkTtlMinutes: null,
          });
          staleInviteMetaCleared += 1;
        }

        if (!groupId) continue;
        const alreadyInGroup = await isGroupMember(bot, groupId, targetId);
        if (alreadyInGroup) continue;

        await revokeSubscriptionInviteLink(bot, sub);
        await unbanFromGroup(bot, groupId, targetId);
        const inviteLink = await generateInviteLink(bot, groupId, targetId, sub.expiryDate);
        if (!inviteLink) continue;

        await Subscription.findByIdAndUpdate(sub._id, {
          inviteLink,
          inviteLinkIssuedAt: new Date(),
          inviteLinkTtlMinutes: Math.max(1, parseInt(process.env.INVITE_LINK_TTL_MINUTES || '10', 10)),
        });

        freshInvitesIssued += 1;
        inviteButtons.push([{ text: `🔗 Join ${getCategoryShortLabel(sub.planCategory || 'movie')}`, url: inviteLink }]);
      }

      await syncUserStatusFromSubscriptions(targetId).catch(() => { });
      const updatedUser = await User.findOne({ telegramId: targetId }).select('status').lean();

      await AdminLog.create({
        adminId: ctx.from.id,
        actionType: 'resend_invite',
        targetUserId: targetId,
        details: {
          command: 'repair',
          resetRequestsCount: Number(resetRequests?.modifiedCount || 0),
          staleInviteMetaCleared,
          freshInvitesIssued,
          activeSubscriptions: activeSubs.length,
          finalStatus: updatedUser?.status || 'unknown',
        },
      });

      await ctx.reply(
        `🛠 *Repair complete* for \`${targetId}\`\n\n` +
        `Requests reset (stale pending/processing): *${Number(resetRequests?.modifiedCount || 0)}*\n` +
        `Active subscriptions: *${activeSubs.length}*\n` +
        `Stale invite metadata cleared: *${staleInviteMetaCleared}*\n` +
        `Fresh invites issued: *${freshInvitesIssued}*\n` +
        `User status now: *${escapeMarkdown(updatedUser?.status || 'unknown')}*`,
        { parse_mode: 'Markdown' }
      );

      if (inviteButtons.length) {
        await safeSend(
          bot,
          targetId,
          `✅ *Support repair completed by admin.*\n\nNiche apke fresh join links diye gaye hain:`,
          {
            parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: inviteButtons },
          }
        );
      }
    } catch (err) {
      logger.error(`repair command error: ${err.message}`);
      await ctx.reply('❌ Failed to repair user state. Please try again.');
    }
  });

  // ── /offeruser <id>|<discount>|[planId] ───────────────────────────────────
  bot.command('offeruser', requireAdmin, async (ctx) => {
    try {
      const raw = String(ctx.message?.text || '').replace('/offeruser', '').trim();
      const [idPart, discountPart, planIdPartRaw] = raw.split('|').map(s => s.trim());

      if (!idPart || !discountPart) {
        return ctx.reply(
          'Usage: `/offeruser <telegramId>|<discountPercent>|[planId]`',
          { parse_mode: 'Markdown' }
        );
      }

      const targetId = parseInt(idPart, 10);
      if (!targetId) {
        return ctx.reply('❌ Invalid telegramId.');
      }

      const targetUser = await User.findOne({ telegramId: targetId });
      if (!targetUser) {
        return ctx.reply(`❌ User \`${targetId}\` not found.`, { parse_mode: 'Markdown' });
      }

      const discount = parseInt(discountPart, 10);
      if (Number.isNaN(discount) || discount < 0 || discount > 100) {
        return ctx.reply('❌ discountPercent must be between 0 and 100.');
      }

      let scopedPlan = null;
      const planIdPart = String(planIdPartRaw || '').trim();
      if (planIdPart) {
        scopedPlan = await Plan.findById(planIdPart).lean();
        if (!scopedPlan) {
          return ctx.reply(`❌ Plan not found: \`${planIdPart}\``, { parse_mode: 'Markdown' });
        }
      }

      const validTill = new Date();
      validTill.setHours(23, 59, 59, 999);

      const defaultTitle = 'Special Discount';
      const defaultDescription = 'Only for you!';

      // Keep only one active one-time private offer per user: remove all previous active offers.
      await UserOffer.deleteMany(
        {
          targetTelegramId: targetId,
          isActive: true,
          isUsed: false,
        }
      );

      const offer = await UserOffer.create({
        targetTelegramId: targetId,
        planId: scopedPlan?._id || null,
        title: defaultTitle,
        description: defaultDescription,
        discountPercent: discount,
        validTill,
        createdBy: ctx.from.id,
      });

      await AdminLog.create({
        adminId: ctx.from.id,
        actionType: 'create_offer',
        targetUserId: targetId,
        details: {
          userOfferId: offer._id,
          userSpecific: true,
          planId: scopedPlan?._id || null,
          planName: scopedPlan?.name || null,
          title: offer.title,
          discountPercent: offer.discountPercent,
          validTill: offer.validTill,
        },
      });

      await safeSend(
        bot,
        targetId,
        `🎁 *Private Offer Received!*\n\n` +
        `*Special Discount*\n` +
        `Only for you!\n` +
        (scopedPlan?.name ? `📦 Plan: *${escapeMarkdown(scopedPlan.name)}*\n` : '') +
        `${offer.discountPercent > 0 ? `💰 Discount: *${offer.discountPercent}%*\n` : ''}` +
        `⏰ Valid till: *Today only*\n\n` +
        `Ye offer sirf aapke liye hai aur next request/renewal par ek hi baar apply hoga${scopedPlan?.name ? ' (selected plan ke liye)' : ''}.`,
        { parse_mode: 'Markdown' }
      );

      await ctx.reply(
        `✅ One-time private offer created for \`${targetId}\`.\n` +
        (scopedPlan?.name ? `Plan: *${escapeMarkdown(scopedPlan.name)}*\n` : 'Plan: *All plans*\n') +
        `Offer ID: \`${offer._id}\``,
        { parse_mode: 'Markdown' }
      );
    } catch (err) {
      logger.error(`offeruser command error: ${err.message}`);
      await ctx.reply('❌ Failed to create private offer. Please check format and try again.');
    }
  });

  // ── /ban <telegramId> — block user from using bot ─────────────────────────
  bot.command('ban', requireAdmin, async (ctx) => {
    try {
      const parts = String(ctx.message?.text || '').trim().split(/\s+/);
      if (parts.length < 2) {
        return ctx.reply('Usage: /ban <telegramId>');
      }

      const targetId = parseInt(parts[1], 10);
      if (!targetId) {
        return ctx.reply('❌ Invalid telegramId. Usage: /ban <telegramId>');
      }

      if (targetId === ctx.from.id) {
        return ctx.reply('❌ You cannot ban yourself.');
      }

      const targetUser = await User.findOne({ telegramId: targetId });
      if (!targetUser) {
        return ctx.reply(`❌ User \`${targetId}\` not found.`, { parse_mode: 'Markdown' });
      }

      if (['admin', 'superadmin'].includes(targetUser.role)) {
        return ctx.reply('⛔ You cannot ban an admin/superadmin user.');
      }

      await User.findOneAndUpdate(
        { telegramId: targetId },
        { isBlocked: true, status: 'blocked', lastInteraction: new Date() }
      );

      const groupIds = getAllPremiumGroupIds();
      for (const groupId of groupIds) {
        await banFromGroup(bot, groupId, targetId);
      }

      await safeSend(
        bot,
        targetId,
        `⛔ *You have been banned from using this bot.*\n\n` +
        `Please contact support for this issue: /support`,
        { parse_mode: 'Markdown' }
      );

      await AdminLog.create({
        adminId: ctx.from.id,
        actionType: 'ban_user',
        targetUserId: targetId,
        details: { reason: 'Manual ban via /ban command' },
      });

      await ctx.reply(`✅ User \`${targetId}\` has been banned from bot usage.`, { parse_mode: 'Markdown' });
    } catch (err) {
      logger.error(`ban command error: ${err.message}`);
      await ctx.reply('❌ Failed to ban user. Please try again.');
    }
  });

  // ── /unban <telegramId> — restore user bot access ────────────────────────
  bot.command('unban', requireAdmin, async (ctx) => {
    try {
      const parts = String(ctx.message?.text || '').trim().split(/\s+/);
      if (parts.length < 2) {
        return ctx.reply('Usage: /unban <telegramId>');
      }

      const targetId = parseInt(parts[1], 10);
      if (!targetId) {
        return ctx.reply('❌ Invalid telegramId. Usage: /unban <telegramId>');
      }

      const targetUser = await User.findOne({ telegramId: targetId });
      if (!targetUser) {
        return ctx.reply(`❌ User \`${targetId}\` not found.`, { parse_mode: 'Markdown' });
      }

      await User.findOneAndUpdate(
        { telegramId: targetId },
        { isBlocked: false, status: 'active', lastInteraction: new Date() }
      );

      const groupIds = getAllPremiumGroupIds();
      for (const groupId of groupIds) {
        await unbanFromGroup(bot, groupId, targetId);
      }

      await safeSend(
        bot,
        targetId,
        `✅ *Your access has been restored.*\n\nYou can now use the bot again.`,
        { parse_mode: 'Markdown' }
      );

      await AdminLog.create({
        adminId: ctx.from.id,
        actionType: 'unban_user',
        targetUserId: targetId,
        details: { reason: 'Manual unban via /unban command' },
      });

      await ctx.reply(`✅ User \`${targetId}\` has been unbanned.`, { parse_mode: 'Markdown' });
    } catch (err) {
      logger.error(`unban command error: ${err.message}`);
      await ctx.reply('❌ Failed to unban user. Please try again.');
    }
  });

  // ── /expiries [today|0|1|3|7] — check upcoming expiries ──────────────────
  bot.command('expiries', requireAdmin, async (ctx) => {
    try {
      const arg = (String(ctx.message?.text || '').trim().split(/\s+/)[1] || '').toLowerCase();

      let checkpoints = [0, 1, 3, 7];
      if (arg) {
        if (arg === 'today') {
          checkpoints = [0];
        } else {
          const days = parseInt(arg, 10);
          if (![0, 1, 3, 7].includes(days)) {
            return ctx.reply('Usage: `/expiries [today|0|1|3|7]`', { parse_mode: 'Markdown' });
          }
          checkpoints = [days];
        }
      }

      let message = '⏰ *Expiry Check*\n\n';
      const today = startOfToday();

      for (const days of checkpoints) {
        const targetStart = addDays(today, days);
        const targetEnd = new Date(targetStart);
        targetEnd.setHours(23, 59, 59, 999);

        const subs = await Subscription.find({
          status: 'active',
          expiryDate: { $gte: targetStart, $lte: targetEnd },
        }).sort({ expiryDate: 1 }).limit(50);

        const label = days === 0 ? 'Today' : `In ${days} day${days > 1 ? 's' : ''}`;
        message += `*${label}:* ${subs.length}\n`;

        if (subs.length) {
          subs.forEach((sub, index) => {
            message += `${index + 1}. \`${sub.telegramId}\` — ${sub.planName} — ${formatDate(sub.expiryDate)}\n`;
          });
        }

        message += '\n';
      }

      if (message.length > 3900) {
        message = message.slice(0, 3900) + '\n...truncated';
      }

      await ctx.reply(message, { parse_mode: 'Markdown' });
    } catch (err) {
      logger.error(`expiries command error: ${err.message}`);
      await ctx.reply('❌ Failed to fetch expiry list. Please try again.');
    }
  });

  // ── /plans ─────────────────────────────────────────────────────────────────
  bot.command('plans', requireAdmin, async (ctx) => {
    const plans = await getActivePlans();
    if (!plans.length) return ctx.reply('No active plans. Use /createplan to add one.');
    let msg = '📋 *Active Plans*\n\n';
    plans.forEach((p, i) => {
      msg += `${i + 1}. *${p.name}* — ${p.durationDays} days${p.price ? ` — ₹${p.price}` : ''}\n   ID: \`${p._id}\`\n`;
    });
    await ctx.reply(msg, { parse_mode: 'Markdown' });
  });

};

module.exports = { registerAdminHandlers, requireAdmin };
