// src/bot/handlers.js
// User-facing bot handlers:
//   /start  - welcome + referral processing
//   /status - subscription panel with renewal buttons
//   /support - show SUPPORT_CONTACT link (external support only)
//   request_access, renew_request, view_offers, my_referral

const { Markup } = require('telegraf');
const fs = require('fs');
const path = require('path');
const User = require('../models/User');
const Request = require('../models/Request');
const Subscription = require('../models/Subscription');
const Plan = require('../models/Plan');
const UserOffer = require('../models/UserOffer');
const DmWordFilter = require('../models/DmWordFilter');

const { findOrCreateUser } = require('../services/userService');
const { getActiveOffers, getActivePlans } = require('../services/adminService');
const { SUPPORT_CONTACT } = require('../services/supportService');
const {
  processReferral,
  processSellerReferral,
  registerSellerProgram,
  getSellerProgramSummary,
  requestSellerWithdrawal,
  getSellerWithdrawalHistory,
  getSellerPayoutLedgerHistory,
} = require('../services/referralService');
const { safeSend, renewalKeyboard, isGroupMember, scheduleDeleteMessage } = require('../utils/telegramUtils');
const { escapeMarkdown, escapeMarkdownInlineCode } = require('../utils/markdownEscape');
const { formatDate, daysRemaining } = require('../utils/dateUtils');
const { getGroupIdForCategory } = require('../utils/premiumGroups');
const { USER_FLOW_STATE, getUserFlowState, buildSetUserFlowUpdate } = require('../utils/userFlowState');
const { isBroadcastSessionActive } = require('../utils/broadcastSession');
const { registerPaymentFlow } = require('./paymentFlow');
const { registerSellerFlow, handleSellerWithdrawalUpiMessage } = require('./sellerFlow');
const logger = require('../utils/logger');

const REJOINING_PENALTY = process.env.REJOINING_PENALTY || '20';
const lastBotMessageByChat = new Map();
let startHelpVideoFileId = null;

const toTelegramContactUrl = (contact) => {
  const value = String(contact || '').trim();
  if (!value) return 'https://t.me/ImaxSupport1Bot';
  if (/^https?:\/\//i.test(value)) return value;
  const username = value.startsWith('@') ? value.slice(1) : value;
  return `https://t.me/${username}`;
};

const SUPPORT_CONTACT_URL = toTelegramContactUrl(SUPPORT_CONTACT);

const PLAN_CATEGORY = {
  MOVIE: 'movie',
  DESI: 'desi',
  NON_DESI: 'non_desi',
};

const PLAN_CATEGORY_LABELS = {
  [PLAN_CATEGORY.MOVIE]: 'Movie Premium',
  [PLAN_CATEGORY.DESI]: 'Desi Premium',
  [PLAN_CATEGORY.NON_DESI]: 'Non Desi Premium',
};

const PLAN_CATEGORY_BUTTON_LABELS = {
  [PLAN_CATEGORY.MOVIE]: 'Movie Plan',
  [PLAN_CATEGORY.DESI]: 'Desi Po*n Plan',
  [PLAN_CATEGORY.NON_DESI]: 'Non-Desi Po*n Plan',
};

const QR_ASSET_BY_CATEGORY = {
  [PLAN_CATEGORY.MOVIE]: 'qr-code.jpg',
  [PLAN_CATEGORY.DESI]: 'qr-code.jpg',
  [PLAN_CATEGORY.NON_DESI]: 'qr-code.jpg',
};

// ── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Button coloring (Bot API 9.4+). Older clients ignore `style`.
 * Styles: 'primary' (blue), 'success' (green), 'danger' (red)
 */
const withStyle = (button, style) => ({ ...button, style });

const normalizePlanCategory = (value) => {
  const normalized = String(value || PLAN_CATEGORY.MOVIE).toLowerCase().replace(/[-\s]/g, '_');

  if ([
    PLAN_CATEGORY.MOVIE,
    PLAN_CATEGORY.DESI,
    PLAN_CATEGORY.NON_DESI,
  ].includes(normalized)) {
    return normalized;
  }
  return PLAN_CATEGORY.MOVIE;
};

const getPlanCategoryLabel = (category) => {
  return PLAN_CATEGORY_LABELS[normalizePlanCategory(category)] || PLAN_CATEGORY_LABELS[PLAN_CATEGORY.MOVIE];
};

const formatInr = (value) => {
  const number = Number(value || 0);
  if (!Number.isFinite(number)) return '0';
  const rounded = Math.round(number * 100) / 100;
  return Number.isInteger(rounded) ? String(rounded) : rounded.toFixed(2);
};

const REFERRAL_REWARD_DISCOUNT_PERCENT = Math.min(100, Math.max(0, parseFloat(process.env.REFERRAL_REWARD_DISCOUNT_PERCENT || '10')));

const getBestPublicOffer = async (planId = null) => {
  const offers = await getActiveOffers();
  if (!offers?.length) return null;

  const validOffers = offers
    .filter((offer) => Number(offer.discountPercent) > 0)
    .sort((left, right) => Number(right.discountPercent || 0) - Number(left.discountPercent || 0));

  if (!planId) {
    return validOffers[0] || null;
  }

  const normalizedPlanId = String(planId);
  const planSpecific = validOffers.filter((offer) => offer?.planId && String(offer.planId) === normalizedPlanId);
  if (planSpecific.length) {
    return planSpecific[0];
  }

  const globalOffers = validOffers.filter((offer) => !offer?.planId);
  return globalOffers[0] || null;
};

const getDiscountedPrice = (price, discountPercent) => {
  const base = Number(price || 0);
  const discount = Number(discountPercent || 0);
  if (!base || discount <= 0) return base;
  return Math.ceil(Math.max(0, base - (base * discount / 100)));
};

const getNextOneTimeUserOffer = async (telegramId, planId = null) => {
  if (!telegramId) return null;

  const query = {
    targetTelegramId: telegramId,
    isActive: true,
    isUsed: false,
    validTill: { $gt: new Date() },
  };

  if (planId) {
    query.$or = [
      { planId: null },
      { planId },
    ];
  }

  return UserOffer.findOne({
    ...query,
  }).sort({ createdAt: -1 });
};

const strikeText = (value) => {
  return String(value || '').split('').map((ch) => `${ch}\u0336`).join('');
};

const isMessageNotModifiedError = (err) => {
  const message = err?.response?.description || err?.description || err?.message || '';
  return String(message).toLowerCase().includes('message is not modified');
};

const safeEditMessage = async (ctx, text, extra = {}) => {
  try {
    await ctx.editMessageText(text, extra);
  } catch (err) {
    if (isMessageNotModifiedError(err)) return;

    const message = String(err?.response?.description || err?.description || err?.message || '').toLowerCase();
    const needsCaptionEdit =
      message.includes('there is no text in the message to edit') ||
      message.includes('message text is empty');

    if (!needsCaptionEdit) throw err;

    try {
      await ctx.editMessageCaption(text, extra);
    } catch (captionErr) {
      if (!isMessageNotModifiedError(captionErr)) throw captionErr;
    }
  }
};

const sellerProgramKeyboard = (isRegistered, canWithdraw = false) => {
  const rows = [
    [withStyle(Markup.button.callback(isRegistered ? '📊 Refresh Seller Dashboard' : '🛍 Register as Seller', isRegistered ? 'seller_program' : 'register_seller'), 'primary')],
  ];

  if (isRegistered) {
    rows.push([withStyle(Markup.button.callback('💸 Request Withdrawal', 'seller_withdraw'), canWithdraw ? 'success' : 'primary')]);
    rows.push([withStyle(Markup.button.callback('🧾 Seller Payout Status', 'seller_payout_status'), 'primary')]);
  }

  rows.push([withStyle(Markup.button.callback('⬅️ Back', 'more_menu'), 'success')]);
  return Markup.inlineKeyboard(rows);
};

const formatSellerProgramMessage = (summary, botUsername) => {
  if (!summary?.isSeller) {
    return (
      `🛍 *Seller Program*\n\n` +
      `Seller banke aap premium refer karke earning kar sakte hain.\n` +
      `Har successful paid referral par *15% commission* milega.\n\n` +
      `Withdrawal eligibility:\n` +
      `• ${summary?.withdrawRules?.minReferrals || 10} qualified referrals *ya*\n` +
      `• ₹${summary?.withdrawRules?.minBalance || 200} balance\n\n` +
      `Niche button dabakar seller program join karein.`
    );
  }

  const sellerLink = `https://t.me/${botUsername}?start=seller_${summary.sellerCode}`;
  return (
    `🛍 *Seller Dashboard*\n\n` +
    `✅ Status: *Registered Seller*\n` +
    `🧾 Seller Code: \`${summary.sellerCode}\`\n` +
    `👥 Total Referred: *${summary.stats.totalReferrals || 0}*\n` +
    `✅ Qualified Referrals: *${summary.stats.qualifiedReferrals || 0}*\n` +
    `💰 Lifetime Earnings: *₹${Number(summary.stats.lifetimeEarnings || 0).toFixed(2)}*\n` +
    `💳 Available Balance: *₹${Number(summary.stats.availableBalance || 0).toFixed(2)}*\n\n` +
    `🔗 *Your Seller Link:*\n\`${sellerLink}\`\n\n` +
    (summary.canWithdraw
      ? `✅ You are eligible to request withdrawal.`
      : `ℹ️ Withdrawal unlock: ${summary.withdrawRules.minReferrals} qualified referrals *or* ₹${summary.withdrawRules.minBalance} balance.`)
  );
};

const replacePreviousBotReply = async (ctx, chatId, sentMessage) => {
  if (!sentMessage?.message_id) return;

  const key = String(chatId);
  const previousMessageId = lastBotMessageByChat.get(key);

  if (previousMessageId && previousMessageId !== sentMessage.message_id) {
    scheduleDeleteMessage(ctx.telegram, chatId, previousMessageId);
  }

  lastBotMessageByChat.set(key, sentMessage.message_id);
};

const normalizeFilterInput = (value) => String(value || '').trim().toLowerCase();

const findMatchedDmFilter = async (text) => {
  const normalizedText = normalizeFilterInput(text);
  if (!normalizedText) return null;

  const filters = await DmWordFilter.find({})
    .select('phrase normalizedPhrase responseType responseText responsePhotoFileId responseStickerFileId responseCaption')
    .lean();
  if (!filters.length) return null;

  const sortedFilters = [...filters].sort((left, right) => {
    const leftLen = String(left?.normalizedPhrase || '').length;
    const rightLen = String(right?.normalizedPhrase || '').length;
    return rightLen - leftLen;
  });

  return sortedFilters.find((filter) => {
    const phrase = normalizeFilterInput(filter?.normalizedPhrase);
    return phrase && normalizedText.includes(phrase);
  }) || null;
};

const sendDmFilterResponse = async (ctx, filter) => {
  const chatId = ctx.chat?.id;
  if (!chatId) return false;

  try {
    let sentMessage;
    if (filter?.responseType === 'photo' && filter?.responsePhotoFileId) {
      sentMessage = await ctx.telegram.sendPhoto(chatId, filter.responsePhotoFileId, {
        ...(filter.responseCaption ? { caption: filter.responseCaption } : {}),
      });
    } else if (filter?.responseType === 'sticker' && filter?.responseStickerFileId) {
      sentMessage = await ctx.telegram.sendSticker(chatId, filter.responseStickerFileId);
    } else {
      const text = String(filter?.responseText || '').trim();
      if (!text) return false;
      sentMessage = await ctx.telegram.sendMessage(chatId, text);
    }

    await replacePreviousBotReply(ctx, chatId, sentMessage);
    return true;
  } catch (err) {
    logger.error(`sendDmFilterResponse error: ${err.message}`);
    return false;
  }
};

const notifySellerWithdrawalRequest = async (bot, ctx, request) => {
  if (!process.env.LOG_CHANNEL_ID) {
    logger.warn('LOG_CHANNEL_ID is not set — seller withdrawal request was created but admins were not notified in Telegram.');
    return;
  }

  const sellerName = escapeMarkdown(ctx.from?.first_name || 'Seller');
  const sellerUsername = ctx.from?.username
    ? `@${escapeMarkdown(ctx.from.username)}`
    : 'N/A';

  await bot.telegram.sendMessage(
    process.env.LOG_CHANNEL_ID,
    `💸 *New Seller Withdrawal Request*\n\n` +
    `Request ID: \`${request._id}\`\n` +
    `Seller: *${sellerName}*\n` +
    `Seller ID: \`${request.sellerTelegramId}\`\n` +
    `Username: ${sellerUsername}\n` +
    `UPI ID: \`${escapeMarkdownInlineCode(request.upiId)}\`\n` +
    `Amount: *₹${Number(request.amount).toFixed(2)}*\n` +
    `Requested At: ${new Date(request.requestedAt).toLocaleString('en-IN')}\n` +
    `⏱ Processing Time: *Minimum 24 hours*`,
    {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [[
          { text: '✅ Approve', callback_data: `swd_approve_${request._id}` },
          { text: '❌ Reject', callback_data: `swd_reject_${request._id}` },
        ]],
      },
    }
  ).catch(() => { });
};

const consumeOneTimeUserOffer = async (telegramId, requestId, planId = null) => {
  const query = {
    targetTelegramId: telegramId,
    isActive: true,
    isUsed: false,
    validTill: { $gt: new Date() },
  };

  if (planId) {
    query.$or = [
      { planId: null },
      { planId },
    ];
  } else {
    // When plan is not known yet, consume only global private offers.
    query.planId = null;
  }

  return UserOffer.findOneAndUpdate(
    query,
    {
      $set: {
        isUsed: true,
        usedAt: new Date(),
        usedByRequestId: requestId,
      },
    },
    {
      sort: { createdAt: -1 },
      new: true,
    }
  );
};

/**
 * Build the approval keyboard for log channel requests.
 * Uses real plans from DB if any exist; falls back to hardcoded day options.
 */
const buildApprovalKeyboard = async (requestId, requestCategory = PLAN_CATEGORY.MOVIE) => {
  const normalizedCategory = normalizePlanCategory(requestCategory);
  const plans = await Plan.find({ isActive: true, category: normalizedCategory }).sort({ durationDays: 1 });

  let planButtons;
  if (plans.length > 0) {
    const rows = [];
    for (let i = 0; i < plans.length; i += 2) {
      rows.push(
        plans.slice(i, i + 2).map(p => withStyle({
          text: `${p.name} (${p.durationDays}d${p.price ? ` · ₹${p.price}` : ''})`,
          callback_data: `approve_${requestId}_${p._id}`,
        }, 'success'))
      );
    }
    planButtons = rows;
  } else {
    planButtons = [];
  }

  return {
    inline_keyboard: [
      ...planButtons,
      [withStyle({ text: '❌ Reject', callback_data: `reject_${requestId}` }, 'danger')],
    ],
  };
};

// ── Register handlers ─────────────────────────────────────────────────────────

const registerUserHandlers = (bot) => {

  bot.use(async (ctx, next) => {
    if (ctx.chat?.type !== 'private') return next();

    const sender = await User.findOne({ telegramId: ctx.from.id }).select('role').lean().catch(() => null);
    const senderRole = String(sender?.role || 'user').toLowerCase();
    const shouldProtectByDefault =
      String(process.env.PROTECT_BOT_MESSAGES || 'true').toLowerCase() === 'true' &&
      senderRole === 'user';

    const incomingUserMessageId = ctx.message?.message_id;
    let userMessageDeleted = false;
    const originalReply = ctx.reply.bind(ctx);

    ctx.reply = async (text, extra) => {
      const safeExtra = {
        ...(extra || {}),
        protect_content: typeof extra?.protect_content === 'undefined'
          ? shouldProtectByDefault
          : extra.protect_content,
      };

      const hasInlineButtons = Array.isArray(safeExtra?.reply_markup?.inline_keyboard)
        && safeExtra.reply_markup.inline_keyboard.length > 0;

      if (!hasInlineButtons) {
        safeExtra.reply_markup = {
          inline_keyboard: [[withStyle(Markup.button.callback('🏠 Main Menu', 'back_to_main'), 'primary')]],
        };
      }

      const sent = await originalReply(text, safeExtra);

      if (incomingUserMessageId && !userMessageDeleted) {
        userMessageDeleted = true;
        scheduleDeleteMessage(ctx.telegram, ctx.chat.id, incomingUserMessageId);
      }

      await replacePreviousBotReply(ctx, ctx.chat.id, sent);
      return sent;
    };

    return next();
  });

  const mainMenuKeyboard = () => Markup.inlineKeyboard([
    [withStyle(Markup.button.callback('📋 Check Plans', 'check_plans'), 'success')],
    [withStyle(Markup.button.callback('📱 More Menu', 'more_menu'), 'primary')],
  ]);

  const startMenuKeyboard = () => Markup.inlineKeyboard([
    [withStyle(Markup.button.callback('📋 Check Plans', 'check_plans'), 'success')],
    [withStyle(Markup.button.callback('📱 More Menu', 'more_menu'), 'primary')],
  ]);

  const CHECK_PLANS_MENU_CONFIG = [
    { category: PLAN_CATEGORY.MOVIE, text: '🎬 Movie Plan', callback: 'plan_menu_movie', style: 'primary' },
    { category: PLAN_CATEGORY.DESI, text: '🔥 Desi Po*n Plan', callback: 'plan_menu_desi', style: 'primary' },
    { category: PLAN_CATEGORY.NON_DESI, text: '🌍 Non-Desi Po*n Plan', callback: 'plan_menu_non_desi', style: 'primary' },
  ];

  const checkPlansKeyboard = async () => {
    const activeCategoryValues = await Plan.distinct('category', { isActive: true });
    const activeCategories = new Set(
      activeCategoryValues.map((value) => normalizePlanCategory(value))
    );

    const rows = CHECK_PLANS_MENU_CONFIG
      .filter((item) => activeCategories.has(item.category))
      .map((item) => [withStyle(Markup.button.callback(item.text, item.callback), item.style)]);

    rows.push([withStyle(Markup.button.callback('⬅️ Back Button', 'back_to_main'), 'success')]);
    return Markup.inlineKeyboard(rows);
  };

  const moreMenuKeyboard = () => Markup.inlineKeyboard([
    [withStyle(Markup.button.callback('📊 Check Subscription Status', 'check_status'), 'primary')],
    [withStyle(Markup.button.callback('🎁 View Current Offers', 'view_offers'), 'primary')],
    [withStyle(Markup.button.callback('🔗 My referal link', 'my_referral'), 'primary')],
    [withStyle(Markup.button.callback('🛍 Seller Program', 'seller_program'), 'primary')],
    [Markup.button.url('🎫 Contact Support', SUPPORT_CONTACT_URL)],
    [withStyle(Markup.button.callback('⬅️ Back button', 'back_to_main'), 'success')],
  ]);

  const premiumSelectionKeyboard = () => Markup.inlineKeyboard([
    [withStyle(Markup.button.callback('🎬 Movie Premium', 'request_premium_movie'), 'success')],
    [withStyle(Markup.button.callback('🔥 Desi Premium', 'request_premium_desi'), 'success')],
    [withStyle(Markup.button.callback('🌍 Non Desi Premium', 'request_premium_non_desi'), 'success')],
    [Markup.button.url('🎫 Support Chat', SUPPORT_CONTACT_URL)],
  ]);

  const renewCategoryKeyboard = (categories) => {
    const rows = [];
    if (categories.includes(PLAN_CATEGORY.MOVIE)) {
      rows.push([withStyle(Markup.button.callback('🔄 Renew Movie Premium', 'status_renew_movie'), 'success')]);
    }
    if (categories.includes(PLAN_CATEGORY.DESI)) {
      rows.push([withStyle(Markup.button.callback('🔄 Renew Desi Premium', 'status_renew_desi'), 'success')]);
    }
    if (categories.includes(PLAN_CATEGORY.NON_DESI)) {
      rows.push([withStyle(Markup.button.callback('🔄 Renew Non Desi Premium', 'status_renew_non_desi'), 'success')]);
    }
    rows.push([withStyle(Markup.button.callback('🏠 Main Menu', 'back_to_main'), 'primary')]);
    return Markup.inlineKeyboard(rows);
  };

  const getCategoryPlans = async (category) => {
    const normalizedCategory = normalizePlanCategory(category);
    return Plan.find({
      isActive: true,
      category: normalizedCategory,
    }).sort({ durationDays: 1 });
  };

  const buildCategoryPlansText = async (category, options = {}) => {
    const plans = await getCategoryPlans(category);
    const title = PLAN_CATEGORY_BUTTON_LABELS[normalizePlanCategory(category)] || getPlanCategoryLabel(category);
    if (!plans.length) {
      return `📋 ${title}\n\nNo active plans found for this category right now.\nPlease contact support from More Menu.`;
    }

    let text = `📋 ${title}\n\n`;
    for (let index = 0; index < plans.length; index += 1) {
      const plan = plans[index];
      const bestOffer = await getBestPublicOffer(plan._id);
      const privateOffer = await getNextOneTimeUserOffer(options.telegramId, plan._id);
      text += `${index + 1}. ${plan.name} — ${plan.durationDays} days`;
      if (plan.price) {
        const privateDiscountPercent = Number(privateOffer?.discountPercent || 0);
        const publicDiscountPercent = Number(bestOffer?.discountPercent || 0);
        const appliedDiscountPercent = privateDiscountPercent > 0
          ? privateDiscountPercent
          : publicDiscountPercent;

        if (appliedDiscountPercent > 0) {
          const discounted = getDiscountedPrice(plan.price, appliedDiscountPercent);
          text += ` — ${strikeText(`₹${formatInr(plan.price)}`)} ₹${formatInr(discounted)} (${appliedDiscountPercent}% OFF)`;
        } else {
          text += ` — ₹${formatInr(plan.price)}`;
        }
      }
      text += `\n`;
    }

    const firstPlanOffer = plans.length > 0
      ? await getNextOneTimeUserOffer(options.telegramId, plans[0]._id)
      : null;

    if (Number(firstPlanOffer?.discountPercent || 0) > 0) {
      text += `\n🎁 Offer Applied: ${firstPlanOffer.discountPercent}% OFF (${firstPlanOffer.title})\n`;
    } else if (plans.length > 0) {
      const categoryOffer = await getBestPublicOffer(plans[0]._id);
      if (categoryOffer?.discountPercent > 0) {
        text += `\n🎁 Offer Applied: ${categoryOffer.discountPercent}% OFF (${categoryOffer.title})\n`;
      }
    }

    text += `\n✅ Payment karne ke baad niche apne plan ka Paid button pe click karein.`;
    return text;
  };

  const sendMainMenuMessage = async (ctx, userName = 'User', options = {}) => {
    const menuKeyboard = options?.includeNonDesiRejoin ? startMenuKeyboard() : mainMenuKeyboard();
    await ctx.reply(
      `👋 *Welcome, ${escapeMarkdown(userName)}!*\n\n` +
      `Help k liye uper ki video dekhein.\n\n ` +
      `Premium lene ke liye pehle *Check Plans* pe tap karein.\n\n` +
      `Agar payment already ho gaya hai, toh *Check Plans* me category select karke *Paid* button pe tap karke apna payment proof submit karein.\n\n`,
      {
        parse_mode: 'Markdown',
        ...menuKeyboard,
      }
    );
  };

  const sendStartWelcomeMessage = async (ctx, userName = 'User') => {
    const caption =
      `👋 *Welcome, ${escapeMarkdown(userName)}!*\n\n` +
      `Help k liye uper ki video dekhein.\n\n ` +
      `Premium lene ke liye pehle *Check Plans* pe tap karein.\n\n` +
      `Agar payment already ho gaya hai, toh *Check Plans* me category select karke *Paid* button pe tap karke apna payment proof submit karein.\n\n`;

    try {
      if (startHelpVideoFileId) {
        await ctx.replyWithVideo(
          startHelpVideoFileId,
          {
            caption,
            parse_mode: 'Markdown',
            ...startMenuKeyboard(),
          }
        );
        return;
      }

      const helpVideoPath = path.join(process.cwd(), 'assets', 'Help_Video.mp4');
      if (!fs.existsSync(helpVideoPath)) {
        return sendMainMenuMessage(ctx, userName, { includeNonDesiRejoin: true });
      }

      const sent = await ctx.replyWithVideo(
        { source: helpVideoPath },
        {
          caption,
          parse_mode: 'Markdown',
          ...startMenuKeyboard(),
        }
      );

      startHelpVideoFileId = sent?.video?.file_id || null;
    } catch (err) {
      logger.warn(`sendStartWelcomeMessage video send failed: ${err.message}`);
      await sendMainMenuMessage(ctx, userName, { includeNonDesiRejoin: true });
    }
  };

  bot.action('start_non_desi_rejoin', async (ctx) => {
    await ctx.answerCbQuery();
    const qrFileName = QR_ASSET_BY_CATEGORY[PLAN_CATEGORY.NON_DESI];
    const qrPath = path.join(process.cwd(), 'assets', qrFileName);

    const rejoinMessage =
      `⚠️Ye MSG DHIYAN se Read Kre\n\n` +
      `Lifetime wala channel ab available nahi hai.\n\n` +
      `Naya channel launch hua hai jisme 3 months validity milegi.\n\n` +
      `Purane users ke liye special offer: ₹30 me 3 months.\n\n` +
      `Renewal price: ₹89 for 3 months.\n`;

    const keyboard = Markup.inlineKeyboard([
      [withStyle(Markup.button.callback('✅ Paid', 'paid_non_desi'), 'success')],
      [withStyle(Markup.button.callback('🏠 Main Menu', 'back_to_main'), 'primary')],
    ]);

    if (fs.existsSync(qrPath)) {
      await ctx.replyWithPhoto(
        { source: qrPath },
        {
          caption: rejoinMessage,
          ...keyboard,
        }
      );
      return;
    }

    await ctx.reply(
      `${rejoinMessage}\n\n⚠️ QR image missing: ${qrFileName} (assets folder).`,
      {
        ...keyboard,
      }
    );
  });

  bot.command('menu', async (ctx) => {
    try {
      const user = await findOrCreateUser(ctx.from);
      await User.findByIdAndUpdate(user._id, { lastInteraction: new Date() });
      await sendMainMenuMessage(ctx, user.name || ctx.from?.first_name || 'User', { includeNonDesiRejoin: true });
    } catch (err) {
      logger.error(`menu command error: ${err.message}`);
      await ctx.reply('❌ Menu open nahi ho paya. Please try again.');
    }
  });

  const submitPremiumRequest = async (ctx, category) => {
    const normalizedCategory = normalizePlanCategory(category);
    const categoryLabel = getPlanCategoryLabel(normalizedCategory);

    const user = await findOrCreateUser(ctx.from);
    await User.findByIdAndUpdate(user._id, { lastInteraction: new Date() });

    const sameCategoryActiveSub = await Subscription.findOne({
      telegramId: ctx.from.id,
      status: 'active',
      expiryDate: { $gt: new Date() },
      planCategory: normalizedCategory,
    });

    if (sameCategoryActiveSub) {
      await ctx.reply(
        `✅ *Aapka ${escapeMarkdown(categoryLabel)} subscription active hai!*\n\n` +
        `📋 Plan: *${escapeMarkdown(sameCategoryActiveSub.planName)}*\n` +
        `📅 Expires: *${formatDate(sameCategoryActiveSub.expiryDate)}*`,
        { parse_mode: 'Markdown' }
      );
      return;
    }

    const sameCategoryPendingReq = await Request.findOne({
      telegramId: ctx.from.id,
      status: 'pending',
      requestCategory: normalizedCategory,
    });

    if (sameCategoryPendingReq) {
      await ctx.reply(
        `⏳ *${escapeMarkdown(categoryLabel)} request already submitted*\n\n` +
        `Admin aapki request verify kar rahe hain. Thoda wait karein.`,
        { parse_mode: 'Markdown' }
      );
      return;
    }

    const userDoc = await User.findOne({ telegramId: ctx.from.id });
    const latestProof = userDoc?.meta?.latestPaymentProof;
    const proofReadyForCategory = normalizePlanCategory(userDoc?.meta?.paymentProofReadyForCategory);
    const selectedPlanId = String(userDoc?.meta?.paymentSelectedPlanId || '').trim();
    const selectedPlan = selectedPlanId ? await Plan.findById(selectedPlanId) : null;
    const isSelectedPlanValid = !!(selectedPlan
      && selectedPlan.isActive
      && normalizePlanCategory(selectedPlan.category) === normalizedCategory);
    if (!latestProof?.fileId) {
      await ctx.reply(
        `⚠️ Payment screenshot required.\n\n` +
        `Pehle *Check Plans* → category select karein → plan ka *Paid* button dabayein aur screenshot upload karein.`,
        { parse_mode: 'Markdown' }
      );
      return;
    }

    if (proofReadyForCategory !== normalizedCategory) {
      await ctx.reply(
        `⚠️ Fresh payment screenshot required.\n\n` +
        `Pehle *Check Plans* → *${escapeMarkdown(getPlanCategoryLabel(normalizedCategory))}* select karein → plan ka *Paid* button dabayein aur naya screenshot upload karein.`,
        { parse_mode: 'Markdown' }
      );
      return;
    }

    if (normalizePlanCategory(latestProof.category) !== normalizedCategory) {
      await ctx.reply(
        `⚠️ Aapne alag category ka screenshot upload kiya hai.\n\n` +
        `Kripya *${escapeMarkdown(getPlanCategoryLabel(normalizedCategory))}* ke liye naya screenshot upload karein.`,
        { parse_mode: 'Markdown' }
      );
      return;
    }

    const newRequest = await Request.create({
      userId: user._id,
      telegramId: ctx.from.id,
      status: 'pending',
      requestCategory: normalizedCategory,
      selectedPlanId: isSelectedPlanValid ? selectedPlan._id : null,
      paymentProof: {
        fileId: latestProof.fileId,
        fileUniqueId: latestProof.fileUniqueId || null,
        sourceType: latestProof.sourceType || null,
        logMessageId: latestProof.logMessageId || null,
      },
    });

    const consumedOffer = await consumeOneTimeUserOffer(
      ctx.from.id,
      newRequest._id,
      isSelectedPlanValid ? selectedPlan._id : null
    );
    if (consumedOffer) {
      await Request.findByIdAndUpdate(newRequest._id, {
        appliedUserOffer: {
          offerId: consumedOffer._id,
          title: consumedOffer.title,
          discountPercent: consumedOffer.discountPercent,
        },
      });
    }

    await User.findByIdAndUpdate(user._id, {
      status: 'pending',
      ...buildSetUserFlowUpdate(
        USER_FLOW_STATE.IDLE,
        {},
        {
          'meta.latestPaymentProof': '',
          'meta.paymentProofReadyForCategory': '',
          'meta.paymentCategory': '',
          'meta.paymentFlowType': '',
          'meta.renewalPlanId': '',
          'meta.paymentSelectedPlanId': '',
        }
      ),
    });

    await ctx.reply(
      `✅ *${escapeMarkdown(categoryLabel)} request submitted!*\n\n` +
      (isSelectedPlanValid
        ? `📋 Selected Plan: *${escapeMarkdown(selectedPlan.name)}* (${selectedPlan.durationDays} days${selectedPlan.price ? ` · ₹${formatInr(selectedPlan.price)}` : ''})\n\n`
        : '') +
      `Admin aapki payment verify karke jaldi approval denge.\n\n` +
      (consumedOffer
        ? `🎁 *Private offer applied:* ${escapeMarkdown(consumedOffer.title)}${consumedOffer.discountPercent > 0 ? ` (*${consumedOffer.discountPercent}% OFF*)` : ''}\n\n`
        : '') +
      `⏱ Usually 20 minutes ke andar approval mil jata hai.`,
      { parse_mode: 'Markdown' }
    );

    const keyboard = await buildApprovalKeyboard(newRequest._id, normalizedCategory);
    const safeName = escapeMarkdown(user.name);
    const safeUsername = user.username ? `@${escapeMarkdown(user.username)}` : 'N/A';
    const referredByUser = user.referredBy || null;
    const referredBySeller = user.sellerReferredBy || null;

    const logCaption =
      `🆕 *New Premium Access Request*\n\n` +
      `📦 Category: *${escapeMarkdown(categoryLabel)}*\n` +
      `👤 Name: ${safeName}\n` +
      `🆔 User ID: \`${ctx.from.id}\`\n` +
      `📛 Username: ${safeUsername}\n` +
      `🤝 Referred By (User): \`${referredByUser || 'N/A'}\`\n` +
      `🛍 Referred By (Seller): \`${referredBySeller || 'N/A'}\`\n` +
      (isSelectedPlanValid
        ? `📋 Selected Plan: *${escapeMarkdown(selectedPlan.name)}* (${selectedPlan.durationDays} days${selectedPlan.price ? ` · ₹${formatInr(selectedPlan.price)}` : ''})\n`
        : '') +
      (consumedOffer
        ? `🎁 Private Offer: *${escapeMarkdown(consumedOffer.title)}*${consumedOffer.discountPercent > 0 ? ` (*${consumedOffer.discountPercent}% OFF*)` : ''}\n`
        : '') +
      `🕒 Time: ${new Date().toLocaleString('en-IN')}`;

    let logMsg;
    try {
      const proofSource = String(latestProof.sourceType || 'photo').toLowerCase();
      if (proofSource === 'photo') {
        logMsg = await bot.telegram.sendPhoto(process.env.LOG_CHANNEL_ID, latestProof.fileId, {
          caption: logCaption,
          parse_mode: 'Markdown',
          reply_markup: keyboard,
        });
      } else {
        logMsg = await bot.telegram.sendDocument(process.env.LOG_CHANNEL_ID, latestProof.fileId, {
          caption: logCaption,
          parse_mode: 'Markdown',
          reply_markup: keyboard,
        });
      }
    } catch (err) {
      logger.error(`new request log channel attach error: ${err.message}`);
      logMsg = await bot.telegram.sendMessage(
        process.env.LOG_CHANNEL_ID,
        `${logCaption}\n\n⚠️ *Could not attach proof file in log channel.*`,
        { parse_mode: 'Markdown', reply_markup: keyboard }
      );
    }

    await Request.findByIdAndUpdate(newRequest._id, {
      logMessageId: logMsg.message_id,
      'paymentProof.logMessageId': logMsg.message_id,
    });
    logger.info(`New ${normalizedCategory} access request for user ${ctx.from.id}`);
  };

  // ── /start ─────────────────────────────────────────────────────────────────
  bot.start(async (ctx) => {
    try {
      const user = await findOrCreateUser(ctx.from);
      await User.findByIdAndUpdate(user._id, { lastInteraction: new Date() });

      // Referral: /start ref_XXXXXXXX
      const payload = ctx.startPayload;
      if (payload && payload.startsWith('seller_')) {
        await processSellerReferral(user, payload.replace('seller_', ''));
      } else if (payload && payload.startsWith('ref_')) {
        await processReferral(user, payload.replace('ref_', ''));
      }

      await sendStartWelcomeMessage(ctx, user.name);
    } catch (err) {
      logger.error(`/start error: ${err.message}`);
      await ctx.reply('❌ Something went wrong. Please try again.');
    }
  });

  bot.action('check_plans', async (ctx) => {
    await ctx.answerCbQuery();

    const baseKeyboard = (await checkPlansKeyboard()).reply_markup?.inline_keyboard || [];
    const backRow = baseKeyboard.length ? [baseKeyboard[baseKeyboard.length - 1]] : [];
    const planRows = baseKeyboard.length ? baseKeyboard.slice(0, -1) : [];
    const keyboardWithSupport = Markup.inlineKeyboard([
      ...planRows,
      [withStyle(Markup.button.url('🎫 Support Chat', SUPPORT_CONTACT_URL), 'primary')],
      ...backRow,
    ]);

    const hasActivePlanRows = planRows.length > 0;

    await safeEditMessage(
      ctx,
      hasActivePlanRows
        ? `📋 *Check Plans*\n\nApni pasand ka plan choose karein.\n\n` +
        `Plan se related koi issue ho to support se contact karein.`
        : `📋 *Check Plans*\n\nAbhi kisi bhi category me active plan available nahi hai.\n\n` +
        `Please support se contact karein.`,
      {
        parse_mode: 'Markdown',
        ...keyboardWithSupport,
      }
    );
  });

  // ── More Menu ───────────────────────────────────────────────────────────────
  bot.action('more_menu', async (ctx) => {
    await ctx.answerCbQuery();
    await safeEditMessage(
      ctx,
      `📋 *More Menu*\n\nNiche diye gaye options me se koi bhi choose karein.`,
      {
        parse_mode: 'Markdown',
        ...moreMenuKeyboard(),
      }
    );
  });

  bot.action('already_paid_menu', async (ctx) => {
    await ctx.answerCbQuery();
    const caption =
      `✅ *Aapne premium k liye pay kar diya*\n\n` +
      `Payment ya approval me koi issue ho to support se contact karein.`;

    const keyboard = Markup.inlineKeyboard([
      [Markup.button.url('🎫 Support Chat', SUPPORT_CONTACT_URL)],
      [withStyle(Markup.button.callback('⬅️ Back', 'back_to_main'), 'success')],
    ]);

    const callbackMessageId = ctx.callbackQuery?.message?.message_id;
    const callbackChatId = ctx.callbackQuery?.message?.chat?.id;

    try {
      if (startHelpVideoFileId) {
        await ctx.replyWithVideo(startHelpVideoFileId, {
          caption,
          parse_mode: 'Markdown',
          ...keyboard,
        });
      } else {
        const helpVideoPath = path.join(process.cwd(), 'assets', 'Help_Video.mp4');
        if (fs.existsSync(helpVideoPath)) {
          const sent = await ctx.replyWithVideo(
            { source: helpVideoPath },
            {
              caption,
              parse_mode: 'Markdown',
              ...keyboard,
            }
          );
          startHelpVideoFileId = sent?.video?.file_id || startHelpVideoFileId;
        } else {
          await ctx.reply(caption, {
            parse_mode: 'Markdown',
            ...keyboard,
          });
        }
      }

      if (callbackChatId && callbackMessageId) {
        scheduleDeleteMessage(ctx.telegram, callbackChatId, callbackMessageId);
      }
    } catch (err) {
      logger.error(`already_paid_menu action error: ${err.message}`);
      await ctx.reply(caption, {
        parse_mode: 'Markdown',
        ...keyboard,
      });
    }
  });

  registerPaymentFlow({
    bot,
    fs,
    path,
    User,
    Request,
    Subscription,
    Plan,
    Markup,
    withStyle,
    logger,
    PLAN_CATEGORY,
    QR_ASSET_BY_CATEGORY,
    normalizePlanCategory,
    getPlanCategoryLabel,
    buildCategoryPlansText,
    buildSetUserFlowUpdate,
    USER_FLOW_STATE,
    escapeMarkdown,
    findOrCreateUser,
    getBestPublicOffer,
    getDiscountedPrice,
    getNextOneTimeUserOffer,
    formatInr,
    consumeOneTimeUserOffer,
    submitPremiumRequest,
  });

  registerSellerFlow({
    bot,
    User,
    Markup,
    withStyle,
    logger,
    registerSellerProgram,
    getSellerProgramSummary,
    getSellerWithdrawalHistory,
    getSellerPayoutLedgerHistory,
    formatSellerProgramMessage,
    sellerProgramKeyboard,
    buildSetUserFlowUpdate,
    USER_FLOW_STATE,
  });

  // ── Back to main menu (same message) ────────────────────────────────────────
  bot.action('back_to_main', async (ctx) => {
    await ctx.answerCbQuery();
    const callbackMessageId = ctx.callbackQuery?.message?.message_id;
    const callbackChatId = ctx.callbackQuery?.message?.chat?.id;

    await sendMainMenuMessage(ctx, ctx.from?.first_name || 'User', { includeNonDesiRejoin: true });

    if (callbackChatId && callbackMessageId) {
      scheduleDeleteMessage(ctx.telegram, callbackChatId, callbackMessageId);
    }
  });

  // ── Premium access callbacks ───────────────────────────────────────────────
  bot.action('request_access', async (ctx) => {
    await ctx.answerCbQuery();
    try {
      await safeEditMessage(
        ctx,
        `✅ *Already Paid for premium*\n\nApna premium type choose karein:`,
        {
          parse_mode: 'Markdown',
          ...premiumSelectionKeyboard(),
        }
      );
    } catch (err) {
      logger.error(`request_access error: ${err.message}`);
      await ctx.reply('❌ An error occurred. Please try again.');
    }
  });

  bot.action('request_premium_movie', async (ctx) => {
    await ctx.answerCbQuery('Submitting...');
    try {
      await submitPremiumRequest(ctx, PLAN_CATEGORY.MOVIE);
    } catch (err) {
      logger.error(`request_premium_movie error: ${err.message}`);
      await ctx.reply('❌ Request failed. Please try again.');
    }
  });

  bot.action('request_premium_desi', async (ctx) => {
    await ctx.answerCbQuery('Submitting...');
    try {
      await submitPremiumRequest(ctx, PLAN_CATEGORY.DESI);
    } catch (err) {
      logger.error(`request_premium_desi error: ${err.message}`);
      await ctx.reply('❌ Request failed. Please try again.');
    }
  });

  bot.action('request_premium_non_desi', async (ctx) => {
    await ctx.answerCbQuery('Submitting...');
    try {
      await submitPremiumRequest(ctx, PLAN_CATEGORY.NON_DESI);
    } catch (err) {
      logger.error(`request_premium_non_desi error: ${err.message}`);
      await ctx.reply('❌ Request failed. Please try again.');
    }
  });

  // ── /status + check_status button ─────────────────────────────────────────
  const showStatus = async (ctx) => {
    try {
      await User.findOneAndUpdate({ telegramId: ctx.from.id }, { lastInteraction: new Date() });

      const activeSubs = await Subscription.find({
        telegramId: ctx.from.id,
        status: 'active',
        expiryDate: { $gt: new Date() },
      }).sort({ expiryDate: 1 });

      if (activeSubs.length) {
        const activeCategories = [...new Set(activeSubs.map((sub) => normalizePlanCategory(sub.planCategory || PLAN_CATEGORY.MOVIE)))]
          .filter((category) => [
            PLAN_CATEGORY.MOVIE,
            PLAN_CATEGORY.DESI,
            PLAN_CATEGORY.NON_DESI,
          ].includes(category));

        let message = `📊 *Your Subscription Status*\n\n` +
          `✅ Status: *Active*\n` +
          `📦 Active Plans: *${activeSubs.length}*\n\n`;

        for (let index = 0; index < activeSubs.length; index += 1) {
          const sub = activeSubs[index];
          const category = normalizePlanCategory(sub.planCategory || PLAN_CATEGORY.MOVIE);
          const groupId = sub.premiumGroupId || getGroupIdForCategory(category);
          const inGroup = groupId ? await isGroupMember(bot, groupId, ctx.from.id) : false;
          const remaining = daysRemaining(sub.expiryDate);

          message +=
            `${index + 1}. *${escapeMarkdown(getPlanCategoryLabel(category))}*\n` +
            `📋 Plan: *${escapeMarkdown(sub.planName)}*\n` +
            `📅 Expires on: *${formatDate(sub.expiryDate)}*\n` +
            `⏳ Days Remaining: *${remaining} days*\n` +
            `👥 Group Status: *${inGroup ? 'Joined' : 'Not Joined'}*\n`;

          if (!inGroup) {
            message +=
              `⚠️ Rejoin penalty for this category: *₹${REJOINING_PENALTY}*\n` +
              `Support: /support\n`;
          }

          message += '\n';
        }

        return ctx.reply(
          message + `💡 Renew karne ke liye niche category select karein.`,
          {
            parse_mode: 'Markdown',
            ...renewCategoryKeyboard(activeCategories),
          }
        );
      }

      await ctx.reply(
        `❌ *No Active Subscription*\n\n` +
        `Aapka koi subscription active nahi hai.\n` +
        `Niche diye gaye button pe click karein premium join request:`,
        {
          parse_mode: 'Markdown',
          ...Markup.inlineKeyboard([
            [withStyle(Markup.button.callback('🌟 Request Access', 'request_access'), 'success')],
          ]),
        }
      );
    } catch (err) {
      logger.error(`showStatus error: ${err.message}`);
    }
  };

  bot.command('status', showStatus);
  bot.action('check_status', async (ctx) => {
    await ctx.answerCbQuery();
    await showStatus(ctx);
  });

  bot.action(/^status_renew_(movie|desi|non_desi)$/, async (ctx) => {
    await ctx.answerCbQuery();
    try {
      const category = normalizePlanCategory(ctx.match[1]);
      const activeSub = await Subscription.findOne({
        telegramId: ctx.from.id,
        status: 'active',
        expiryDate: { $gt: new Date() },
        planCategory: category,
      });
      if (!activeSub) {
        return safeEditMessage(
          ctx,
          `⚠️ *${escapeMarkdown(getPlanCategoryLabel(category))}* currently active nahi hai.\n\n` +
          `Renew button sirf active categories ke liye available hai.`,
          {
            parse_mode: 'Markdown',
            ...Markup.inlineKeyboard([
              [withStyle(Markup.button.callback('⬅️ Back to Status', 'check_status'), 'success')],
              [withStyle(Markup.button.callback('🏠 Main Menu', 'back_to_main'), 'primary')],
            ]),
          }
        );
      }

      const plans = await Plan.find({ isActive: true, category }).sort({ durationDays: 1 });
      const categoryLabel = getPlanCategoryLabel(category);

      if (!plans.length) {
        return safeEditMessage(
          ctx,
          `⚠️ *${escapeMarkdown(categoryLabel)}* ke liye abhi active renewal plans available nahi hain.\n\n` +
          `Please support se contact karein.`,
          {
            parse_mode: 'Markdown',
            ...Markup.inlineKeyboard([
              [withStyle(Markup.button.callback('⬅️ Back to Status', 'check_status'), 'success')],
              [withStyle(Markup.button.callback('🏠 Main Menu', 'back_to_main'), 'primary')],
            ]),
          }
        );
      }

      const keyboard = {
        inline_keyboard: [
          ...await Promise.all(plans.map(async (plan) => {
            const bestOffer = await getBestPublicOffer(plan._id);
            const privateOffer = await getNextOneTimeUserOffer(ctx.from.id, plan._id);
            return [withStyle({
              text: (() => {
                if (!plan.price) return `🔄 Renew ${plan.durationDays} Days`;
                const privateDiscountPercent = Number(privateOffer?.discountPercent || 0);
                const publicDiscountPercent = Number(bestOffer?.discountPercent || 0);
                const appliedDiscountPercent = privateDiscountPercent > 0
                  ? privateDiscountPercent
                  : publicDiscountPercent;

                if (appliedDiscountPercent > 0) {
                  const discounted = getDiscountedPrice(plan.price, appliedDiscountPercent);
                  return `🔄 Renew ${plan.durationDays} Days · ₹${formatInr(discounted)}`;
                }
                return `🔄 Renew ${plan.durationDays} Days · ₹${formatInr(plan.price)}`;
              })(),
              callback_data: `renew_request_${category}_${plan._id}`,
            }, 'success')];
          })),
          [withStyle(Markup.button.callback('⬅️ Back to Status', 'check_status'), 'success')],
          [withStyle(Markup.button.callback('🏠 Main Menu', 'back_to_main'), 'primary')],
        ],
      };

      await safeEditMessage(
        ctx,
        `🔄 *${escapeMarkdown(categoryLabel)} Renewal*\n\n` +
        `Niche se plan select karein:`,
        {
          parse_mode: 'Markdown',
          reply_markup: keyboard,
        }
      );
    } catch (err) {
      logger.error(`status_renew action error: ${err.message}`);
      await ctx.reply('❌ Renewal options load nahi ho paye. Please try again.');
    }
  });

  // ── View Plans + Offers ───────────────────────────────────────────────────
  bot.action('view_plans_offers', async (ctx) => {
    await ctx.answerCbQuery();
    try {
      await User.findOneAndUpdate({ telegramId: ctx.from.id }, { lastInteraction: new Date() });

      const plans = await getActivePlans();
      const offers = await getActiveOffers();

      let message = `📋 *Plans ki Jankari*\n\n`;

      if (!plans.length) {
        message += `Plan check karne k liye /start type karen\n\n`;
      } else {
        plans.forEach((plan, i) => {
          message += `${i + 1}. *${escapeMarkdown(plan.name)}* — ${plan.durationDays} days`;
          if (plan.price) message += ` — ₹${plan.price}`;
          message += `\n`;
        });
        message += `\n`;
      }

      message += `🎁 *Current Offers*\n\n`;
      if (!offers.length) {
        message += `Koi active offer nahi hai abhi.`;
      } else {
        offers.forEach((offer, i) => {
          const days = Math.max(0, Math.ceil((new Date(offer.validTill) - new Date()) / 86400000));
          message += `${i + 1}. *${escapeMarkdown(offer.title)}*\n`;
          message += `${escapeMarkdown(offer.description)}\n`;
          if (offer.discountPercent > 0) message += `💰 *${offer.discountPercent}% OFF*\n`;
          message += `⏰ Expires in *${days} day${days !== 1 ? 's' : ''}*\n\n`;
        });
      }

      await ctx.reply(message, { parse_mode: 'Markdown' });
    } catch (err) {
      logger.error(`view_plans_offers error: ${err.message}`);
      await ctx.reply('❌ Unable to fetch plans right now. Please try again.');
    }
  });

  // ── View Offers ────────────────────────────────────────────────────────────
  const showCurrentOffers = async (ctx) => {
    try {
      await User.findOneAndUpdate({ telegramId: ctx.from.id }, { lastInteraction: new Date() });
      const offers = await getActiveOffers();
      const privateOffers = await UserOffer.find({
        targetTelegramId: ctx.from.id,
        isActive: true,
        isUsed: false,
        validTill: { $gt: new Date() },
      })
        .populate('planId', 'name')
        .sort({ createdAt: -1 });

      if (!offers.length && !privateOffers.length) {
        return ctx.reply(
          `😔 *Koi active offers nahi hai abhi filhaal!*\n\n` +
          `New offer aane pe aapko notification mil jayega.`,
          { parse_mode: 'Markdown' }
        );
      }

      let message = `🎁 *Current Offers*\n\n`;
      if (privateOffers.length) {
        message += `⭐ *Your Private One-Time Offers*\n\n`;
        privateOffers.forEach((offer, i) => {
          const days = Math.max(0, Math.ceil((new Date(offer.validTill) - new Date()) / 86400000));
          message += `*${i + 1}. ${escapeMarkdown(offer.title)}*\n`;
          message += `${escapeMarkdown(offer.description)}\n`;
          if (offer?.planId?.name) {
            message += `📦 Plan: *${escapeMarkdown(offer.planId.name)}*\n`;
          }
          if (offer.discountPercent > 0) message += `💰 *${offer.discountPercent}% OFF*\n`;
          message += `⏰ Expires in *${days} day${days !== 1 ? 's' : ''}*\n`;
          message += `ℹ️ *Auto-applies on your next request/renewal (one time only).*\n\n`;
        });
      }

      if (offers.length) {
        message += `🎁 *Public Offers*\n\n`;
      }
      offers.forEach((offer, i) => {
        const days = Math.max(0, Math.ceil((new Date(offer.validTill) - new Date()) / 86400000));
        message += `*${i + 1}. ${escapeMarkdown(offer.title)}*\n`;
        message += `${escapeMarkdown(offer.description)}\n`;
        if (offer.discountPercent > 0) message += `💰 *${offer.discountPercent}% OFF*\n`;
        message += `⏰ Expires in *${days} day${days !== 1 ? 's' : ''}*\n\n`;
      });

      await ctx.reply(message, { parse_mode: 'Markdown' });
    } catch (err) {
      logger.error(`view_offers error: ${err.message}`);
    }
  };

  bot.action('view_offers', async (ctx) => {
    await ctx.answerCbQuery();
    await showCurrentOffers(ctx);
  });

  bot.command('offers', async (ctx) => {
    await showCurrentOffers(ctx);
  });

  // ── My Referral Link ───────────────────────────────────────────────────────
  bot.action('my_referral', async (ctx) => {
    await ctx.answerCbQuery();
    try {
      const user = await findOrCreateUser(ctx.from);
      const botInfo = await bot.telegram.getMe();
      const link = `https://t.me/${botInfo.username}?start=ref_${user.referralCode}`;
      const count = await User.countDocuments({ referredBy: ctx.from.id });

      await ctx.reply(
        `🤝 *Referral Program*\n\n` +
        `Share your link with friends.\n` +
        `When they subscribe, you earn *${REFERRAL_REWARD_DISCOUNT_PERCENT}% OFF* for your next premium purchase/renewal!\n\n` +
        `🔗 *Your Link:*\n\`${link}\`\n\n` +
        `👥 Friends Referred: *${count}*`,
        { parse_mode: 'Markdown' }
      );
    } catch (err) {
      logger.error(`my_referral error: ${err.message}`);
    }
  });

  bot.command('referral', async (ctx) => {
    const user = await findOrCreateUser(ctx.from);
    const botInfo = await bot.telegram.getMe();
    const link = `https://t.me/${botInfo.username}?start=ref_${user.referralCode}`;
    const count = await User.countDocuments({ referredBy: ctx.from.id });
    await ctx.reply(
      `🤝 *Your Referral Link*\n\n` +
      `Reward: *${REFERRAL_REWARD_DISCOUNT_PERCENT}% OFF* on next premium purchase/renewal\n\n` +
      `\`${link}\`\n\n👥 Referrals: *${count}*`,
      { parse_mode: 'Markdown' }
    );
  });

  const showSupportContact = async (ctx) => {
    await ctx.reply(
      `🎫 *Support*\n\n` +
      `Contact our team using the button below.`,
      {
        parse_mode: 'Markdown',
        ...Markup.inlineKeyboard([
          [withStyle(Markup.button.url('📩 Contact Support', SUPPORT_CONTACT_URL), 'primary')],
          [withStyle(Markup.button.callback('🏠 Main Menu', 'back_to_main'), 'success')],
        ]),
      }
    );
  };

  bot.action('open_support', async (ctx) => {
    await ctx.answerCbQuery();
    await showSupportContact(ctx);
  });

  bot.command('support', showSupportContact);

  // ── /cancel — hint for payment flow (no in-bot support tickets) ───────────
  bot.command('cancel', async (ctx) => {
    await ctx.reply(
      `ℹ️ *Screenshot upload* cancel karne ke liye us screen par *Cancel Upload* use karein.\n\n` +
      `Support ke liye niche button dabayein:`,
      {
        parse_mode: 'Markdown',
        ...Markup.inlineKeyboard([
          [withStyle(Markup.button.url('📩 Contact Support', SUPPORT_CONTACT_URL), 'primary')],
          [withStyle(Markup.button.callback('🏠 Main Menu', 'back_to_main'), 'success')],
        ]),
      }
    );
  });

  // ── Message handler: seller UPI, payment text nudge, DM word filters ─────
  bot.on('message', async (ctx, next) => {
    // Only process private messages (not group messages)
    if (ctx.chat.type !== 'private') return next();

    const userId = ctx.from.id;
    const message = ctx.message;
    const text = String(message?.text || message?.caption || '').trim();

    // Skip commands
    if (String(message?.text || '').startsWith('/')) return next();

    // While superadmin is composing broadcast payload, suppress other DM flows.
    if (isBroadcastSessionActive(userId)) return next();

    await User.findOneAndUpdate({ telegramId: userId }, { lastInteraction: new Date() }).catch(() => { });

    try {
      const user = await findOrCreateUser(ctx.from);

      // Check if user is awaiting support (about to create ticket)
      const userDoc = await User.findOne({ telegramId: userId });
      const flowState = getUserFlowState(userDoc);

      if (flowState === USER_FLOW_STATE.AWAITING_SELLER_UPI) {
        const handled = await handleSellerWithdrawalUpiMessage({
          ctx,
          userId,
          message,
          requestSellerWithdrawal,
          User,
          buildSetUserFlowUpdate,
          USER_FLOW_STATE,
          notifySellerWithdrawalRequest,
          bot,
        });
        if (handled) return;
      }

      if (flowState === USER_FLOW_STATE.AWAITING_PAYMENT_SCREENSHOT) {
        if (!message?.text) return next();
        return ctx.reply(
          `📸 Aap payment screenshot upload mode me hain.\n\n` +
          `Kripya screenshot as photo/document bhejiye ya cancel karein.`,
          {
            parse_mode: 'Markdown',
            ...Markup.inlineKeyboard([
              [withStyle(Markup.button.callback('❌ Cancel Upload', 'cancel_payment_upload'), 'danger')],
              [withStyle(Markup.button.callback('🏠 Main Menu', 'back_to_main'), 'primary')],
            ]),
          }
        );
      }

      // DM word filters (admin-configured auto-replies)
      if (message?.text) {
        const matchedFilter = await findMatchedDmFilter(text);
        if (matchedFilter) {
          const sent = await sendDmFilterResponse(ctx, matchedFilter);
          if (sent) return;
        }
      }

      return next();

    } catch (err) {
      logger.error(`user DM message handler error: ${err.message}`);
      await ctx.reply('❌ Error sending message. Please try again.');
    }
  });
};

module.exports = { registerUserHandlers };
