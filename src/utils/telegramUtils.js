// src/utils/telegramUtils.js
// Telegram API helpers: safe message sending with block detection, invite link generation

const logger = require('../utils/logger');
const DELETE_DELAY_MINUTES = Math.max(0, parseInt(process.env.MESSAGE_DELETE_DELAY_MINUTES || '30', 10));
const MAX_TIMEOUT_MS = 2147483647;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, Math.max(0, Number(ms) || 0)));

const getRetryAfterSeconds = (err) => {
  const fromParams = Number(err?.parameters?.retry_after || err?.response?.parameters?.retry_after || 0);
  if (Number.isFinite(fromParams) && fromParams > 0) return fromParams;

  const message = String(err?.description || err?.response?.description || err?.message || '');
  const parsed = message.match(/retry after\s+(\d+)/i);
  return parsed ? Number(parsed[1]) : 0;
};

/**
 * Safely send a message to a user.
 * If bot is blocked (403), log it as a delivery issue only.
 * Returns true on success, false on failure.
 */
const safeSend = async (bot, telegramId, text, extra = {}) => {
  try {
    const safeExtra = {
      ...extra,
      protect_content: typeof extra?.protect_content === 'undefined' ? true : extra.protect_content,
    };
    await bot.telegram.sendMessage(telegramId, text, safeExtra);
    return true;
  } catch (err) {
    // 403 = user blocked the bot
    if (err.code === 403 || (err.response && err.response.error_code === 403)) {
      await handleBlockedUser(bot, telegramId);
    } else {
      logger.warn(`safeSend to ${telegramId} failed: ${err.message}`);
    }
    return false;
  }
};

/**
 * Like safeSend(), but can also pin the sent message in the user's private chat.
 * Returns true on send success, false on failure (same behavior as safeSend()).
 */
const safeSendAndPin = async (bot, telegramId, text, extra = {}, { pin = false, disableNotification = true } = {}) => {
  try {
    const safeExtra = {
      ...extra,
      protect_content: typeof extra?.protect_content === 'undefined' ? true : extra.protect_content,
    };

    const sent = await bot.telegram.sendMessage(telegramId, text, safeExtra);

    if (pin && sent?.message_id) {
      try {
        await bot.telegram.pinChatMessage(telegramId, sent.message_id, { disable_notification: disableNotification });
      } catch (e) {
        // Pin failures shouldn't break the user flow (permissions / unsupported).
        logger.warn(`pinChatMessage failed for ${telegramId}: ${e?.message || e}`);
      }
    }

    return true;
  } catch (err) {
    // 403 = user blocked the bot
    if (err.code === 403 || (err.response && err.response.error_code === 403)) {
      await handleBlockedUser(bot, telegramId);
    } else {
      logger.warn(`safeSendAndPin to ${telegramId} failed: ${err.message}`);
    }
    return false;
  }
};

/**
 * Handle a 403 blocked-bot error
 */
const handleBlockedUser = async (_bot, telegramId) => {
  try {
    logger.warn(`User ${telegramId} has blocked the bot — message skipped without auto-ban`);
  } catch (e) {
    logger.error(`handleBlockedUser error: ${e.message}`);
  }
};

/**
 * Generate a single-use invite link.
 * - member_limit: 1
 * - expire_date: short TTL (default 10 min) to reduce misuse window
 * If maxValidTill is provided, link expiry won't exceed that date.
 */
const generateInviteLink = async (bot, groupId, userId, maxValidTill = null) => {
  const maxAttempts = Math.max(1, parseInt(process.env.INVITE_LINK_RETRY_ATTEMPTS || '3', 10));
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const ttlMinutes = Math.max(1, parseInt(process.env.INVITE_LINK_TTL_MINUTES || '10', 10));
      const nowUnix = Math.floor(Date.now() / 1000);
      const ttlExpiryUnix = nowUnix + (ttlMinutes * 60);

      let expireDateUnix = ttlExpiryUnix;
      if (maxValidTill) {
        const maxValidUnix = Math.floor(new Date(maxValidTill).getTime() / 1000);
        if (!Number.isNaN(maxValidUnix) && maxValidUnix > nowUnix) {
          expireDateUnix = Math.min(ttlExpiryUnix, maxValidUnix);
        }
      }

      const invite = await bot.telegram.createChatInviteLink(groupId, {
        name: `User_${userId}`,
        member_limit: 1,
        expire_date: expireDateUnix,
        creates_join_request: false,
      });
      return invite.invite_link;
    } catch (err) {
      const retryAfterSeconds = getRetryAfterSeconds(err);
      const isFloodError = Number(retryAfterSeconds) > 0;
      const shouldRetry = isFloodError && attempt < maxAttempts;

      if (shouldRetry) {
        const waitSeconds = Math.min(60, Math.max(1, retryAfterSeconds + 1));
        logger.warn(
          `generateInviteLink flood-control for user ${userId}; retrying in ${waitSeconds}s (attempt ${attempt}/${maxAttempts})`
        );
        await sleep(waitSeconds * 1000);
        continue;
      }

      logger.error(`generateInviteLink error for user ${userId}: ${err.message}`);
      return null;
    }
  }
  return null;
};

/**
 * Revoke a specific invite link immediately.
 */
const revokeInviteLink = async (bot, groupId, inviteLink) => {
  try {
    if (!inviteLink) return false;
    await bot.telegram.revokeChatInviteLink(groupId, inviteLink);
    return true;
  } catch (err) {
    logger.warn(`Could not revoke invite link: ${err.message}`);
    return false;
  }
};

/**
 * Check if a user is currently a member of the premium group
 */
const isGroupMember = async (bot, groupId, telegramId) => {
  try {
    const member = await bot.telegram.getChatMember(groupId, telegramId);
    return ['member', 'administrator', 'creator'].includes(member.status);
  } catch (_) {
    return false;
  }
};

/**
 * Ban a user from the premium group
 */
const banFromGroup = async (bot, groupId, telegramId) => {
  try {
    await bot.telegram.banChatMember(groupId, telegramId);
    logger.info(`User ${telegramId} banned from group ${groupId}`);
    return true;
  } catch (err) {
    logger.warn(`Could not ban ${telegramId}: ${err.message}`);
    return false;
  }
};

/**
 * Unban a user from the premium group
 */
const unbanFromGroup = async (bot, groupId, telegramId) => {
  try {
    await bot.telegram.unbanChatMember(groupId, telegramId, { only_if_banned: true });
    logger.info(`User ${telegramId} unbanned from group ${groupId}`);
    return true;
  } catch (err) {
    logger.warn(`Could not unban ${telegramId}: ${err.message}`);
    return false;
  }
};

/**
 * Renewal inline keyboard with plan buttons
 */
const renewalKeyboard = (plans, category = null) => {
  const safeCategory = category ? String(category).toLowerCase() : null;
  const buttons = plans.map(p => ([{
    text: `🔄 Renew ${p.durationDays} Days${p.price ? ` · ₹${p.price}` : ''}`,
    callback_data: safeCategory
      ? `renew_request_${safeCategory}_${p._id}`
      : `renew_request_${p._id}`,
  }]));
  return { inline_keyboard: buttons };
};

/**
 * Delete a message after configured delay (default: 30 minutes).
 * Uses setTimeout in-process; deletion is skipped if chat/message ids are missing.
 */
const scheduleDeleteMessage = (telegram, chatId, messageId, delayMinutes = DELETE_DELAY_MINUTES) => {
  if (!telegram || !chatId || !messageId) return;

  const safeMinutes = Math.max(0, Number(delayMinutes) || 0);
  const delayMs = Math.min(MAX_TIMEOUT_MS, safeMinutes * 60 * 1000);

  setTimeout(async () => {
    try {
      await telegram.deleteMessage(chatId, messageId);
    } catch (_) {
      // Best-effort cleanup only (message may already be gone or not deletable anymore).
    }
  }, delayMs);
};

module.exports = {
  safeSend,
  safeSendAndPin,
  handleBlockedUser,
  generateInviteLink,
  revokeInviteLink,
  isGroupMember,
  banFromGroup,
  unbanFromGroup,
  renewalKeyboard,
  scheduleDeleteMessage,
};
