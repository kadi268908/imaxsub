// src/index.js
// Main entry point — wires up DB, bot, cron jobs, and optional health check

require('dotenv').config({ override: true });
const { Telegraf } = require('telegraf');
const express = require('express');
const fs = require('fs');
const path = require('path');

const connectDB = require('./config/database');
const logger = require('./utils/logger');
const { registerUserHandlers } = require('./bot/handlers');
const { registerAdminHandlers } = require('./bot/adminHandlers');
const { registerSuperAdminHandlers } = require('./bot/superAdminHandlers');
const { initCronJobs } = require('./services/cronService');
const { revokeInviteLink } = require('./utils/telegramUtils');

let httpServer = null;

const getClientIp = (req) => {
  const xff = req.headers['x-forwarded-for'];
  if (typeof xff === 'string' && xff.length) return xff.split(',')[0].trim();
  return req.socket?.remoteAddress || req.ip || '';
};

const normalizeClientIp = (ip) => String(ip || '').replace(/^::ffff:/, '');

const healthTokenMatches = (req, token) => {
  const auth = req.headers.authorization;
  const q = req.query?.token;
  return auth === `Bearer ${token}` || q === token;
};

const healthIpAllowed = (req, rules) => {
  const ip = normalizeClientIp(getClientIp(req));
  return rules.some((rule) => {
    const r = String(rule || '').trim();
    if (!r) return false;
    return ip === r || ip === normalizeClientIp(r);
  });
};

/**
 * Optional protection for HTTP GET /health when PORT is set.
 * - HEALTH_CHECK_TOKEN: require Authorization: Bearer <token> or ?token=
 * - HEALTH_CHECK_ALLOWED_IPS: comma-separated IPs (use with TRUST_PROXY=true behind nginx)
 * - If both set: pass if token OR IP matches (set HEALTH_CHECK_REQUIRE_TOKEN_AND_IP=true to require both)
 */
const healthAuthMiddleware = (req, res, next) => {
  const token = process.env.HEALTH_CHECK_TOKEN;
  const allowedIps = String(process.env.HEALTH_CHECK_ALLOWED_IPS || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  const requireBoth = String(process.env.HEALTH_CHECK_REQUIRE_TOKEN_AND_IP || 'false').toLowerCase() === 'true';

  if (!token && !allowedIps.length) return next();

  const tOk = token ? healthTokenMatches(req, token) : false;
  const iOk = allowedIps.length ? healthIpAllowed(req, allowedIps) : false;

  let ok = false;
  if (token && allowedIps.length) {
    ok = requireBoth ? tOk && iOk : tOk || iOk;
  } else if (token) {
    ok = tOk;
  } else {
    ok = iOk;
  }

  if (!ok) return res.status(403).json({ error: 'Forbidden' });
  return next();
};

const startHttpServer = () => new Promise((resolve, reject) => {
  if (!process.env.PORT) return resolve(null);

  if (
    String(process.env.NODE_ENV || '').toLowerCase() === 'production'
    && !process.env.HEALTH_CHECK_TOKEN
    && !process.env.HEALTH_CHECK_ALLOWED_IPS
  ) {
    logger.warn(
      'HTTP /health is reachable without auth. Set HEALTH_CHECK_TOKEN and/or HEALTH_CHECK_ALLOWED_IPS in production.'
    );
  }

  const app = express();
  if (String(process.env.TRUST_PROXY || 'false').toLowerCase() === 'true') {
    app.set('trust proxy', 1);
  }

  app.get('/health', healthAuthMiddleware, (req, res) => {
    res.json({
      status: 'ok',
      uptime: process.uptime(),
      pid: process.pid,
      nodeEnv: process.env.NODE_ENV || 'development',
    });
  });

  const server = app.listen(Number(process.env.PORT), () => {
    logger.info(`HTTP health listening on port ${process.env.PORT}`);
    resolve(server);
  });
  server.on('error', reject);
});

const gracefulShutdown = async (signal) => {
  logger.info(`Shutting down (${signal})...`);
  try {
    if (httpServer) {
      await new Promise((resolve, reject) => {
        httpServer.close((err) => (err ? reject(err) : resolve()));
      });
      httpServer = null;
    }
  } catch (e) {
    logger.warn(`HTTP server close: ${e.message}`);
  }
  try {
    await bot.stop(signal);
  } catch (_) { /* ignore */ }
  process.exit(0);
};

const parseSuperAdminIds = () => {
  return String(process.env.SUPER_ADMIN_IDS || process.env.SUPER_ADMIN_ID || '')
    .split(',')
    .map(id => parseInt(id.trim(), 10))
    .filter(Boolean);
};

const isLikelyTelegramGroupId = (value) => /^-?\d+$/.test(String(value || '').trim());

const isMarkdownParseEntityError = (err) => {
  const message = err?.response?.description || err?.description || err?.message || '';
  return String(message).toLowerCase().includes("can't parse entities");
};

const extractEntityErrorOffset = (message) => {
  const match = String(message || '').match(/byte offset\s+(\d+)/i);
  return match ? Number(match[1]) : null;
};

const getUpdateDebugPreview = (ctx) => {
  const messageText = String(ctx.message?.text || ctx.message?.caption || '').trim();
  const callbackData = String(ctx.callbackQuery?.data || '').trim();
  const inlineText = String(ctx.callbackQuery?.message?.text || ctx.callbackQuery?.message?.caption || '').trim();
  const source = messageText || callbackData || inlineText;
  return source.replace(/\s+/g, ' ').slice(0, 220);
};

const normalizeLeadingBackslashCommand = (message) => {
  const text = String(message?.text || '');
  if (!text.startsWith('\\')) return;

  // Convert "\command args" into "/command args" for users who type backslash commands.
  const converted = `/${text.slice(1)}`;
  message.text = converted;

  const firstToken = converted.trim().split(/\s+/, 1)[0] || '';
  const commandLength = firstToken.length;
  if (!commandLength) return;

  const entities = Array.isArray(message.entities) ? [...message.entities] : [];
  if (!entities.some((entity) => entity?.offset === 0 && entity?.type === 'bot_command')) {
    entities.unshift({
      offset: 0,
      length: commandLength,
      type: 'bot_command',
    });
  }
  message.entities = entities;
};

const validatePremiumGroupConfigAtStartup = () => {
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
      errors.push(`${category} group ID looks invalid: ${groupId}`);
    }
  });
  if (fallback && !isLikelyTelegramGroupId(fallback)) {
    errors.push(`fallback PREMIUM_GROUP_ID looks invalid: ${fallback}`);
  }

  const configuredPairs = Object.entries(categoryMap).filter(([, groupId]) => Boolean(groupId));
  const reverse = new Map();
  configuredPairs.forEach(([category, groupId]) => {
    const key = String(groupId);
    const existing = reverse.get(key) || [];
    existing.push(category);
    reverse.set(key, existing);
  });
  for (const [groupId, categories] of reverse.entries()) {
    if (categories.length > 1) {
      errors.push(`categories share same group ID ${groupId}: ${categories.join(', ')}`);
    }
  }

  const missingCategories = Object.entries(categoryMap)
    .filter(([, groupId]) => !groupId)
    .map(([category]) => category);
  if (missingCategories.length && fallback) {
    warnings.push(`fallback PREMIUM_GROUP_ID will be used for missing categories: ${missingCategories.join(', ')}`);
  }
  if (missingCategories.length && !fallback) {
    warnings.push(`missing category group IDs without fallback: ${missingCategories.join(', ')}`);
  }

  return { warnings, errors };
};

// Ensure logs directory exists
const logsDir = path.join(process.cwd(), 'logs');
if (!fs.existsSync(logsDir)) fs.mkdirSync(logsDir, { recursive: true });

// Validate required env vars
const required = ['BOT_TOKEN', 'MONGO_URI', 'LOG_CHANNEL_ID'];
for (const key of required) {
  if (!process.env[key]) {
    logger.error(`Missing required environment variable: ${key}`);
    process.exit(1);
  }
}

const configuredPremiumGroups = [
  process.env.MOVIE_PREMIUM_GROUP_ID,
  process.env.DESI_PREMIUM_GROUP_ID,
  process.env.NON_DESI_PREMIUM_GROUP_ID,
  process.env.PREMIUM_GROUP_ID,
].filter(Boolean);

if (!configuredPremiumGroups.length) {
  logger.error('Missing premium group configuration. Set at least one of MOVIE_PREMIUM_GROUP_ID, DESI_PREMIUM_GROUP_ID, NON_DESI_PREMIUM_GROUP_ID, or PREMIUM_GROUP_ID.');
  process.exit(1);
}

const premiumGroupConfigHealth = validatePremiumGroupConfigAtStartup();
for (const line of premiumGroupConfigHealth.warnings) {
  logger.warn(`Premium group config: ${line}`);
}
for (const line of premiumGroupConfigHealth.errors) {
  logger.error(`Premium group config: ${line}`);
}
if (premiumGroupConfigHealth.errors.length) {
  process.exit(1);
}

const superAdminIds = parseSuperAdminIds();
if (!superAdminIds.length) {
  logger.error('Missing SUPER_ADMIN_IDS or SUPER_ADMIN_ID. Configure at least one super admin Telegram ID.');
  process.exit(1);
}

const bot = new Telegraf(process.env.BOT_TOKEN);

const protectBotMessages = String(process.env.PROTECT_BOT_MESSAGES || 'true').toLowerCase() !== 'false';
const protectedMethods = new Set([
  'sendMessage',
  'sendPhoto',
  'sendVideo',
  'sendAudio',
  'sendDocument',
  'sendVoice',
  'sendAnimation',
  'sendVideoNote',
  'sendSticker',
  'sendMediaGroup',
]);

const originalCallApi = bot.telegram.callApi.bind(bot.telegram);
bot.telegram.callApi = (method, payload, ...rest) => {
  const safePayload = payload && typeof payload === 'object' ? { ...payload } : payload;

  if (
    protectBotMessages &&
    safePayload &&
    protectedMethods.has(method) &&
    typeof safePayload.protect_content === 'undefined'
  ) {
    safePayload.protect_content = true;
  }

  return originalCallApi(method, safePayload, ...rest).catch((err) => {
    const hasFormattingPayload = Boolean(
      safePayload?.parse_mode === 'Markdown'
      || safePayload?.parse_mode === 'MarkdownV2'
      || Array.isArray(safePayload?.entities)
      || Array.isArray(safePayload?.caption_entities)
    );
    if (!hasFormattingPayload || !isMarkdownParseEntityError(err)) {
      throw err;
    }

    const previewRaw = typeof safePayload?.text === 'string'
      ? safePayload.text
      : (typeof safePayload?.caption === 'string' ? safePayload.caption : '');
    const preview = previewRaw.replace(/\s+/g, ' ').slice(0, 160);

    logger.warn(
      `Telegram Markdown parse fallback on ${method}; retrying without parse_mode. Preview: ${preview}`
    );

    const fallbackPayload = { ...safePayload };
    delete fallbackPayload.parse_mode;
    delete fallbackPayload.entities;
    delete fallbackPayload.caption_entities;

    return originalCallApi(method, fallbackPayload, ...rest);
  });
};

if (protectBotMessages) {
  logger.info('Bot content protection enabled (anti-forward on supported clients).');
}

// Global error handler — prevents crashes on unexpected Telegram errors
bot.catch((err, ctx) => {
  const message = err?.response?.description || err?.description || err?.message || '';
  if (String(message).toLowerCase().includes('message is not modified')) {
    return;
  }

  if (isMarkdownParseEntityError(err)) {
    const offset = extractEntityErrorOffset(message);
    const preview = getUpdateDebugPreview(ctx);
    const callbackData = String(ctx.callbackQuery?.data || '');
    logger.error(
      `Parse entities debug: updateType=${ctx.updateType}; offset=${offset ?? 'n/a'}; callback=${callbackData || 'n/a'}; preview=${preview || 'n/a'}`
    );
  }

  logger.error(`Bot error [${ctx.updateType}]: ${err.message}`);
  if (ctx.reply) {
    ctx.reply('❌ An unexpected error occurred. Please try again.').catch(() => { });
  }
});

// Revoke invite link immediately after first successful join (single-use hardening)
bot.on('chat_member', async (ctx, next) => {
  try {
    const update = ctx.update?.chat_member;
    const oldStatus = update?.old_chat_member?.status;
    const newStatus = update?.new_chat_member?.status;
    const invite = update?.invite_link;
    const joinedNow = ['member', 'administrator', 'creator'].includes(newStatus)
      && ['left', 'kicked'].includes(oldStatus);

    if (joinedNow && invite?.invite_link) {
      const linkName = String(invite.name || '');
      if (linkName.startsWith('User_')) {
        await revokeInviteLink(bot, update.chat.id, invite.invite_link);
      }
    }
  } catch (err) {
    logger.warn(`chat_member invite revoke handler error: ${err.message}`);
  }

  return next();
});

// Global guard: blocked users cannot use the bot
bot.use(async (ctx, next) => {
  if (ctx.updateType === 'message' && ctx.message?.text) {
    normalizeLeadingBackslashCommand(ctx.message);
  }

  const telegramId = ctx.from?.id;
  if (!telegramId) return next();

  const User = require('./models/User');
  const user = await User.findOne({ telegramId });
  if (!user?.isBlocked) return next();

  const supportContact = process.env.SUPPORT_CONTACT || '@ImaxSupport1Bot';
  const blockedMsg = `⛔ *You have been banned from using this bot.*\n\nPlease contact support for this issue: ${supportContact}`;

  if (ctx.callbackQuery) {
    await ctx.answerCbQuery('You are banned. Contact support.', { show_alert: true }).catch(() => { });
  }

  if (ctx.reply) {
    await ctx.reply(blockedMsg, { parse_mode: 'Markdown' }).catch(() => { });
  }
});

// Register all handler layers
registerUserHandlers(bot);
registerAdminHandlers(bot);
registerSuperAdminHandlers(bot);

// ── /help ──────────────────────────────────────────────────────────────────
bot.command('help', async (ctx) => {
  const User = require('./models/User');
  const user = await User.findOne({ telegramId: ctx.from.id });
  const role = user?.role || 'user';

  // Legacy Markdown: escape _ in SUPPORT_CONTACT and \\[ in [...] patterns (else Telegram parses italic/links).
  let msg = `🤖 *Bot Commands*\n\n`;
  msg += `*User:*\n/start — Main menu\n/menu — Quick main menu\n/status — Subscription status\n/offers — View current offers\n/referral — Your referral link\n/seller — Seller program dashboard\n/support — Connect with support\n`;

  if (['admin', 'superadmin'].includes(role)) {
    msg += `\n*Admin:*\n/user <id> — User search panel\n/history <id> — Subscription / purchase history\n/diagnose <id> — Incident diagnostics snapshot\n/repair <id> — Auto-repair user invite/request state\n/ban <id> — Ban user from bot\n/unban <id> — Restore bot access\n/invite <id> — Send fresh join link / reset pending request\n/offeruser <id>|<discount>|\\[planId\\] — One-time private offer (today only)\n/revokeplan <id> — Terminate subscription + remove from group\n/modifyplan <id>|<plan>|\\[category\\]|\\[reset\\] — Fix plan (keeps expiry; add \\|reset to restart from today)\n/expiries \\[today|0|1|3|7\\] — Check upcoming expiry users\n/plans — Active plans\n`;
  }
  if (role === 'superadmin') {
    msg += `\n*Super Admin:*\n/addadmin <id> /removeadmin <id> /admins\n` +
      `/createplan /editplan /deleteplan /pauseplan /resumeplan /listplans\n` +
      `/addoffer /createoffer /deleteoffer /listoffers\n` +
      `/removeprivateoffers all|<id> — Delete private user offers\n` +
      `/listprivateoffers — List non-redeemed private offers\n` +
      `/reset [all|requests|withdrawals] — Reset pending user joins/withdrawals\n` +
      `/broadcast — Broadcast to users\n` +
      `/bcancel — Cancel active broadcast compose session\n` +
      `/report <Nd|Nm> — Custom CSV report (e.g. 7d, 28d, 1m)\n` +
      `/sellerpayoutscsv \\[Nd|Nm|all\\] — Seller payout CSV export\n` +
      `/sellerwithdrawalscsv \\[Nd|Nm|all\\] \\[status\\] — Withdrawals-only CSV export\n` +
      `/sellerstats \\[limit\\] — Seller list (ID, referrals, balance)\n` +
      `/referralstats \\[limit\\] — User referral leaderboard\n` +
      `/health — Runtime snapshot (DB/bot/cron)\n` +
      `/reports — Sales reports\n` +
      `/stats — Growth dashboard\n` +
      `/categorystats — Category-wise CSV snapshot\n` +
      `/planstats — Plan performance\n` +
      `/adminlogs — Audit log\n` +
      `/sellerwithdrawals /approvesellerwd /rejectsellerwd — Seller payouts\n` +
      `/revokeseller <id> — Revoke seller access\n` +
      `/resetsellerid <id> — Set seller referral id to 0\n` +
      `/configcheck — Validate category group configuration\n` +
      `/cleanupdryrun /cleanuprun /cleanupreport — Retention cleanup tools\n`;
  }

  await ctx.reply(msg, { parse_mode: 'Markdown' });
});

// ── Startup ────────────────────────────────────────────────────────────────
const start = async () => {
  try {
    await connectDB();

    httpServer = await startHttpServer();

    // Sync superadmin roles from env on every startup.
    // This prevents removed test IDs from retaining superadmin access in DB.
    const User = require('./models/User');
    const configuredSuperAdminSet = new Set(superAdminIds.map((id) => Number(id)));

    // Promote configured IDs to superadmin (and create if missing).
    for (const id of configuredSuperAdminSet) {
      await User.findOneAndUpdate(
        { telegramId: id },
        {
          $set: {
            role: 'superadmin',
            status: 'active',
          },
          $setOnInsert: {
            telegramId: id,
            name: 'Super Admin',
          },
        },
        { upsert: true }
      );
    }

    // Demote IDs that are no longer configured in env.
    const demotionResult = await User.updateMany(
      {
        role: 'superadmin',
        telegramId: { $nin: Array.from(configuredSuperAdminSet) },
      },
      {
        $set: { role: 'user' },
      }
    );

    if (demotionResult.modifiedCount > 0) {
      logger.info(`Superadmin role sync: demoted ${demotionResult.modifiedCount} stale superadmin account(s).`);
    }

    initCronJobs(bot);

    await bot.launch();
    logger.info('🤖 Telegram Subscription Bot v2.0 started!');

    process.once('SIGINT', () => { gracefulShutdown('SIGINT'); });
    process.once('SIGTERM', () => { gracefulShutdown('SIGTERM'); });

  } catch (error) {
    logger.error(`Startup failed: ${error.message}`);
    process.exit(1);
  }
};

start();
