# Telegram Subscription Bot v2.0

Production-ready Telegram bot for paid premium access management with category-wise plans, request approval workflows, renewals, referrals, seller commissions, external support link, anti-fraud controls, analytics, and scheduled automations.

## Overview

This bot is built for Telegram premium communities where users:

- View category-specific plans
- Pay externally and submit screenshot proof
- Get approved by admin/superadmin
- Receive single-use invite links
- Renew plans before or after expiry

The project includes:

- User, admin, and superadmin command layers
- Seller referral and withdrawal workflow
- External support via `SUPPORT_CONTACT` (Telegram @username or `https://` URL)
- Cron jobs for reminders, expiry enforcement, membership monitoring, and summaries
- MongoDB-backed auditing and analytics

## Tech Stack

- Runtime: Node.js
- Bot framework: `telegraf`
- Database: MongoDB + `mongoose`
- Scheduling: `node-cron`
- HTTP health endpoint: `express`
- Logging: `winston`

## Key Features

- Category-based plan system: `movie`, `desi`, `non_desi`
- Screenshot-based payment proof submission
- Admin approval/rejection in log channel via inline buttons
- Renewal flow with category-aware checks
- Referral rewards as one-time private offers
- Seller program with commission ledger and withdrawals
- Invite-link hardening: single-use behavior and expiry handling
- Expiry enforcement (no grace period): expired users are removed from premium groups
- `/support` opens your support link (no in-bot ticket / forum topics)
- Audit logging of sensitive admin/system actions
- CSV exports for reports and payout data

## Project Structure

```text
src/
  index.js
  bot/
    handlers.js
    paymentFlow.js
    sellerFlow.js
    adminHandlers.js
    superAdminHandlers.js
  config/
    database.js
  models/
    User.js
    Plan.js
    Subscription.js
    Request.js
    Offer.js
    UserOffer.js
    DailySummary.js
    AdminLog.js
    SellerWithdrawalRequest.js
    SellerPayoutLedger.js
    DmWordFilter.js
  services/
    userService.js
    adminService.js
    subscriptionService.js
    referralService.js
    supportService.js
    analyticsService.js
    cronService.js
  utils/
    mongoErrors.js
    telegramUtils.js
    premiumGroups.js
    dateUtils.js
    userFlowState.js
    logger.js
    importLegacyUsersCsv.js
    backfillSubscriptionCategories.js
    resetDatabase.js
assets/
logs/
```

## Quick Start

### 1. Install

```bash
npm install
```

### 2. Configure environment

```bash
cp .env.example .env
```

Fill required values in `.env`.

### 3. Start bot

```bash
npm start
```

For development:

```bash
npm run dev
```

## NPM Scripts

- `npm start`: run production entry (`src/index.js`)
- `npm run dev`: run with nodemon
- `npm run verify`: syntax-check all `src/**/*.js` (cross-platform)
- `npm run import:legacy-csv -- --file <csv> --plan <planIdOrDays> --expiry <DD/MM/YYYY> [--skipGroupCheck true]`
- `npm run migrate:subscription-categories -- [--apply] [--sync-group]`

## Required Telegram Setup

### Bot permissions in premium groups

Grant bot admin rights to:

- Create invite links
- Restrict/ban users

### Bot permissions in log channel

Grant bot admin rights to:

- Send/edit messages
- Send documents

## Environment Variables

### Required

| Variable | Description |
|---|---|
| `BOT_TOKEN` | Telegram bot token from BotFather |
| `MONGO_URI` | MongoDB connection URI |
| `LOG_CHANNEL_ID` | Telegram channel/group ID for moderation logs |
| `SUPER_ADMIN_IDS` or `SUPER_ADMIN_ID` | Superadmin Telegram IDs |
| One premium group ID | At least one of `MOVIE_PREMIUM_GROUP_ID`, `DESI_PREMIUM_GROUP_ID`, `NON_DESI_PREMIUM_GROUP_ID`, `PREMIUM_GROUP_ID` |

### Premium group routing

| Variable | Purpose |
|---|---|
| `MOVIE_PREMIUM_GROUP_ID` | Movie category premium group |
| `DESI_PREMIUM_GROUP_ID` | Desi category premium group |
| `NON_DESI_PREMIUM_GROUP_ID` | Non-Desi category premium group |
| `PREMIUM_GROUP_ID` | General fallback premium group |

### Support and runtime

| Variable | Default | Purpose |
|---|---|---|
| `SUPPORT_CONTACT` | `@ImaxSupport1Bot` | Support link: `@username` or full `https://` URL; used by `/support` and buttons |
| `NODE_ENV` | `production` | Runtime mode |
| `PORT` | unset | Enables `/health` endpoint when set |
| `PROTECT_BOT_MESSAGES` | `true` | Adds anti-forward protection on outgoing bot media/messages |

### HTTP `/health` security (when `PORT` is set)

| Variable | Default | Purpose |
|---|---|---|
| `HEALTH_CHECK_TOKEN` | unset | If set, require `Authorization: Bearer <token>` or `GET /health?token=` |
| `HEALTH_CHECK_ALLOWED_IPS` | unset | Comma-separated IPs allowed to call `/health` |
| `HEALTH_CHECK_REQUIRE_TOKEN_AND_IP` | `false` | If `true` and both token + IPs are set, require **both**; otherwise token **or** IP is enough |
| `TRUST_PROXY` | `false` | Set `true` behind nginx/reverse proxy so `HEALTH_CHECK_ALLOWED_IPS` uses real client IP |

In production, set `HEALTH_CHECK_TOKEN` and/or `HEALTH_CHECK_ALLOWED_IPS`. The app logs a warning if neither is set while `NODE_ENV=production`.

### Daily backup privacy (log channel JSON)

| Variable | Default | Purpose |
|---|---|---|
| `DAILY_BACKUP_INCLUDE_USER_IDS` | `false` | If `false`, nightly JSON backup strips Telegram IDs, usernames, names, and payment proof file IDs. Set `true` only on a **fully trusted** log channel / compliance need. |

### Subscription, referral, seller

| Variable | Default | Purpose |
|---|---|---|
| `INVITE_LINK_TTL_MINUTES` | `10` | Invite link validity |
| `REFERRAL_REWARD_DISCOUNT_PERCENT` | `10` | One-time referral reward discount |
| `REJOINING_PENALTY` | `20` | Penalty displayed to users if active but left group |
| `SELLER_COMMISSION_PERCENT` | `15` | Seller commission percentage |
| `SELLER_MIN_WITHDRAW_REFERRALS` | `10` | Withdrawal eligibility threshold |
| `SELLER_MIN_WITHDRAW_BALANCE` | `200` | Withdrawal eligibility threshold |
| `SELLER_WITHDRAW_MIN_PROCESS_HOURS` | `24` | Hours after request before **Approve** is allowed (`0` = immediate) |
| `SELLER_WITHDRAW_ALLOW_EARLY_APPROVAL` | _(unset)_ | If `true`, super admin can approve **before** the minimum wait (optional override) |
| `SELLER_PAYOUT_HISTORY_LIMIT` | `10` | Max payout history rows shown in user view |

### Anti-fraud and reminders

| Variable | Default | Purpose |
|---|---|---|
| `PAYMENT_PROOF_SPAM_WINDOW_MINUTES` | `10` | Window for screenshot spam checks |
| `PAYMENT_PROOF_MAX_ATTEMPTS` | `5` | Max attempts in spam window |
| `PAYMENT_PROOF_COOLDOWN_MINUTES` | `30` | Cooldown duration after spam threshold |
| `PENDING_REQUEST_REMINDER_AFTER_HOURS` | `2` | First pending reminder delay |
| `PENDING_REQUEST_REMINDER_REPEAT_HOURS` | `12` | Reminder repeat interval |

### Cron customization

| Variable | Default |
|---|---|
| `CRON_TIMEZONE` | `Asia/Kolkata` |
| `REMINDER_CRON_SCHEDULES` | `15 9 * * *,0 20 * * *,15 23 * * *` |
| `EXPIRY_ENFORCEMENT_CRON_SCHEDULES` | `0 8 * * *,0 14 * * *,30 20 * * *` |

### Mongo DNS/retry tuning

| Variable | Default | Purpose |
|---|---|---|
| `MONGO_DNS_SERVERS` | unset | Custom DNS servers for SRV lookup fallback |
| `MONGO_CONNECT_RETRY_BASE_MS` | `5000` | Reconnect backoff base |
| `MONGO_CONNECT_RETRY_MAX_MS` | `60000` | Reconnect backoff cap |
| `MONGO_CONNECT_RETRY_JITTER_MS` | `1500` | Random jitter in reconnect delay |

## Command Reference

## User Commands

- `/start`: initialize profile, parse referral payload, open main menu
- `/menu`: show main menu
- `/status`: active subscription status and renewal actions
- `/offers`: show private and public offers
- `/referral`: show referral link and referral count
- `/seller`: seller dashboard
- `/sellerwithdraw`: start seller withdrawal UPI flow
- `/sellerpayouts`: seller payout/withdraw history
- `/support`: show button to open `SUPPORT_CONTACT` (external support)
- `/cancel`: hint for cancelling payment upload + support link
- `/help`: role-aware command list

## Admin Commands

- `/user <telegramId>`
- `/history <telegramId>` — subscription / purchase history (latest records, splits long output)
- `/ban <telegramId>`
- `/unban <telegramId>`
- `/invite <telegramId> [category]`
- `/offeruser <telegramId>|<discount>`
- `/revokeplan <telegramId> [category]`
- `/modifyplan <telegramId>|<planIdOrDays>|[category]`
- `/legacyadd <planIdOrDays>|<DD/MM/YYYY>|<id1,id2,...>`
- `/expiries [today|0|1|3|7]`
- `/plans`
- `/filter "Any Word"`
- `/unfilter "Any Word"`
- `/filters`

## Superadmin Commands

- `/addadmin <id>`, `/removeadmin <id>`, `/admins`
- `/createplan`, `/editplan`, `/deleteplan`, `/pauseplan`, `/resumeplan`, `/listplans`
- `/addoffer`, `/deleteoffer`, `/listoffers`
- `/broadcast`, `/bcancel`
- `/reports`
- `/report <Nd|Nm>`
- `/sellerpayoutscsv [Nd|Nm|all]`
- `/sellerwithdrawalscsv [Nd|Nm|all] [pending|approved|rejected|all]`
- `/stats`, `/categorystats`, `/planstats`
- `/health`
- `/sellerstats [limit]`
- `/referralstats [limit]`
- `/sellerwithdrawals`, `/approvesellerwd`, `/rejectsellerwd`
- `/revokeseller <id>`
- `/adminlogs`

## Core Flows

## 1. New premium request flow

- User picks category under `Check Plans`
- User uploads payment screenshot/photo
- Bot creates `Request` (`pending`) with payment proof metadata
- Bot posts approval card in `LOG_CHANNEL_ID` with inline Approve/Reject
- Admin approves with plan selection
- Bot creates/updates subscription and sends invite button to user

## 2. Renewal flow

- Renewal is category-aware and requires active sub in same category
- User picks renewal plan, uploads proof
- Request goes to admin queue
- On approval, subscription expiry is extended

## 3. Expiry and reminders

- Reminder checkpoints: 7/3/1/0 days
- No grace-period mode in v2.0 current code
- Expired active subs are marked `expired` and removed from group on next enforcement run

## 4. Referral rewards and offers

- Referral reward creates one-time `UserOffer`
- Coupon is consumed once when request/renewal is submitted
- Multiple successful referrals can create multiple one-time coupons
- One request consumes one available coupon (oldest first)

## 5. Seller program

- Seller referral attribution through `start=seller_<code>`
- Commission credited when referred user completes first paid subscription
- Ledger entries tracked in `SellerPayoutLedger`
- Withdrawals tracked in `SellerWithdrawalRequest`
- New withdrawal requests are posted to **`LOG_CHANNEL_ID`** with inline Approve/Reject — if this env var is missing, the user still gets “submitted” but **admins are not notified** (check server logs for a warning)
- **Approve/reject** (inline buttons and `/approvesellerwd` / `/rejectsellerwd`) is **super admin only** (`SUPER_ADMIN_IDS`)
- One pending withdrawal per seller is enforced by DB index

## 6. Support (external only)

- Set `SUPPORT_CONTACT` to your support bot `@username` or a full `https://` URL
- `/support` and inline **Contact Support** buttons open that link
- No forum group, topics, or in-bot ticket threads

## Cron Jobs

Configured in `src/services/cronService.js`.

| Job | Schedule | Purpose |
|---|---|---|
| `reminderScheduler` | `REMINDER_CRON_SCHEDULES` | Pre-expiry reminder notifications |
| `expiryEnforcementHandler` | `EXPIRY_ENFORCEMENT_CRON_SCHEDULES` | Mark expired + remove from premium group |
| `inactiveUserDetector` | `0 10 * * *` | Re-engagement messages |
| `membershipMonitor` | `0 11 * * *` | Membership consistency checks |
| `dailySummaryJob` | `59 23 * * *` | Summary + JSON backup to log channel |
| `offerExpiryChecker` | `5 0 * * *` | Deactivate expired offers |
| `inviteLinkExpiryNotifier` | `*/15 * * * *` | Expired invite link nudges |
| `pendingRequestReminderJob` | `0 */2 * * *` | Pending request follow-up reminders |

## Data Model Summary

- `User`: role/status, referral/seller info, activity metadata
- `Plan`: plan duration, price, category, active flag
- `Subscription`: lifecycle, expiry, reminders, invite-link tracking
- `Request`: approval queue, proof, selected plan, applied user offer snapshot
- `Offer`: public campaign offers
- `UserOffer`: private one-time offer coupons
- `DailySummary`: aggregated daily counters
- `AdminLog`: auditable admin/system actions
- `SellerWithdrawalRequest`: seller cash-out workflow
- `SellerPayoutLedger`: seller credit/debit ledger

## Utility Scripts

## Import legacy active users from CSV

```bash
npm run import:legacy-csv -- \
  --file ./path/to/users.csv \
  --plan <planIdOrDays> \
  --expiry <DD/MM/YYYY> \
  [--skipGroupCheck true]
```

Notes:

- Requires `BOT_TOKEN` and `PREMIUM_GROUP_ID`
- CSV accepts first column as Telegram ID or a recognized ID header

## Backfill subscription categories/groups

```bash
npm run migrate:subscription-categories -- --apply
npm run migrate:subscription-categories -- --apply --sync-group
```

Modes:

- default: dry-run
- `--apply`: write changes
- `--sync-group`: force category group ID sync (not just fill missing)

## Full database reset (danger)

```bash
node src/utils/resetDatabase.js
```

This deletes all documents from all collections in connected DB.

## Deployment

### PM2 example

```bash
npm install -g pm2
# Option A — from repo root (uses ecosystem.config.cjs)
pm2 start ecosystem.config.cjs
# Option B — one-liner
pm2 start src/index.js --name telegram-sub-bot
pm2 save
pm2 startup
```

### PM2 on Windows (wmic / pidusage noise)

PM2 uses the `pidusage` package to show **CPU and memory** in `pm2 list` / `pm2 monit`. On Windows it may call **`wmic`**, which is **deprecated** and often **missing** on newer Windows (or not on `PATH`). You may see:

- `Error caught while calling pidusage` / `spawn wmic ENOENT`
- `memory` showing as `0b` in `pm2 list`

**This does not stop your bot** — only live stats collection fails. Your app logs (e.g. access requests, rejections) are unrelated.

**What you can do:**

1. **Upgrade PM2** (often pulls a newer `pidusage`): `npm install -g pm2@latest` then `pm2 update`.
2. **Ignore the noise** if the process stays `online` and the bot works.
3. **Production:** run the bot on **Linux** or **WSL2** where PM2 monitoring works reliably.
4. On older Windows where `wmic.exe` still exists, ensure **`C:\Windows\System32\wbem`** is on the system **PATH** (so `wmic` can be found).

## Troubleshooting

- PM2 `wmic` / `pidusage` errors on Windows: see **Deployment → PM2 on Windows** above (not a bot bug).
- Mongo SRV/DNS errors: set `MONGO_DNS_SERVERS=8.8.8.8,1.1.1.1`
- Bot cannot approve/invite/ban: verify bot admin rights in premium groups
- Support: set `SUPPORT_CONTACT` to a valid `@username` or `https://` URL
- Missing reminders: verify cron timezone and schedule vars

## License

MIT
