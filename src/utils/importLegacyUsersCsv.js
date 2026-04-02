// src/utils/importLegacyUsersCsv.js
// CLI utility: import legacy active users from CSV into User + Subscription collections

require('dotenv').config({ override: true });

const fs = require('fs');
const path = require('path');
const mongoose = require('mongoose');
const { Telegraf } = require('telegraf');

const connectDB = require('../config/database');
const User = require('../models/User');
const Plan = require('../models/Plan');
const Subscription = require('../models/Subscription');

const parseArgs = () => {
    const raw = process.argv.slice(2);
    const args = {};

    for (let i = 0; i < raw.length; i += 1) {
        const token = raw[i];
        if (!token.startsWith('--')) continue;

        const key = token.slice(2);
        const next = raw[i + 1];
        if (!next || next.startsWith('--')) {
            args[key] = 'true';
        } else {
            args[key] = next;
            i += 1;
        }
    }

    return args;
};

const parseExpiryDate = (value) => {
    const [d, m, y] = String(value || '').split('/').map(v => parseInt(v, 10));
    if (!d || !m || !y) return null;
    const date = new Date(y, m - 1, d, 23, 59, 59, 999);
    if (Number.isNaN(date.getTime())) return null;
    return date;
};

const parseCsvLine = (line) => {
    const cols = [];
    let current = '';
    let inQuotes = false;

    for (let i = 0; i < line.length; i += 1) {
        const char = line[i];
        const next = line[i + 1];

        if (char === '"' && inQuotes && next === '"') {
            current += '"';
            i += 1;
            continue;
        }

        if (char === '"') {
            inQuotes = !inQuotes;
            continue;
        }

        if (char === ',' && !inQuotes) {
            cols.push(current.trim());
            current = '';
            continue;
        }

        current += char;
    }

    cols.push(current.trim());
    return cols;
};

const parseCsvIds = (csvText) => {
    const lines = csvText
        .split(/\r?\n/)
        .map(l => l.trim())
        .filter(Boolean);

    if (!lines.length) return [];

    const firstRow = parseCsvLine(lines[0]);
    const headerIndex = firstRow.findIndex(v => /^(telegramid|telegram_id|userid|user_id|id)$/i.test(v));
    const dataStart = headerIndex >= 0 ? 1 : 0;

    const out = [];
    for (let i = dataStart; i < lines.length; i += 1) {
        const cols = parseCsvLine(lines[i]);
        const rawId = headerIndex >= 0 ? cols[headerIndex] : cols[0];
        const id = parseInt(rawId, 10);
        if (id) out.push(id);
    }

    return [...new Set(out)];
};

const resolvePlan = async (planArg) => {
    let plan = await Plan.findById(planArg).catch(() => null);
    if (plan) return plan;

    const days = parseInt(planArg, 10);
    if (!days) return null;

    plan = await Plan.findOne({ durationDays: days, isActive: true });
    if (plan) return plan;

    return Plan.create({
        name: `${days} Days Plan`,
        durationDays: days,
        price: 0,
        isActive: true,
    });
};

const isMemberOfPremiumGroup = async (bot, telegramId) => {
    try {
        const groupId = process.env.PREMIUM_GROUP_ID;
        if (!groupId) return false;
        const member = await bot.telegram.getChatMember(groupId, telegramId);
        return ['member', 'administrator', 'creator'].includes(member.status);
    } catch (_) {
        return false;
    }
};

const main = async () => {
    const args = parseArgs();
    const fileArg = args.file;
    const planArg = args.plan;
    const expiryArg = args.expiry;
    const skipGroupCheck = String(args.skipGroupCheck || 'false').toLowerCase() === 'true';

    if (!fileArg || !planArg || !expiryArg) {
        console.log('Usage: npm run import:legacy-csv -- --file <path.csv> --plan <planIdOrDays> --expiry <DD/MM/YYYY> [--skipGroupCheck true]');
        process.exit(1);
    }

    if (!process.env.BOT_TOKEN || !process.env.PREMIUM_GROUP_ID) {
        console.log('Missing BOT_TOKEN or PREMIUM_GROUP_ID in environment.');
        process.exit(1);
    }

    const absoluteFile = path.isAbsolute(fileArg) ? fileArg : path.join(process.cwd(), fileArg);
    if (!fs.existsSync(absoluteFile)) {
        console.log(`CSV file not found: ${absoluteFile}`);
        process.exit(1);
    }

    const expiryDate = parseExpiryDate(expiryArg);
    if (!expiryDate || expiryDate <= new Date()) {
        console.log('Invalid expiry date. Use future date in DD/MM/YYYY format.');
        process.exit(1);
    }

    await connectDB();
    const bot = new Telegraf(process.env.BOT_TOKEN);

    try {
        const csvText = fs.readFileSync(absoluteFile, 'utf8');
        const telegramIds = parseCsvIds(csvText);
        if (!telegramIds.length) {
            console.log('No valid telegram IDs found in CSV.');
            process.exit(1);
        }

        const plan = await resolvePlan(planArg);
        if (!plan) {
            console.log('Could not resolve plan. Use valid planId or duration days.');
            process.exit(1);
        }

        const startDate = new Date(expiryDate.getTime() - plan.durationDays * 24 * 60 * 60 * 1000);

        let imported = 0;
        let updated = 0;
        let skippedNotInGroup = 0;
        let failed = 0;

        for (const telegramId of telegramIds) {
            try {
                if (!skipGroupCheck) {
                    const inGroup = await isMemberOfPremiumGroup(bot, telegramId);
                    if (!inGroup) {
                        skippedNotInGroup += 1;
                        continue;
                    }
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

                const activeSub = await Subscription.findOne({
                    telegramId,
                    status: 'active',
                }).sort({ createdAt: -1 });

                if (activeSub) {
                    activeSub.planId = plan._id;
                    activeSub.planName = plan.name;
                    activeSub.durationDays = plan.durationDays;
                    activeSub.startDate = startDate;
                    activeSub.expiryDate = expiryDate;
                    activeSub.status = 'active';
                    activeSub.isRenewal = false;
                    activeSub.reminderFlags = { day7: false, day3: false, day1: false, day0: false };
                    await activeSub.save();
                    updated += 1;
                } else {
                    await Subscription.create({
                        userId: user._id,
                        telegramId,
                        planId: plan._id,
                        planName: plan.name,
                        durationDays: plan.durationDays,
                        startDate,
                        expiryDate,
                        status: 'active',
                        approvedBy: 0,
                        isRenewal: false,
                    });
                    imported += 1;
                }

                await User.findOneAndUpdate(
                    { telegramId },
                    { status: 'active', isBlocked: false, lastInteraction: new Date() }
                );
            } catch (err) {
                failed += 1;
                console.log(`Failed ID ${telegramId}: ${err.message}`);
            }
        }

        console.log('Legacy CSV import complete');
        console.log(`Total IDs: ${telegramIds.length}`);
        console.log(`Imported: ${imported}`);
        console.log(`Updated: ${updated}`);
        console.log(`Skipped (not in group): ${skippedNotInGroup}`);
        console.log(`Failed: ${failed}`);
    } finally {
        await mongoose.connection.close();
    }
};

main().catch(async (err) => {
    console.error(`Import failed: ${err.message}`);
    try {
        await mongoose.connection.close();
    } catch (_) { }
    process.exit(1);
});
