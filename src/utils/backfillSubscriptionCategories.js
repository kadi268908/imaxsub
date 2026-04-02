require('dotenv').config({ override: true });
const mongoose = require('mongoose');
const dns = require('dns');

const connectDB = require('../config/database');

const Subscription = require('../models/Subscription');
const logger = require('./logger');
const { PLAN_CATEGORY, normalizePlanCategory, getGroupIdForCategory } = require('./premiumGroups');

const VALID_CATEGORIES = new Set(Object.values(PLAN_CATEGORY));

const APPLY_MODE = process.argv.includes('--apply');
const SYNC_GROUP_MODE = process.argv.includes('--sync-group');

const connect = async () => {
    const dnsServersRaw = process.env.MONGO_DNS_SERVERS;
    if (dnsServersRaw) {
        const dnsServers = dnsServersRaw
            .split(',')
            .map((server) => server.trim())
            .filter(Boolean);

        if (dnsServers.length > 0) {
            dns.setServers(dnsServers);
            logger.info(`Using custom DNS servers for MongoDB lookup: ${dnsServers.join(', ')}`);
        }
    }

    const mongoUri = connectDB.normalizeMongoUri(process.env.MONGO_URI);
    await mongoose.connect(mongoUri, {
        serverSelectionTimeoutMS: 5000,
        socketTimeoutMS: 45000,
    });
};

const flushBulk = async (ops, stats) => {
    if (!ops.length || !APPLY_MODE) return;
    const result = await Subscription.bulkWrite(ops, { ordered: false });
    stats.modified += result.modifiedCount || 0;
    ops.length = 0;
};

const run = async () => {
    const stats = {
        scanned: 0,
        categoryBackfilled: 0,
        groupBackfilled: 0,
        groupSynced: 0,
        modified: 0,
        skippedNoGroupMapping: 0,
    };

    logger.info(`Starting subscription category migration in ${APPLY_MODE ? 'APPLY' : 'DRY-RUN'} mode`);
    logger.info(`Group behavior: ${SYNC_GROUP_MODE ? 'SYNC category group IDs for all matched records' : 'FILL only missing group IDs'}`);

    const bulkOps = [];
    const cursor = Subscription.find({})
        .populate('planId', 'category')
        .cursor();

    for await (const sub of cursor) {
        stats.scanned++;

        const rawCategory = sub.planCategory;
        const hasValidCategory = VALID_CATEGORIES.has(String(rawCategory || '').toLowerCase());
        const inferredCategory = normalizePlanCategory(rawCategory || sub.planId?.category || PLAN_CATEGORY.MOVIE);

        const currentGroupId = sub.premiumGroupId ? String(sub.premiumGroupId) : '';
        const inferredGroupId = String(getGroupIdForCategory(inferredCategory) || '');

        const shouldBackfillCategory = !hasValidCategory;
        const shouldBackfillGroup = !currentGroupId && Boolean(inferredGroupId);
        const shouldSyncGroup = SYNC_GROUP_MODE && Boolean(inferredGroupId) && currentGroupId !== inferredGroupId;

        if (!inferredGroupId && (!currentGroupId || SYNC_GROUP_MODE)) {
            stats.skippedNoGroupMapping++;
        }

        const nextCategory = shouldBackfillCategory ? inferredCategory : sub.planCategory;
        const nextGroupId = shouldSyncGroup
            ? inferredGroupId
            : (shouldBackfillGroup ? inferredGroupId : currentGroupId);

        const categoryChanged = String(nextCategory || '') !== String(sub.planCategory || '');
        const groupChanged = String(nextGroupId || '') !== String(currentGroupId || '');

        if (!categoryChanged && !groupChanged) continue;

        if (categoryChanged) stats.categoryBackfilled++;
        if (shouldSyncGroup && groupChanged) stats.groupSynced++;
        else if (shouldBackfillGroup && groupChanged) stats.groupBackfilled++;

        bulkOps.push({
            updateOne: {
                filter: { _id: sub._id },
                update: {
                    $set: {
                        planCategory: nextCategory,
                        premiumGroupId: nextGroupId || null,
                    },
                },
            },
        });

        if (bulkOps.length >= 500) {
            await flushBulk(bulkOps, stats);
        }
    }

    await flushBulk(bulkOps, stats);

    const wouldModify = stats.categoryBackfilled + stats.groupBackfilled + stats.groupSynced;
    logger.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    logger.info(`Scanned: ${stats.scanned}`);
    logger.info(`Category backfills: ${stats.categoryBackfilled}`);
    logger.info(`Group backfills (missing only): ${stats.groupBackfilled}`);
    logger.info(`Group sync updates: ${stats.groupSynced}`);
    logger.info(`Skipped (no group mapping available): ${stats.skippedNoGroupMapping}`);
    logger.info(APPLY_MODE ? `Modified: ${stats.modified}` : `Would modify: ${wouldModify}`);
    logger.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

    if (!APPLY_MODE) {
        logger.info('Dry-run complete. Re-run with --apply to persist changes.');
    }
};

run()
    .then(async () => {
        await mongoose.disconnect();
        process.exit(0);
    })
    .catch(async (err) => {
        logger.error(`❌ Subscription category migration failed: ${err.message}`);
        if (err.message.includes('querySrv ECONNREFUSED')) {
            logger.error('SRV DNS lookup failed. Set MONGO_DNS_SERVERS in .env (example: 8.8.8.8,1.1.1.1) and retry.');
        }
        await mongoose.disconnect().catch(() => { });
        process.exit(1);
    });
