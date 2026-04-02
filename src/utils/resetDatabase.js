// resetDatabase.js
// Clears all collections in the connected MongoDB database

require('dotenv').config({ override: true });
const mongoose = require('mongoose');
const logger = require('./logger');
const dns = require('dns');
const connectDB = require('../config/database');

async function resetDatabase() {
    try {
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
        const collections = await mongoose.connection.db.collections();
        for (const collection of collections) {
            await collection.deleteMany({});
            logger.info(`Cleared collection: ${collection.collectionName}`);
        }
        logger.info('✅ Database reset complete.');
        process.exit(0);
    } catch (err) {
        logger.error('❌ Database reset failed: ' + err.message);
        if (err.message.includes('querySrv ECONNREFUSED')) {
            logger.error('SRV DNS lookup failed. Set MONGO_DNS_SERVERS in .env (example: 8.8.8.8,1.1.1.1) and retry.');
        }
        process.exit(1);
    }
}

resetDatabase();
