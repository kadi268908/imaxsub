// src/config/database.js
// MongoDB connection setup with reconnection logic

const mongoose = require('mongoose');
const dns = require('dns');
const logger = require('../utils/logger');

const MONGO_CONNECT_RETRY_BASE_MS = Math.max(1000, parseInt(process.env.MONGO_CONNECT_RETRY_BASE_MS || '5000', 10));
const MONGO_CONNECT_RETRY_MAX_MS = Math.max(MONGO_CONNECT_RETRY_BASE_MS, parseInt(process.env.MONGO_CONNECT_RETRY_MAX_MS || '60000', 10));
const MONGO_CONNECT_RETRY_JITTER_MS = Math.max(0, parseInt(process.env.MONGO_CONNECT_RETRY_JITTER_MS || '1500', 10));

let reconnectTimer = null;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const getRetryDelayMs = (attempt) => {
  const exponential = Math.min(MONGO_CONNECT_RETRY_MAX_MS, MONGO_CONNECT_RETRY_BASE_MS * (2 ** Math.min(attempt, 6)));
  const jitter = MONGO_CONNECT_RETRY_JITTER_MS > 0 ? Math.floor(Math.random() * MONGO_CONNECT_RETRY_JITTER_MS) : 0;
  return exponential + jitter;
};

const isRetryableMongoError = (error) => {
  const message = String(error?.message || '').toLowerCase();
  const retryableHints = [
    'querysrv enotfound',
    'querysrv econnrefused',
    'getaddrinfo enotfound',
    'eai_again',
    'etimedout',
    'server selection timed out',
    'timed out',
  ];

  return retryableHints.some((hint) => message.includes(hint));
};

const normalizeMongoUri = (value) => {
  if (!value) return value;

  let normalized = String(value).trim();

  if (
    (normalized.startsWith('"') && normalized.endsWith('"')) ||
    (normalized.startsWith("'") && normalized.endsWith("'"))
  ) {
    normalized = normalized.slice(1, -1).trim();
  }

  if (normalized.startsWith('MONGO_URI=')) {
    normalized = normalized.slice('MONGO_URI='.length).trim();
  }

  return normalized;
};

const connectDB = async () => {
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

  const mongoUri = normalizeMongoUri(process.env.MONGO_URI);
  let attempt = 0;

  while (true) {
    try {
      const conn = await mongoose.connect(mongoUri, {
        serverSelectionTimeoutMS: 5000,
        socketTimeoutMS: 45000,
      });

      logger.info(`MongoDB Connected: ${conn.connection.host}`);

      mongoose.connection.removeAllListeners('disconnected');
      mongoose.connection.removeAllListeners('error');

      mongoose.connection.on('disconnected', () => {
        logger.warn('MongoDB disconnected. Attempting reconnect...');
        if (reconnectTimer) return;

        reconnectTimer = setTimeout(async () => {
          reconnectTimer = null;
          try {
            await connectDB();
          } catch (err) {
            logger.error(`MongoDB reconnect loop error: ${err.message}`);
          }
        }, MONGO_CONNECT_RETRY_BASE_MS);
      });

      mongoose.connection.on('error', (err) => {
        logger.error(`MongoDB error: ${err.message}`);
      });

      return conn;
    } catch (error) {
      if (!isRetryableMongoError(error)) {
        logger.error(`MongoDB connection failed (non-retryable): ${error.message}`);
        if (String(error?.message || '').includes('querySrv ECONNREFUSED')) {
          logger.error('SRV DNS lookup failed. Set MONGO_DNS_SERVERS in .env (example: 8.8.8.8,1.1.1.1) and retry.');
        }
        throw error;
      }

      attempt += 1;
      const delayMs = getRetryDelayMs(attempt);
      logger.warn(`MongoDB connection retry ${attempt} in ${delayMs}ms. Reason: ${error.message}`);
      await sleep(delayMs);
    }
  }
};

module.exports = connectDB;
module.exports.normalizeMongoUri = normalizeMongoUri;
