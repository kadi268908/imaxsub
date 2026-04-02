// src/services/userService.js
// Business logic for user management

const User = require('../models/User');
const Request = require('../models/Request');
const Subscription = require('../models/Subscription');
const logger = require('../utils/logger');

const SUPER_ADMIN_IDS = String(process.env.SUPER_ADMIN_IDS || process.env.SUPER_ADMIN_ID || '')
  .split(',')
  .map(id => parseInt(id.trim(), 10))
  .filter(Boolean);

/**
 * Find or create a user from Telegram context
 */
const findOrCreateUser = async (telegramUser) => {
  try {
    let user = await User.findOne({ telegramId: telegramUser.id });
    
    if (!user) {
      user = await User.create({
        telegramId: telegramUser.id,
        name: `${telegramUser.first_name}${telegramUser.last_name ? ' ' + telegramUser.last_name : ''}`,
        username: telegramUser.username || null,
        role: SUPER_ADMIN_IDS.includes(telegramUser.id) ? 'superadmin' : 'user',
      });
      logger.info(`New user registered: ${user.telegramId} (@${user.username})`);
    } else {
      // Update name/username in case they changed
      user.name = `${telegramUser.first_name}${telegramUser.last_name ? ' ' + telegramUser.last_name : ''}`;
      user.username = telegramUser.username || null;
      await user.save();
    }
    
    return user;
  } catch (error) {
    logger.error(`findOrCreateUser error: ${error.message}`);
    throw error;
  }
};

/**
 * Get user's active subscription
 */
const getActiveSubscription = async (telegramId) => {
  return Subscription.findOne({
    telegramId,
    status: 'active',
    expiryDate: { $gt: new Date() },
  }).populate('planId');
};

/**
 * Get user's pending request
 */
const getPendingRequest = async (telegramId) => {
  return Request.findOne({ telegramId, status: 'pending' });
};

/**
 * Get all users by status filter
 */
const getUsersByStatus = async (filter = {}) => {
  return User.find(filter).sort({ createdAt: -1 });
};

/**
 * Count users by role/status
 */
const getUserStats = async () => {
  const [total, active, expired, pending] = await Promise.all([
    User.countDocuments({ role: 'user' }),
    User.countDocuments({ status: 'active' }),
    User.countDocuments({ status: 'expired' }),
    User.countDocuments({ status: 'pending' }),
  ]);
  return { total, active, expired, pending };
};

module.exports = {
  findOrCreateUser,
  getActiveSubscription,
  getPendingRequest,
  getUsersByStatus,
  getUserStats,
};
