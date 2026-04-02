// src/models/User.js
// User schema - stores all bot users with their roles, status, referrals, and activity

const mongoose = require('mongoose');
const { v4: uuidv4 } = require('uuid');

const userSchema = new mongoose.Schema({
  telegramId: {
    type: Number,
    required: true,
    unique: true,
    index: true,
  },
  name: {
    type: String,
    required: true,
    trim: true,
  },
  username: {
    type: String,
    trim: true,
    default: null,
  },
  role: {
    type: String,
    enum: ['user', 'admin', 'superadmin'],
    default: 'user',
  },
  joinDate: {
    type: Date,
    default: Date.now,
  },
  status: {
    type: String,
    enum: ['active', 'expired', 'pending', 'inactive', 'blocked'],
    default: 'inactive',
    index: true,
  },
  isBlocked: {
    type: Boolean,
    default: false,
    index: true,
  },
  // Referral system
  referralCode: {
    type: String,
    unique: true,
    sparse: true,
    default: () => uuidv4().substring(0, 8).toUpperCase(),
  },
  referredBy: {
    type: Number, // telegramId of referrer
    default: null,
    index: true,
  },
  referralBonusApplied: {
    type: Boolean,
    default: false,
  },
  // Seller referral program
  isSeller: {
    type: Boolean,
    default: false,
    index: true,
  },
  sellerCode: {
    type: String,
    unique: true,
    sparse: true,
    default: undefined,
  },
  sellerReferredBy: {
    type: Number, // telegramId of seller referrer
    default: null,
    index: true,
  },
  sellerCommissionApplied: {
    type: Boolean,
    default: false,
  },
  sellerStats: {
    totalReferrals: { type: Number, default: 0 },
    qualifiedReferrals: { type: Number, default: 0 },
    lifetimeEarnings: { type: Number, default: 0 },
    availableBalance: { type: Number, default: 0 },
  },
  // Activity tracking
  lastInteraction: {
    type: Date,
    default: Date.now,
    index: true,
  },
  // Flexible metadata (e.g. awaitingSupport flag)
  meta: {
    type: mongoose.Schema.Types.Mixed,
    default: {},
  },
}, { timestamps: true });

module.exports = mongoose.model('User', userSchema);
