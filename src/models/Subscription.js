// src/models/Subscription.js
// Tracks all user subscriptions with dates, status, and reminder flags

const mongoose = require('mongoose');

const subscriptionSchema = new mongoose.Schema({
  userId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'User',
    required: true,
    index: true,
  },
  telegramId: {
    type: Number,
    required: true,
    index: true,
  },
  planId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Plan',
    required: true,
  },
  planName: {
    type: String,
    required: true,
  },
  planCategory: {
    type: String,
    enum: ['movie', 'desi', 'non_desi'],
    default: 'movie',
    index: true,
  },
  premiumGroupId: {
    type: String,
    default: null,
    index: true,
  },
  durationDays: {
    type: Number,
    required: true,
  },
  startDate: {
    type: Date,
    default: Date.now,
  },
  expiryDate: {
    type: Date,
    required: true,
    index: true,
  },
  status: {
    type: String,
    enum: ['active', 'expired', 'cancelled'],
    default: 'active',
    index: true,
  },
  approvedBy: {
    type: Number,
    default: null,
  },
  inviteLink: {
    type: String,
    default: null,
  },
  inviteLinkIssuedAt: {
    type: Date,
    default: null,
  },
  inviteLinkTtlMinutes: {
    type: Number,
    default: null,
  },
  reminderFlags: {
    day7: { type: Boolean, default: false },
    day3: { type: Boolean, default: false },
    day1: { type: Boolean, default: false },
    day0: { type: Boolean, default: false },
  },
  isRenewal: {
    type: Boolean,
    default: false,
  },
}, { timestamps: true });

module.exports = mongoose.model('Subscription', subscriptionSchema);
