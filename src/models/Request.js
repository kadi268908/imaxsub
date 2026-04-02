// src/models/Request.js
// Tracks premium access requests from users

const mongoose = require('mongoose');

const requestSchema = new mongoose.Schema({
  userId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'User',
    required: true,
  },
  telegramId: {
    type: Number,
    required: true,
    index: true,
  },
  status: {
    type: String,
    enum: ['pending', 'processing', 'approved', 'rejected'],
    default: 'pending',
    index: true,
  },
  requestDate: {
    type: Date,
    default: Date.now,
  },
  actionDate: {
    type: Date,
    default: null,
  },
  actionBy: {
    type: Number, // admin/superadmin telegramId
    default: null,
  },
  processingBy: {
    type: Number,
    default: null,
    index: true,
  },
  processingAt: {
    type: Date,
    default: null,
    index: true,
  },
  processingToken: {
    type: String,
    default: null,
  },
  processingError: {
    type: String,
    default: null,
  },
  pendingReminderAt: {
    type: Date,
    default: null,
    index: true,
  },
  selectedPlanId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Plan',
    default: null,
  },
  requestCategory: {
    type: String,
    enum: ['movie', 'desi', 'non_desi'],
    default: 'movie',
    index: true,
  },
  paymentProof: {
    fileId: {
      type: String,
      default: null,
    },
    fileUniqueId: {
      type: String,
      default: null,
    },
    sourceType: {
      type: String,
      enum: ['photo', 'document', null],
      default: null,
    },
    logMessageId: {
      type: Number,
      default: null,
    },
  },
  logMessageId: {
    type: Number, // message ID in log channel for editing
    default: null,
  },
  appliedUserOffer: {
    offerId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'UserOffer',
      default: null,
    },
    title: {
      type: String,
      default: null,
    },
    discountPercent: {
      type: Number,
      default: 0,
    },
  },
}, { timestamps: true });

module.exports = mongoose.model('Request', requestSchema);
