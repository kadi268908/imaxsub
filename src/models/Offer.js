// src/models/Offer.js
// Special offers / discounts displayed to users

const mongoose = require('mongoose');

const offerSchema = new mongoose.Schema({
  planId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Plan',
    default: null,
    index: true,
  },
  title: {
    type: String,
    required: true,
    trim: true,
  },
  description: {
    type: String,
    required: true,
    trim: true,
  },
  discountPercent: {
    type: Number,
    default: 0,
    min: 0,
    max: 100,
  },
  validTill: {
    type: Date,
    required: true,
  },
  isActive: {
    type: Boolean,
    default: true,
  },
  createdBy: {
    type: Number, // superadmin telegramId
    required: true,
  },
}, { timestamps: true });

module.exports = mongoose.model('Offer', offerSchema);
