// src/models/Plan.js
// Subscription plans (30/90/180/365 days, etc.)

const mongoose = require('mongoose');

const planSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true,
    trim: true,
  },
  durationDays: {
    type: Number,
    required: true,
    min: 1,
  },
  price: {
    type: Number,
    default: 0,
    min: 0,
  },
  currency: {
    type: String,
    default: 'INR',
  },
  description: {
    type: String,
    trim: true,
    default: '',
  },
  category: {
    type: String,
    enum: ['movie', 'desi', 'non_desi'],
    default: 'movie',
    index: true,
  },
  isActive: {
    type: Boolean,
    default: true,
  },
}, { timestamps: true });

module.exports = mongoose.model('Plan', planSchema);
