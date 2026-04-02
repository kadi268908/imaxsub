// src/models/DailySummary.js
// Stores daily activity summary for analytics/reporting

const mongoose = require('mongoose');

const dailySummarySchema = new mongoose.Schema({
  date: {
    type: Date,
    required: true,
    unique: true,
    index: true,
  },
  newUsers: { type: Number, default: 0 },
  requestsReceived: { type: Number, default: 0 },
  approvals: { type: Number, default: 0 },
  renewals: { type: Number, default: 0 },
  expiredToday: { type: Number, default: 0 },
  removedFromGroup: { type: Number, default: 0 },
  broadcasts: { type: Number, default: 0 },
}, { timestamps: true });

module.exports = mongoose.model('DailySummary', dailySummarySchema);
