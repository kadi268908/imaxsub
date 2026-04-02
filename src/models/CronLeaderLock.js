const mongoose = require('mongoose');

const cronLeaderLockSchema = new mongoose.Schema({
  jobName: {
    type: String,
    required: true,
    unique: true,
    index: true,
  },
  ownerId: {
    type: String,
    required: true,
    index: true,
  },
  expiresAt: {
    type: Date,
    required: true,
    index: true,
  },
  updatedAt: {
    type: Date,
    default: Date.now,
  },
}, { timestamps: false });

module.exports = mongoose.model('CronLeaderLock', cronLeaderLockSchema);
