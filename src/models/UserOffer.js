// src/models/UserOffer.js
// One-time private offers assigned to individual users

const mongoose = require('mongoose');

const userOfferSchema = new mongoose.Schema({
    targetTelegramId: {
        type: Number,
        required: true,
        index: true,
    },
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
        index: true,
    },
    isActive: {
        type: Boolean,
        default: true,
        index: true,
    },
    isUsed: {
        type: Boolean,
        default: false,
        index: true,
    },
    usedAt: {
        type: Date,
        default: null,
    },
    usedByRequestId: {
        type: mongoose.Schema.Types.ObjectId,
        ref: 'Request',
        default: null,
    },
    createdBy: {
        type: Number,
        required: true,
    },
}, { timestamps: true });

module.exports = mongoose.model('UserOffer', userOfferSchema);
