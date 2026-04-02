const mongoose = require('mongoose');

const sellerWithdrawalRequestSchema = new mongoose.Schema({
    sellerTelegramId: {
        type: Number,
        required: true,
        index: true,
    },
    upiId: {
        type: String,
        trim: true,
        default: null,
    },
    amount: {
        type: Number,
        required: true,
        min: 0,
    },
    status: {
        type: String,
        enum: ['pending', 'approved', 'rejected'],
        default: 'pending',
        index: true,
    },
    requestedAt: {
        type: Date,
        default: Date.now,
        index: true,
    },
    reviewedAt: {
        type: Date,
        default: null,
    },
    reviewedBy: {
        type: Number,
        default: null,
    },
    note: {
        type: String,
        trim: true,
        default: '',
    },
}, { timestamps: true });

sellerWithdrawalRequestSchema.index(
    { sellerTelegramId: 1, status: 1 },
    {
        unique: true,
        partialFilterExpression: { status: 'pending' },
        name: 'uniq_pending_withdrawal_per_seller',
    }
);

module.exports = mongoose.model('SellerWithdrawalRequest', sellerWithdrawalRequestSchema);
