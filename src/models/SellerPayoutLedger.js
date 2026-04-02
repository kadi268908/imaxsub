const mongoose = require('mongoose');

const sellerPayoutLedgerSchema = new mongoose.Schema({
    sellerTelegramId: {
        type: Number,
        required: true,
        index: true,
    },
    entryType: {
        type: String,
        enum: ['credit', 'debit'],
        required: true,
        index: true,
    },
    source: {
        type: String,
        enum: ['commission', 'withdrawal_approved', 'manual_adjustment'],
        required: true,
        index: true,
    },
    amount: {
        type: Number,
        required: true,
        min: 0,
    },
    balanceAfter: {
        type: Number,
        required: true,
        min: 0,
    },
    relatedUserTelegramId: {
        type: Number,
        default: null,
    },
    relatedWithdrawalRequestId: {
        type: mongoose.Schema.Types.ObjectId,
        ref: 'SellerWithdrawalRequest',
        default: null,
    },
    note: {
        type: String,
        trim: true,
        default: '',
    },
    createdBy: {
        type: Number,
        default: 0,
        index: true,
    },
}, { timestamps: true });

sellerPayoutLedgerSchema.index({ sellerTelegramId: 1, createdAt: -1 });

module.exports = mongoose.model('SellerPayoutLedger', sellerPayoutLedgerSchema);
