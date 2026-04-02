// src/models/DmWordFilter.js
// DM word filters added by admins/superadmins to gate fallback reply behavior

const mongoose = require('mongoose');

const dmWordFilterSchema = new mongoose.Schema({
    phrase: {
        type: String,
        required: true,
        trim: true,
        unique: true,
        index: true,
    },
    normalizedPhrase: {
        type: String,
        required: true,
        trim: true,
        unique: true,
        index: true,
    },
    createdBy: {
        type: Number,
        required: true,
        index: true,
    },
    responseType: {
        type: String,
        enum: ['text', 'photo', 'sticker'],
        required: true,
        index: true,
    },
    responseText: {
        type: String,
        default: null,
    },
    responsePhotoFileId: {
        type: String,
        default: null,
    },
    responseStickerFileId: {
        type: String,
        default: null,
    },
    responseCaption: {
        type: String,
        default: null,
    },
    createdAt: {
        type: Date,
        default: Date.now,
    },
});

module.exports = mongoose.model('DmWordFilter', dmWordFilterSchema);
