const { escapeMarkdownInlineCode } = require('../utils/markdownEscape');

const registerSellerFlow = ({
    bot,
    User,
    Markup,
    withStyle,
    logger,
    registerSellerProgram,
    getSellerProgramSummary,
    getSellerWithdrawalHistory,
    getSellerPayoutLedgerHistory,
    formatSellerProgramMessage,
    sellerProgramKeyboard,
    buildSetUserFlowUpdate,
    USER_FLOW_STATE,
}) => {
    const showSellerProgram = async (ctx) => {
        const summary = await getSellerProgramSummary(ctx.from.id);
        if (!summary) return ctx.reply('❌ User not found. Please use /start first.');

        const botInfo = await bot.telegram.getMe();
        await ctx.reply(
            formatSellerProgramMessage(summary, botInfo.username),
            {
                parse_mode: 'Markdown',
                ...sellerProgramKeyboard(summary.isSeller, summary.canWithdraw),
            }
        );
    };

    const startSellerWithdrawalUpiFlow = async (ctx) => {
        await User.findOneAndUpdate(
            { telegramId: ctx.from.id },
            buildSetUserFlowUpdate(USER_FLOW_STATE.AWAITING_SELLER_UPI)
        );

        await ctx.reply(
            `💸 *Withdrawal Request*\n\n` +
            `Payout receive karne ke liye apna UPI ID bhejiye.\n` +
            `Example: \`name@bank\`\n\n` +
            `⚠️ Sahi UPI ID bhejein. Request submit hone ke baad process hone me *minimum 24 hours* lag sakte hain.`,
            {
                parse_mode: 'Markdown',
                ...Markup.inlineKeyboard([
                    [withStyle(Markup.button.callback('❌ Cancel', 'cancel_seller_withdraw_upi'), 'danger')],
                    [withStyle(Markup.button.callback('🎫 Support Chat', 'open_support'), 'primary')],
                ]),
            }
        );
    };

    bot.action('seller_program', async (ctx) => {
        await ctx.answerCbQuery();
        await showSellerProgram(ctx);
    });

    bot.action('register_seller', async (ctx) => {
        await ctx.answerCbQuery('Registering...');
        try {
            await registerSellerProgram(ctx.from.id);
            await showSellerProgram(ctx);
        } catch (err) {
            logger.error(`register_seller error: ${err.message}`);
            await ctx.reply('❌ Seller registration failed. Please try again.');
        }
    });

    bot.command('seller', showSellerProgram);

    const showSellerPayoutStatus = async (ctx) => {
        try {
            const withdrawals = await getSellerWithdrawalHistory(ctx.from.id, 10);
            const ledgerEntries = await getSellerPayoutLedgerHistory(ctx.from.id, 10);

            if (!withdrawals.length && !ledgerEntries.length) {
                return ctx.reply('ℹ️ No seller payout history found yet.');
            }

            let msg = '💸 *Seller Payout Status*\n\n';

            if (withdrawals.length) {
                msg += '*Withdrawal Requests:*\n';
                withdrawals.forEach((item, index) => {
                    msg += `${index + 1}. \`${item._id}\`\n`;
                    msg += `   Status: *${String(item.status || '').toUpperCase()}*\n`;
                    msg += `   Amount: *₹${Number(item.amount || 0).toFixed(2)}*\n`;
                    msg += `   UPI: \`${item.upiId || 'N/A'}\`\n`;
                    msg += `   Requested: ${new Date(item.requestedAt).toLocaleString('en-IN')}\n`;
                    if (item.reviewedAt) {
                        msg += `   Reviewed: ${new Date(item.reviewedAt).toLocaleString('en-IN')}\n`;
                    }
                    if (item.note) {
                        msg += `   Note: ${item.note}\n`;
                    }
                    msg += '\n';
                });
            }

            if (ledgerEntries.length) {
                msg += '*Ledger (credits/debits):*\n';
                ledgerEntries.forEach((entry, index) => {
                    const sign = entry.entryType === 'credit' ? '+' : '-';
                    msg += `${index + 1}. ${String(entry.source || '').replace(/_/g, ' ')}\n`;
                    msg += `   ${sign}₹${Number(entry.amount || 0).toFixed(2)} | Balance: ₹${Number(entry.balanceAfter || 0).toFixed(2)}\n`;
                    msg += `   Time: ${new Date(entry.createdAt).toLocaleString('en-IN')}\n\n`;
                });
            }

            if (msg.length > 3900) {
                msg = `${msg.slice(0, 3900)}\n...truncated`;
            }

            await ctx.reply(msg, { parse_mode: 'Markdown' });
        } catch (err) {
            logger.error(`sellerpayouts error: ${err.message}`);
            await ctx.reply('❌ Unable to fetch payout status right now. Please try again.');
        }
    };

    bot.command('sellerpayouts', async (ctx) => {
        await showSellerPayoutStatus(ctx);
    });

    bot.action('seller_payout_status', async (ctx) => {
        await ctx.answerCbQuery('Opening payout status...');
        await showSellerPayoutStatus(ctx);
    });

    bot.action('seller_withdraw', async (ctx) => {
        await ctx.answerCbQuery('Send your UPI ID');
        try {
            await startSellerWithdrawalUpiFlow(ctx);
        } catch (err) {
            await ctx.reply(`⚠️ ${err.message}`);
        }
    });

    bot.command('sellerwithdraw', async (ctx) => {
        try {
            await startSellerWithdrawalUpiFlow(ctx);
        } catch (err) {
            await ctx.reply(`⚠️ ${err.message}`);
        }
    });

    bot.action('cancel_seller_withdraw_upi', async (ctx) => {
        await ctx.answerCbQuery('Cancelled');
        try {
            await User.findOneAndUpdate(
                { telegramId: ctx.from.id },
                buildSetUserFlowUpdate(USER_FLOW_STATE.IDLE)
            );

            await ctx.reply(
                `✅ Withdrawal request process cancelled.`,
                {
                    parse_mode: 'Markdown',
                    ...Markup.inlineKeyboard([
                        [withStyle(Markup.button.callback('🛍 Seller Dashboard', 'seller_program'), 'success')],
                        [withStyle(Markup.button.callback('🎫 Support Chat', 'open_support'), 'primary')],
                    ]),
                }
            );
        } catch (err) {
            await ctx.reply('❌ Unable to cancel right now. Please try again.');
        }
    });
};

const handleSellerWithdrawalUpiMessage = async ({
    ctx,
    userId,
    message,
    requestSellerWithdrawal,
    User,
    buildSetUserFlowUpdate,
    USER_FLOW_STATE,
    notifySellerWithdrawalRequest,
    bot,
}) => {
    const upiId = String(message?.text || '').trim().toLowerCase();
    if (!upiId) {
        await ctx.reply('⚠️ UPI ID text me bhejiye. Example: `name@bank`', { parse_mode: 'Markdown' });
        return true;
    }

    let req;
    try {
        req = await requestSellerWithdrawal(ctx.from.id, upiId);
    } catch (err) {
        const errMsg = String(err?.message || 'Unable to create withdrawal request.');
        if (errMsg.toLowerCase().includes('invalid upi id format')) {
            await ctx.reply('⚠️ Invalid UPI ID format. Example: `name@bank`', { parse_mode: 'Markdown' });
            return true;
        }
        if (errMsg.toLowerCase().includes('upi id is required')) {
            await ctx.reply('⚠️ UPI ID required hai. Example: `name@bank`', { parse_mode: 'Markdown' });
            return true;
        }

        await User.findOneAndUpdate(
            { telegramId: userId },
            buildSetUserFlowUpdate(USER_FLOW_STATE.IDLE)
        );
        await ctx.reply(`⚠️ ${errMsg}`);
        return true;
    }

    await User.findOneAndUpdate(
        { telegramId: userId },
        buildSetUserFlowUpdate(USER_FLOW_STATE.IDLE)
    );

    await notifySellerWithdrawalRequest(bot, ctx, req);
    await ctx.reply(
        `✅ *Withdrawal Request Submitted*\n\n` +
        `Request ID: \`${req._id}\`\n` +
        `UPI ID: \`${escapeMarkdownInlineCode(req.upiId)}\`\n` +
        `Amount: *₹${Number(req.amount).toFixed(2)}*\n\n` +
        `⏱ Withdrawal process hone me *minimum 24 hours* lag sakte hain.`,
        { parse_mode: 'Markdown' }
    );

    return true;
};

module.exports = {
    registerSellerFlow,
    handleSellerWithdrawalUpiMessage,
};
