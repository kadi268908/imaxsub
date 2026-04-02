const { getUserFlowState } = require('../utils/userFlowState');
const { isBroadcastSessionActive } = require('../utils/broadcastSession');
const { scheduleDeleteMessage } = require('../utils/telegramUtils');

const PAYMENT_PROOF_SPAM_WINDOW_MINUTES = Math.max(1, parseInt(process.env.PAYMENT_PROOF_SPAM_WINDOW_MINUTES || '10', 10));
const PAYMENT_PROOF_MAX_ATTEMPTS = Math.max(1, parseInt(process.env.PAYMENT_PROOF_MAX_ATTEMPTS || '5', 10));
const PAYMENT_PROOF_COOLDOWN_MINUTES = Math.max(1, parseInt(process.env.PAYMENT_PROOF_COOLDOWN_MINUTES || '30', 10));
const PAYMENT_PROOF_HANDLER_LOCK_MS = Math.max(
    5000,
    parseInt(process.env.PAYMENT_PROOF_HANDLER_LOCK_MS || '15000', 10)
);

// In Telegram, forwarding multiple screenshots can trigger multiple `photo/document`
// updates almost at the same time. Without a lock, two handlers can race and
// create multiple pending Requests + duplicate user/admin messages.
/** @type {Map<number, number>} */
const paymentProofProcessingLocks = new Map(); // telegramId -> expiresAtMs

const acquirePaymentProofLock = (telegramId) => {
    const now = Date.now();
    const expiresAt = paymentProofProcessingLocks.get(telegramId) || 0;
    if (expiresAt > now) return false;
    paymentProofProcessingLocks.set(telegramId, now + PAYMENT_PROOF_HANDLER_LOCK_MS);
    return true;
};

const releasePaymentProofLock = (telegramId) => {
    paymentProofProcessingLocks.delete(telegramId);
};

const formatPlanButtonPrice = (planPrice, appliedDiscountPercent, discountedPrice, formatInr) => {
    if (!planPrice) return '';
    if (appliedDiscountPercent > 0 && discountedPrice > 0) {
        return `₹${formatInr(discountedPrice)}`;
    }
    return `₹${formatInr(planPrice)}`;
};

const registerPaymentFlow = ({
    bot,
    fs,
    path,
    User,
    Request,
    Subscription,
    Plan,
    Markup,
    withStyle,
    logger,
    PLAN_CATEGORY,
    QR_ASSET_BY_CATEGORY,
    normalizePlanCategory,
    getPlanCategoryLabel,
    buildCategoryPlansText,
    buildSetUserFlowUpdate,
    USER_FLOW_STATE,
    escapeMarkdown,
    findOrCreateUser,
    getBestPublicOffer,
    getDiscountedPrice,
    getNextOneTimeUserOffer,
    formatInr,
    consumeOneTimeUserOffer,
    submitPremiumRequest,
}) => {
    bot.action(/^plan_menu_(movie|desi|non_desi)$/, async (ctx) => {
        await ctx.answerCbQuery();
        try {
            const category = normalizePlanCategory(ctx.match[1]);
            const plansText = await buildCategoryPlansText(category, { telegramId: ctx.from.id });
            const activeCategorySub = await Subscription.findOne({
                telegramId: ctx.from.id,
                status: 'active',
                expiryDate: { $gt: new Date() },
                planCategory: category,
            });
            const plansMessage = activeCategorySub
                ? `${plansText}\n\n✅ You already have an active ${getPlanCategoryLabel(category)} subscription.\nIf you want to extend it, click *Renew*.`
                : plansText;
            const qrFileName = QR_ASSET_BY_CATEGORY[category];
            const qrPath = path.join(process.cwd(), 'assets', qrFileName);
            const paidRows = [];
            if (activeCategorySub) {
                paidRows.push([withStyle(Markup.button.callback(`🔄 Renew ${getPlanCategoryLabel(category)}`, `status_renew_${category}`), 'success')]);
            } else {
                const plans = await Plan.find({ isActive: true, category }).sort({ durationDays: 1 });
                for (const plan of plans) {
                    const bestOffer = await getBestPublicOffer(plan._id);
                    const privateOffer = await getNextOneTimeUserOffer(ctx.from.id, plan._id);
                    const privateDiscountPercent = Number(privateOffer?.discountPercent || 0);
                    const publicDiscountPercent = Number(bestOffer?.discountPercent || 0);
                    const appliedDiscountPercent = privateDiscountPercent > 0
                        ? privateDiscountPercent
                        : publicDiscountPercent;
                    const discountedPrice = appliedDiscountPercent > 0
                        ? getDiscountedPrice(plan.price, appliedDiscountPercent)
                        : plan.price;
                    const priceText = formatPlanButtonPrice(plan.price, appliedDiscountPercent, discountedPrice, formatInr);
                    const buttonLabel = priceText
                        ? `✅ Paid ${plan.durationDays}d · ${priceText}`
                        : `✅ Paid ${plan.durationDays}d`;
                    paidRows.push([withStyle(Markup.button.callback(buttonLabel, `paid_plan_${plan._id}`), 'success')]);
                }
                if (!paidRows.length) {
                    paidRows.push([withStyle(Markup.button.callback('✅ Paid', `paid_${category}`), 'success')]);
                }
            }
            paidRows.push([withStyle(Markup.button.callback('🏠 Main Menu', 'back_to_main'), 'primary')]);
            const paidKeyboard = Markup.inlineKeyboard(paidRows);

            if (fs.existsSync(qrPath)) {
                await ctx.replyWithPhoto(
                    { source: qrPath },
                    {
                        caption: plansMessage,
                        ...paidKeyboard,
                    }
                );
            } else {
                await ctx.reply(
                    `${plansMessage}\n\n⚠️ QR image missing: ${qrFileName} (assets folder).`,
                    {
                        ...paidKeyboard,
                    }
                );
            }
        } catch (err) {
            logger.error(`plan_menu error: ${err.message}`);
            await ctx.reply('❌ Plan fetch failed. Please try again.');
        }
    });

    bot.action(/^paid_(movie|desi|non_desi)$/, async (ctx) => {
        await ctx.answerCbQuery();
        try {
            const callbackMessageId = ctx.callbackQuery?.message?.message_id;
            const callbackChatId = ctx.callbackQuery?.message?.chat?.id;
            const category = normalizePlanCategory(ctx.match[1]);
            await User.findOneAndUpdate(
                { telegramId: ctx.from.id },
                buildSetUserFlowUpdate(
                    USER_FLOW_STATE.AWAITING_PAYMENT_SCREENSHOT,
                    {
                        'meta.paymentCategory': category,
                        'meta.paymentFlowType': 'new_request',
                    },
                    {
                        'meta.paymentProofReadyForCategory': '',
                        'meta.renewalPlanId': '',
                        'meta.paymentSelectedPlanId': '',
                    }
                ),
                { upsert: false }
            );

            await ctx.reply(
                `📸 *Payment screenshot upload karein*\n\n` +
                `Aapne *${escapeMarkdown(getPlanCategoryLabel(category))}* select kiya hai.\n` +
                `Ab payment screenshot photo/document bhejiye.` +
                `🚫 Agar aapne fake screenshot upload kiya to aap hamesha k liye ban ho jaogye.\n\n`,
                {
                    parse_mode: 'Markdown',
                    ...Markup.inlineKeyboard([
                        [withStyle(Markup.button.callback('❌ Cancel Upload', 'cancel_payment_upload'), 'danger')],
                        [withStyle(Markup.button.callback('🏠 Main Menu', 'back_to_main'), 'primary')],
                    ]),
                }
            );

            if (callbackChatId && callbackMessageId) {
                scheduleDeleteMessage(ctx.telegram, callbackChatId, callbackMessageId);
            }
        } catch (err) {
            logger.error(`paid action error: ${err.message}`);
            await ctx.reply('❌ Unable to process. Please try again.');
        }
    });

    bot.action(/^paid_plan_([a-fA-F0-9]{24})$/, async (ctx) => {
        await ctx.answerCbQuery();
        try {
            const callbackMessageId = ctx.callbackQuery?.message?.message_id;
            const callbackChatId = ctx.callbackQuery?.message?.chat?.id;
            const planId = ctx.match[1];
            const plan = await Plan.findById(planId);
            if (!plan || !plan.isActive) {
                await ctx.reply('❌ Selected plan is not available right now. Please choose again from Check Plans.');
                return;
            }

            const category = normalizePlanCategory(plan.category);
            const bestOffer = await getBestPublicOffer(plan._id);
            const privateOffer = await getNextOneTimeUserOffer(ctx.from.id, plan._id);
            const privateDiscountPercent = Number(privateOffer?.discountPercent || 0);
            const publicDiscountPercent = Number(bestOffer?.discountPercent || 0);
            const appliedDiscountPercent = privateDiscountPercent > 0
                ? privateDiscountPercent
                : publicDiscountPercent;
            const payableAmount = appliedDiscountPercent > 0
                ? getDiscountedPrice(plan.price, appliedDiscountPercent)
                : plan.price;
            const appliedOfferTitle = privateDiscountPercent > 0
                ? privateOffer.title
                : bestOffer?.title;

            await User.findOneAndUpdate(
                { telegramId: ctx.from.id },
                buildSetUserFlowUpdate(
                    USER_FLOW_STATE.AWAITING_PAYMENT_SCREENSHOT,
                    {
                        'meta.paymentCategory': category,
                        'meta.paymentFlowType': 'new_request',
                        'meta.paymentSelectedPlanId': String(plan._id),
                    },
                    {
                        'meta.paymentProofReadyForCategory': '',
                        'meta.renewalPlanId': '',
                    }
                ),
                { upsert: false }
            );

            await ctx.reply(
                `📸 *Payment screenshot upload karein*\n\n` +
                `Aapne *${escapeMarkdown(getPlanCategoryLabel(category))}* select kiya hai.\n` +
                `Plan: *${escapeMarkdown(plan.name)}* (${plan.durationDays} days${plan.price ? ` · ₹${formatInr(plan.price)}` : ''})\n` +
                (plan.price && appliedDiscountPercent > 0
                    ? `🎁 Offer: *${escapeMarkdown(appliedOfferTitle || 'Applied Offer')}* (${appliedDiscountPercent}% OFF)\n` +
                    `💰 Payable: *₹${formatInr(payableAmount)}*\n\n`
                    : '\n') +
                `Ab payment screenshot photo/document bhejiye.` +
                `🚫 Agar aapne fake screenshot upload kiya to aap hamesha k liye ban ho jaogye.\n\n`,
                {
                    parse_mode: 'Markdown',
                    ...Markup.inlineKeyboard([
                        [withStyle(Markup.button.callback('❌ Cancel Upload', 'cancel_payment_upload'), 'danger')],
                        [withStyle(Markup.button.callback('🏠 Main Menu', 'back_to_main'), 'primary')],
                    ]),
                }
            );

            if (callbackChatId && callbackMessageId) {
                scheduleDeleteMessage(ctx.telegram, callbackChatId, callbackMessageId);
            }
        } catch (err) {
            logger.error(`paid_plan action error: ${err.message}`);
            await ctx.reply('❌ Unable to process selected plan. Please try again.');
        }
    });

    bot.action('cancel_payment_upload', async (ctx) => {
        await ctx.answerCbQuery('Upload cancelled');
        try {
            await User.findOneAndUpdate(
                { telegramId: ctx.from.id },
                buildSetUserFlowUpdate(
                    USER_FLOW_STATE.IDLE,
                    {},
                    {
                        'meta.paymentCategory': '',
                        'meta.paymentProofReadyForCategory': '',
                        'meta.paymentFlowType': '',
                        'meta.renewalPlanId': '',
                        'meta.paymentSelectedPlanId': '',
                    }
                )
            );

            await ctx.reply(
                `✅ Screenshot upload mode cancelled.\n\n` +
                `Aap dubara plan choose karke continue kar sakte hain.`,
                {
                    parse_mode: 'Markdown',
                    ...Markup.inlineKeyboard([
                        [withStyle(Markup.button.callback('📋 Check Plans', 'check_plans'), 'success')],
                        [withStyle(Markup.button.callback('🏠 Main Menu', 'back_to_main'), 'primary')],
                    ]),
                }
            );
        } catch (err) {
            logger.error(`cancel_payment_upload error: ${err.message}`);
            await ctx.reply('❌ Unable to cancel upload mode right now. Please try again.');
        }
    });

    bot.action(/^renew_request_(?:(movie|desi|non_desi)_)?(.+)$/, async (ctx) => {
        await ctx.answerCbQuery('Continue with payment screenshot...');
        try {
            const categoryFromCallback = normalizePlanCategory(ctx.match[1] || PLAN_CATEGORY.MOVIE);
            const planId = ctx.match[2];
            const user = await findOrCreateUser(ctx.from);
            await User.findByIdAndUpdate(user._id, { lastInteraction: new Date() });

            const plan = await Plan.findById(planId);
            if (!plan) return ctx.reply('❌ Plan not found. Please try again.');

            const renewalCategory = normalizePlanCategory(plan.category || categoryFromCallback);
            if (ctx.match[1] && renewalCategory !== categoryFromCallback) {
                return ctx.reply('❌ Selected plan category mismatch. Please retry renewal from status.');
            }

            const activeSub = await Subscription.findOne({
                telegramId: ctx.from.id,
                status: 'active',
                planCategory: renewalCategory,
                expiryDate: { $gt: new Date() },
            });
            if (!activeSub) {
                const hadOldSubscription = await Subscription.exists({
                    telegramId: ctx.from.id,
                    planCategory: renewalCategory,
                });

                if (!hadOldSubscription) {
                    return ctx.reply(
                        `⚠️ Aapke paas *${escapeMarkdown(getPlanCategoryLabel(renewalCategory))}* ka active subscription nahi hai.\n\n` +
                        `Renew karne ke liye pehle us category ka active plan hona chahiye.`,
                        { parse_mode: 'Markdown' }
                    );
                }

                await User.findByIdAndUpdate(
                    user._id,
                    buildSetUserFlowUpdate(
                        USER_FLOW_STATE.AWAITING_PAYMENT_SCREENSHOT,
                        {
                            'meta.paymentCategory': renewalCategory,
                            'meta.paymentFlowType': 'new_request',
                        },
                        {
                            'meta.paymentProofReadyForCategory': '',
                            'meta.latestPaymentProof': '',
                            'meta.renewalPlanId': '',
                            'meta.paymentSelectedPlanId': '',
                        }
                    )
                );

                return ctx.reply(
                    `ℹ️ *${escapeMarkdown(getPlanCategoryLabel(renewalCategory))}* subscription expire ho chuka hai, isliye purana renew button invalid hai.\n\n` +
                    `Ab issi category ke liye naya request create hoga. Payment screenshot bhejiye.`,
                    {
                        parse_mode: 'Markdown',
                        ...Markup.inlineKeyboard([
                            [withStyle(Markup.button.callback('❌ Cancel Upload', 'cancel_payment_upload'), 'danger')],
                            [withStyle(Markup.button.callback('🏠 Main Menu', 'back_to_main'), 'primary')],
                        ]),
                    }
                );
            }

            const existingPending = await Request.findOne({
                telegramId: ctx.from.id,
                status: 'pending',
                requestCategory: renewalCategory,
            });
            if (existingPending) {
                return ctx.reply(
                    `⏳ *${escapeMarkdown(getPlanCategoryLabel(renewalCategory))} renewal pending hai!*\n\n` +
                    `Admin approval ka wait kijiye.`,
                    { parse_mode: 'Markdown' }
                );
            }

            await User.findByIdAndUpdate(
                user._id,
                buildSetUserFlowUpdate(
                    USER_FLOW_STATE.AWAITING_PAYMENT_SCREENSHOT,
                    {
                        'meta.paymentCategory': renewalCategory,
                        'meta.paymentFlowType': 'renewal',
                        'meta.renewalPlanId': String(plan._id),
                    },
                    {
                        'meta.paymentProofReadyForCategory': '',
                        'meta.latestPaymentProof': '',
                        'meta.paymentSelectedPlanId': '',
                    }
                )
            );

            const callbackMessageId = ctx.callbackQuery?.message?.message_id;
            const callbackChatId = ctx.callbackQuery?.message?.chat?.id;
            const bestOffer = await getBestPublicOffer(plan._id);
            const privateOffer = await getNextOneTimeUserOffer(ctx.from.id);
            const privateDiscountPercent = Number(privateOffer?.discountPercent || 0);
            const publicDiscountPercent = Number(bestOffer?.discountPercent || 0);
            const appliedDiscountPercent = privateDiscountPercent > 0
                ? privateDiscountPercent
                : publicDiscountPercent;
            const discountedRenewalPrice = appliedDiscountPercent > 0
                ? getDiscountedPrice(plan.price, appliedDiscountPercent)
                : plan.price;
            const appliedOfferTitle = privateDiscountPercent > 0
                ? privateOffer.title
                : bestOffer?.title;

            await ctx.reply(
                `📸 *Renewal Payment Screenshot Upload Karein*\n\n` +
                `Category: *${escapeMarkdown(getPlanCategoryLabel(renewalCategory))}*\n` +
                `Plan: *${escapeMarkdown(plan.name)}* (${plan.durationDays} days${plan.price ? ` · ₹${formatInr(plan.price)}` : ''})\n` +
                (plan.price && appliedDiscountPercent > 0
                    ? `🎁 Offer: *${escapeMarkdown(appliedOfferTitle || 'Applied Offer')}* (${appliedDiscountPercent}% OFF)\n` +
                    `💰 Payable: *₹${formatInr(discountedRenewalPrice)}*\n\n`
                    : '\n') +
                `Ab payment screenshot photo/document bhejiye.`,
                {
                    parse_mode: 'Markdown',
                    ...Markup.inlineKeyboard([
                        [withStyle(Markup.button.callback('❌ Cancel Upload', 'cancel_payment_upload'), 'danger')],
                        [withStyle(Markup.button.callback('🏠 Main Menu', 'back_to_main'), 'primary')],
                    ]),
                }
            );

            if (callbackChatId && callbackMessageId) {
                scheduleDeleteMessage(ctx.telegram, callbackChatId, callbackMessageId);
            }
        } catch (err) {
            logger.error(`renew_request error: ${err.message}`);
        }
    });

    const clearPaymentFlowMetaUnset = {
        'meta.paymentCategory': '',
        'meta.paymentProofReadyForCategory': '',
        'meta.paymentFlowType': '',
        'meta.renewalPlanId': '',
        'meta.paymentSelectedPlanId': '',
        'meta.latestPaymentProof': '',
    };

    const onPaymentProofReceived = async (ctx, sourceType) => {
        if (ctx.chat?.type !== 'private') return;

        const telegramId = ctx.from?.id;
        if (!telegramId) return;

        // Lock per user to avoid multiple concurrent screenshot handlers creating
        // multiple pending payment Requests when the user forwards many photos.
        if (!acquirePaymentProofLock(telegramId)) {
            await ctx.reply('⏳ Screenshot processing already in progress. Please wait.');
            return;
        }

        try {
            const userDoc = await User.findOne({ telegramId: ctx.from.id });
        if (getUserFlowState(userDoc) !== USER_FLOW_STATE.AWAITING_PAYMENT_SCREENSHOT) return;

        const category = normalizePlanCategory(userDoc?.meta?.paymentCategory);
        const paymentFlowType = String(userDoc?.meta?.paymentFlowType || 'new_request');
        const renewalPlanId = userDoc?.meta?.renewalPlanId || null;
        const categoryLabel = getPlanCategoryLabel(category);

        let fileId;
        let fileUniqueId;

        if (sourceType === 'photo') {
            const photos = ctx.message?.photo || [];
            const bestPhoto = photos[photos.length - 1];
            fileId = bestPhoto?.file_id;
            fileUniqueId = bestPhoto?.file_unique_id;
        } else {
            fileId = ctx.message?.document?.file_id;
            fileUniqueId = ctx.message?.document?.file_unique_id;
        }

        if (!fileId) {
            await ctx.reply('❌ Invalid screenshot. Please send a clear image.');
            return;
        }

        const latestStored = userDoc?.meta?.latestPaymentProof;
        if (
            fileUniqueId
            && latestStored?.fileUniqueId === fileUniqueId
            && normalizePlanCategory(latestStored?.category) === category
        ) {
            await ctx.reply(
                `ℹ️ Ye screenshot pehle receive ho chuka hai.\n\n` +
                `Agar naya proof bhejna hai to *alag image* bhejiye, ya *Cancel Upload* use karein aur dubara flow start karein.`,
                { parse_mode: 'Markdown' }
            );
            return;
        }

        const isRenewalFlow = paymentFlowType === 'renewal' && renewalPlanId;

        if (!isRenewalFlow) {
            const pendingReq = await Request.findOne({
                telegramId: ctx.from.id,
                status: 'pending',
                requestCategory: category,
            });
            if (pendingReq) {
                await User.findOneAndUpdate(
                    { telegramId: ctx.from.id },
                    buildSetUserFlowUpdate(USER_FLOW_STATE.IDLE, {}, clearPaymentFlowMetaUnset)
                );
                await ctx.reply(
                    `⏳ *${escapeMarkdown(categoryLabel)}* ke liye request pehle se pending hai.\n\n` +
                    `Admin approval ka wait karein — bar bar screenshot bhejne ki zarurat nahi.`,
                    { parse_mode: 'Markdown' }
                );
                return;
            }

            const activeSub = await Subscription.findOne({
                telegramId: ctx.from.id,
                status: 'active',
                expiryDate: { $gt: new Date() },
                planCategory: category,
            });
            if (activeSub) {
                await User.findOneAndUpdate(
                    { telegramId: ctx.from.id },
                    buildSetUserFlowUpdate(USER_FLOW_STATE.IDLE, {}, clearPaymentFlowMetaUnset)
                );
                await ctx.reply(
                    `✅ *${escapeMarkdown(categoryLabel)}* subscription already active hai.\n\n` +
                    `Bar bar screenshot bhejne ki zarurat nahi.`,
                    { parse_mode: 'Markdown' }
                );
                return;
            }
        }

        const now = Date.now();
        const rateInfo = userDoc?.meta?.paymentProofRateLimit || {};
        const blockedUntilMs = rateInfo?.blockedUntil ? new Date(rateInfo.blockedUntil).getTime() : 0;
        if (blockedUntilMs && now < blockedUntilMs) {
            const unblockTime = new Date(blockedUntilMs).toLocaleString('en-IN');
            await ctx.reply(
                `⏳ Screenshot upload temporarily paused due to too many attempts.\n\n` +
                `Please try again after: *${escapeMarkdown(unblockTime)}*`,
                { parse_mode: 'Markdown' }
            );
            return;
        }

        const windowStartMs = rateInfo?.windowStart ? new Date(rateInfo.windowStart).getTime() : 0;
        const isInsideWindow = windowStartMs && (now - windowStartMs) < PAYMENT_PROOF_SPAM_WINDOW_MINUTES * 60 * 1000;
        const nextCount = isInsideWindow ? Number(rateInfo?.count || 0) + 1 : 1;
        const nextWindowStart = isInsideWindow ? new Date(windowStartMs) : new Date(now);

        if (nextCount > PAYMENT_PROOF_MAX_ATTEMPTS) {
            const cooldownUntil = new Date(now + PAYMENT_PROOF_COOLDOWN_MINUTES * 60 * 1000);
            await User.findOneAndUpdate(
                { telegramId: ctx.from.id },
                {
                    $set: {
                        'meta.paymentProofRateLimit': {
                            windowStart: nextWindowStart,
                            count: nextCount,
                            blockedUntil: cooldownUntil,
                        },
                    },
                }
            );

            logger.warn(`Payment proof cooldown applied for ${ctx.from.id} until ${cooldownUntil.toISOString()}`);
            await ctx.reply(
                `🚫 Too many screenshot attempts detected.\n\n` +
                `Please wait *${PAYMENT_PROOF_COOLDOWN_MINUTES} minutes* and try again.`,
                { parse_mode: 'Markdown' }
            );
            return;
        }

        await User.findOneAndUpdate(
            { telegramId: ctx.from.id },
            {
                $set: {
                    'meta.paymentProofRateLimit': {
                        windowStart: nextWindowStart,
                        count: nextCount,
                        blockedUntil: null,
                    },
                },
            }
        );

        const safeName = escapeMarkdown(userDoc.name || ctx.from.first_name || 'User');
        const safeUsername = userDoc.username ? `@${escapeMarkdown(userDoc.username)}` : 'N/A';

        if (paymentFlowType === 'renewal' && renewalPlanId) {
            const user = await findOrCreateUser(ctx.from);
            const plan = await Plan.findById(renewalPlanId);
            if (!plan) {
                await User.findOneAndUpdate(
                    { telegramId: ctx.from.id },
                    buildSetUserFlowUpdate(
                        USER_FLOW_STATE.IDLE,
                        {},
                        {
                            'meta.paymentCategory': '',
                            'meta.paymentFlowType': '',
                            'meta.renewalPlanId': '',
                            'meta.paymentSelectedPlanId': '',
                        }
                    )
                );
                await ctx.reply('❌ Renewal plan not found. Please open status and retry renewal.');
                return;
            }

            const renewalCategory = normalizePlanCategory(plan.category || category);
            if (renewalCategory !== category) {
                await User.findOneAndUpdate(
                    { telegramId: ctx.from.id },
                    buildSetUserFlowUpdate(
                        USER_FLOW_STATE.IDLE,
                        {},
                        {
                            'meta.paymentCategory': '',
                            'meta.paymentFlowType': '',
                            'meta.renewalPlanId': '',
                            'meta.paymentSelectedPlanId': '',
                        }
                    )
                );
                await ctx.reply('❌ Renewal category mismatch. Please retry renewal from status.');
                return;
            }

            const existingPending = await Request.findOne({
                telegramId: ctx.from.id,
                status: 'pending',
                requestCategory: renewalCategory,
            });
            if (existingPending) {
                await User.findOneAndUpdate(
                    { telegramId: ctx.from.id },
                    buildSetUserFlowUpdate(
                        USER_FLOW_STATE.IDLE,
                        {},
                        {
                            'meta.paymentCategory': '',
                            'meta.paymentFlowType': '',
                            'meta.renewalPlanId': '',
                            'meta.paymentSelectedPlanId': '',
                        }
                    )
                );
                await ctx.reply(
                    `⏳ *${escapeMarkdown(getPlanCategoryLabel(renewalCategory))} renewal pending hai!*\n\n` +
                    `Admin approval ka wait kijiye.`,
                    { parse_mode: 'Markdown' }
                );
                return;
            }

            const renewalReq = await Request.create({
                userId: user._id,
                telegramId: ctx.from.id,
                status: 'pending',
                requestCategory: renewalCategory,
                selectedPlanId: plan._id,
                paymentProof: {
                    fileId,
                    fileUniqueId,
                    sourceType,
                    logMessageId: null,
                },
            });

            const consumedOffer = await consumeOneTimeUserOffer(ctx.from.id, renewalReq._id);
            const bestOffer = await getBestPublicOffer(plan._id);
            const privateDiscountPercent = Number(consumedOffer?.discountPercent || 0);
            const publicDiscountPercent = Number(bestOffer?.discountPercent || 0);
            const appliedDiscountPercent = privateDiscountPercent > 0
                ? privateDiscountPercent
                : publicDiscountPercent;
            const appliedOfferTitle = privateDiscountPercent > 0
                ? consumedOffer?.title
                : bestOffer?.title;
            const discountedPlanPrice = appliedDiscountPercent > 0
                ? getDiscountedPrice(plan.price, appliedDiscountPercent)
                : plan.price;
            if (consumedOffer) {
                await Request.findByIdAndUpdate(renewalReq._id, {
                    appliedUserOffer: {
                        offerId: consumedOffer._id,
                        title: consumedOffer.title,
                        discountPercent: consumedOffer.discountPercent,
                    },
                });
            } else if (bestOffer?.discountPercent > 0) {
                await Request.findByIdAndUpdate(renewalReq._id, {
                    appliedUserOffer: {
                        offerId: bestOffer._id,
                        title: bestOffer.title,
                        discountPercent: Number(bestOffer.discountPercent || 0),
                    },
                });
            }

            await User.findByIdAndUpdate(user._id, {
                ...buildSetUserFlowUpdate(
                    USER_FLOW_STATE.IDLE,
                    {},
                    {
                        'meta.latestPaymentProof': '',
                        'meta.paymentProofReadyForCategory': '',
                        'meta.paymentCategory': '',
                        'meta.paymentFlowType': '',
                        'meta.renewalPlanId': '',
                        'meta.paymentSelectedPlanId': '',
                    }
                ),
            });

            const safePlanName = escapeMarkdown(plan.name);
            const renewalLogCaption =
                `🔄 *Renewal Request*\n\n` +
                `📦 Category: *${escapeMarkdown(categoryLabel)}*\n` +
                `👤 Name: ${safeName}\n` +
                `🆔 ID: \`${ctx.from.id}\`\n` +
                `📛 Username: ${safeUsername}\n` +
                (plan.price && appliedDiscountPercent > 0
                    ? `🎁 Offer: *${escapeMarkdown(appliedOfferTitle || 'Applied Offer')}* (${appliedDiscountPercent}% OFF)\n` +
                    `💰 Price: ~₹${formatInr(plan.price)}~ → *₹${formatInr(discountedPlanPrice)}*\n`
                    : '') +
                `📋 Plan: ${safePlanName} (${plan.durationDays} days${plan.price ? ` · ₹${formatInr(plan.price)}` : ''})\n` +
                `🕒 Time: ${new Date().toLocaleString('en-IN')}`;

            const renewalKeyboard = {
                inline_keyboard: [[
                    withStyle({ text: '✅ Approve', callback_data: `approve_${renewalReq._id}_${plan._id}` }, 'success'),
                    withStyle({ text: '❌ Reject', callback_data: `reject_${renewalReq._id}` }, 'danger'),
                ]],
            };

            let logMsg;
            try {
                logMsg = sourceType === 'photo'
                    ? await bot.telegram.sendPhoto(
                        process.env.LOG_CHANNEL_ID,
                        fileId,
                        {
                            caption: renewalLogCaption,
                            parse_mode: 'Markdown',
                            reply_markup: renewalKeyboard,
                        }
                    )
                    : await bot.telegram.sendDocument(
                        process.env.LOG_CHANNEL_ID,
                        fileId,
                        {
                            caption: renewalLogCaption,
                            parse_mode: 'Markdown',
                            reply_markup: renewalKeyboard,
                        }
                    );
            } catch (err) {
                logger.error(`renewal log channel send error: ${err.message}`);
                logMsg = await bot.telegram.sendMessage(
                    process.env.LOG_CHANNEL_ID,
                    `${renewalLogCaption}\n\n⚠️ (Could not attach file; see user chat.)`,
                    { parse_mode: 'Markdown', reply_markup: renewalKeyboard }
                );
            }

            await Request.findByIdAndUpdate(renewalReq._id, {
                logMessageId: logMsg.message_id,
                'paymentProof.logMessageId': logMsg.message_id,
            });

            await ctx.reply(
                `✅ *${escapeMarkdown(categoryLabel)} renewal request submitted!*\n\n` +
                `📋 Plan: *${escapeMarkdown(plan.name)}* (${plan.durationDays} days${plan.price ? ` · ₹${formatInr(plan.price)}` : ''})\n` +
                (plan.price && appliedDiscountPercent > 0
                    ? `🎁 *Offer applied:* ${escapeMarkdown(appliedOfferTitle || 'Applied Offer')} (${appliedDiscountPercent}% OFF)\n` +
                    `💰 Price: ~₹${formatInr(plan.price)}~ → *₹${formatInr(discountedPlanPrice)}*\n\n`
                    : '\n') +
                `Admin screenshot verify karke approval denge. Approval ke baad isi category plan me days add honge.`,
                { parse_mode: 'Markdown' }
            );
            return;
        }

        await User.findOneAndUpdate(
            { telegramId: ctx.from.id },
            buildSetUserFlowUpdate(
                USER_FLOW_STATE.AWAITING_PAYMENT_SCREENSHOT,
                {
                    'meta.paymentProofReadyForCategory': category,
                    'meta.latestPaymentProof': {
                        fileId,
                        fileUniqueId,
                        sourceType,
                        logMessageId: null,
                        category,
                        uploadedAt: new Date(),
                    },
                },
            )
        );

        await submitPremiumRequest(ctx, category);
        } finally {
            releasePaymentProofLock(telegramId);
        }
    };

    const maybePromptCheckPlansBeforeScreenshot = async (ctx) => {
        if (ctx.chat?.type !== 'private') return false;

        const userId = ctx.from?.id;
        if (!userId) return false;

        const userDoc = await User.findOne({ telegramId: userId });
        if (getUserFlowState(userDoc) === USER_FLOW_STATE.AWAITING_PAYMENT_SCREENSHOT) return false;
        // Don't hijack users who are entering UPI for seller withdrawal (handled in handlers.js).
        if (getUserFlowState(userDoc) === USER_FLOW_STATE.AWAITING_SELLER_UPI) return false;

        await ctx.reply(
            `⚠️ Screenshot upload karne se pehle *Check Plans* me category select karein.\n\n` +
            `Flow: *Check Plans* → category choose karein → plan ke *Paid* button pe tap karein → screenshot upload karein.`,
            {
                parse_mode: 'Markdown',
                ...Markup.inlineKeyboard([
                    [withStyle(Markup.button.callback('📋 Check Plans', 'check_plans'), 'success')],
                    [withStyle(Markup.button.callback('🏠 Main Menu', 'back_to_main'), 'primary')],
                ]),
            }
        );

        return true;
    };

    bot.on('photo', async (ctx, next) => {
        try {
            if (isBroadcastSessionActive(ctx.from?.id)) return next();
            if (await maybePromptCheckPlansBeforeScreenshot(ctx)) return next();
            await onPaymentProofReceived(ctx, 'photo');
        } catch (err) {
            logger.error(`payment proof photo handler error: ${err.message}`);
            await ctx.reply('❌ Screenshot process failed. Please try again.');
        }
        return next();
    });

    bot.on('document', async (ctx, next) => {
        try {
            if (isBroadcastSessionActive(ctx.from?.id)) return next();
            const mime = String(ctx.message?.document?.mime_type || '').toLowerCase();
            if (!mime.startsWith('image/')) return next();
            if (await maybePromptCheckPlansBeforeScreenshot(ctx)) return next();
            await onPaymentProofReceived(ctx, 'document');
        } catch (err) {
            logger.error(`payment proof document handler error: ${err.message}`);
            await ctx.reply('❌ Screenshot process failed. Please try again.');
        }
        return next();
    });
};

module.exports = { registerPaymentFlow };
