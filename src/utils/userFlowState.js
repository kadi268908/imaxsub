const USER_FLOW_STATE = {
    IDLE: 'idle',
    AWAITING_PAYMENT_SCREENSHOT: 'awaiting_payment_screenshot',
    AWAITING_SELLER_UPI: 'awaiting_seller_upi',
};

const VALID_STATES = new Set(Object.values(USER_FLOW_STATE));

const getUserFlowState = (userDoc) => {
    const state = String(userDoc?.meta?.flowState || '').trim();
    if (VALID_STATES.has(state)) return state;

    if (userDoc?.meta?.awaitingSellerWithdrawalUpi) {
        return USER_FLOW_STATE.AWAITING_SELLER_UPI;
    }

    if (userDoc?.meta?.awaitingPaymentScreenshot) {
        return USER_FLOW_STATE.AWAITING_PAYMENT_SCREENSHOT;
    }

    return USER_FLOW_STATE.IDLE;
};

const buildSetUserFlowUpdate = (state, setMeta = {}, unsetMeta = {}) => {
    const nextState = VALID_STATES.has(state) ? state : USER_FLOW_STATE.IDLE;

    const baseSet = {
        'meta.flowState': nextState,
        'meta.awaitingPaymentScreenshot': nextState === USER_FLOW_STATE.AWAITING_PAYMENT_SCREENSHOT,
        'meta.awaitingSellerWithdrawalUpi': nextState === USER_FLOW_STATE.AWAITING_SELLER_UPI,
    };

    const normalizedUnset = Array.isArray(unsetMeta)
        ? unsetMeta.reduce((acc, key) => {
            if (key) acc[key] = '';
            return acc;
        }, {})
        : (unsetMeta || {});

    const update = {
        $set: {
            ...baseSet,
            ...setMeta,
        },
    };

    if (Object.keys(normalizedUnset).length) {
        update.$unset = normalizedUnset;
    }

    return update;
};

module.exports = {
    USER_FLOW_STATE,
    getUserFlowState,
    buildSetUserFlowUpdate,
};
