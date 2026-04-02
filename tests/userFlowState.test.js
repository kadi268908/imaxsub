const {
  USER_FLOW_STATE,
  getUserFlowState,
  buildSetUserFlowUpdate,
} = require('../src/utils/userFlowState');

describe('userFlowState', () => {
  it('getUserFlowState returns idle by default', () => {
    expect(getUserFlowState({})).toBe(USER_FLOW_STATE.IDLE);
  });

  it('getUserFlowState reads meta.flowState when valid', () => {
    expect(getUserFlowState({ meta: { flowState: 'awaiting_payment_screenshot' } })).toBe(
      USER_FLOW_STATE.AWAITING_PAYMENT_SCREENSHOT
    );
  });

  it('getUserFlowState infers from legacy flags', () => {
    expect(getUserFlowState({ meta: { awaitingPaymentScreenshot: true } })).toBe(
      USER_FLOW_STATE.AWAITING_PAYMENT_SCREENSHOT
    );
    expect(getUserFlowState({ meta: { awaitingSellerWithdrawalUpi: true } })).toBe(
      USER_FLOW_STATE.AWAITING_SELLER_UPI
    );
  });

  it('buildSetUserFlowUpdate sets flow fields', () => {
    const u = buildSetUserFlowUpdate(USER_FLOW_STATE.AWAITING_PAYMENT_SCREENSHOT);
    expect(u.$set['meta.flowState']).toBe(USER_FLOW_STATE.AWAITING_PAYMENT_SCREENSHOT);
    expect(u.$set['meta.awaitingPaymentScreenshot']).toBe(true);
    expect(u.$set['meta.awaitingSellerWithdrawalUpi']).toBe(false);
  });

  it('buildSetUserFlowUpdate invalid state becomes idle', () => {
    const u = buildSetUserFlowUpdate('not_a_real_state');
    expect(u.$set['meta.flowState']).toBe(USER_FLOW_STATE.IDLE);
  });

  it('buildSetUserFlowUpdate supports $unset', () => {
    const u = buildSetUserFlowUpdate(USER_FLOW_STATE.IDLE, {}, { 'meta.foo': '' });
    expect(u.$unset).toEqual({ 'meta.foo': '' });
  });
});
