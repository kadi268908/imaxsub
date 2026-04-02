const Plan = require('../src/models/Plan');
const Subscription = require('../src/models/Subscription');
const Request = require('../src/models/Request');

const CATEGORY_ENUM = ['movie', 'desi', 'non_desi'];

describe('mongoose schemas — category enums', () => {
  it('Plan.category allows movie, desi, non_desi', () => {
    expect(Plan.schema.path('category').enumValues).toEqual(CATEGORY_ENUM);
  });

  it('Subscription.planCategory matches Plan', () => {
    expect(Subscription.schema.path('planCategory').enumValues).toEqual(CATEGORY_ENUM);
  });

  it('Request.requestCategory matches Plan', () => {
    expect(Request.schema.path('requestCategory').enumValues).toEqual(CATEGORY_ENUM);
  });
});
