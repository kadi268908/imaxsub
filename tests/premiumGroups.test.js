describe('premiumGroups', () => {
  const KEYS = ['MOVIE_PREMIUM_GROUP_ID', 'DESI_PREMIUM_GROUP_ID', 'NON_DESI_PREMIUM_GROUP_ID', 'PREMIUM_GROUP_ID'];
  const snapshot = {};
  beforeAll(() => {
    KEYS.forEach((k) => {
      snapshot[k] = process.env[k];
    });
  });

  const load = () => {
    jest.resetModules();
    return require('../src/utils/premiumGroups');
  };

  afterEach(() => {
    KEYS.forEach((k) => {
      if (snapshot[k] === undefined) delete process.env[k];
      else process.env[k] = snapshot[k];
    });
  });

  it('normalizePlanCategory maps variants to canonical values', () => {
    process.env.MOVIE_PREMIUM_GROUP_ID = '-1';
    const { normalizePlanCategory } = load();
    expect(normalizePlanCategory('MOVIE')).toBe('movie');
    expect(normalizePlanCategory('non desi')).toBe('non_desi');
    expect(normalizePlanCategory('non-desi')).toBe('non_desi');
    expect(normalizePlanCategory(undefined)).toBe('movie');
    expect(normalizePlanCategory('unknown')).toBe('movie');
  });

  it('getGroupIdForCategory returns mapped env id', () => {
    process.env.MOVIE_PREMIUM_GROUP_ID = '-100movie';
    process.env.DESI_PREMIUM_GROUP_ID = '-100desi';
    process.env.NON_DESI_PREMIUM_GROUP_ID = '-100nd';
    delete process.env.PREMIUM_GROUP_ID;
    const { getGroupIdForCategory } = load();
    expect(getGroupIdForCategory('desi')).toBe('-100desi');
    expect(getGroupIdForCategory('non_desi')).toBe('-100nd');
  });

  it('getGroupIdForCategory falls back to PREMIUM_GROUP_ID', () => {
    delete process.env.MOVIE_PREMIUM_GROUP_ID;
    delete process.env.DESI_PREMIUM_GROUP_ID;
    delete process.env.NON_DESI_PREMIUM_GROUP_ID;
    process.env.PREMIUM_GROUP_ID = '-fallback';
    const { getGroupIdForCategory } = load();
    expect(getGroupIdForCategory('movie')).toBe('-fallback');
  });

  it('getAllPremiumGroupIds dedupes env values', () => {
    process.env.MOVIE_PREMIUM_GROUP_ID = '-a';
    process.env.DESI_PREMIUM_GROUP_ID = '-b';
    process.env.NON_DESI_PREMIUM_GROUP_ID = '-c';
    process.env.PREMIUM_GROUP_ID = '-a';
    const { getAllPremiumGroupIds } = load();
    const ids = getAllPremiumGroupIds();
    expect(ids.sort()).toEqual(['-a', '-b', '-c'].sort());
  });
});
