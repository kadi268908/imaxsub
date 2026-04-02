/**
 * Global defaults for tests that read process.env at module load time.
 * Individual tests may override via jest.resetModules() + require().
 */
process.env.NODE_ENV = 'test';
process.env.MOVIE_PREMIUM_GROUP_ID = process.env.MOVIE_PREMIUM_GROUP_ID || '-1001';
process.env.DESI_PREMIUM_GROUP_ID = process.env.DESI_PREMIUM_GROUP_ID || '-1002';
process.env.NON_DESI_PREMIUM_GROUP_ID = process.env.NON_DESI_PREMIUM_GROUP_ID || '-1003';
