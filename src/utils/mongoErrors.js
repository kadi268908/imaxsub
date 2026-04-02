/**
 * Detect MongoDB errors where multi-document transactions are not available
 * (standalone / no replica set). Safe to retry the same logical work without a transaction.
 */
const isTransactionUnsupportedError = (err) => {
  if (!err) return false;
  const msg = String(err.message || err).toLowerCase();
  const code = err.code;
  const codeName = String(err.codeName || '').toLowerCase();

  if (code === 20 && codeName === 'illegaloperation') return true;
  if (code === 251) return true; // Transaction numbers are only allowed on a replica set member or mongos
  if (msg.includes('transaction numbers are only allowed')) return true;
  if (msg.includes('multi-document transactions')) return true;
  if (msg.includes('replica set') && msg.includes('transaction')) return true;
  if (msg.includes('standalone') && msg.includes('transaction')) return true;
  if (msg.includes('mongos') && msg.includes('transaction')) return true;

  return false;
};

module.exports = { isTransactionUnsupportedError };
