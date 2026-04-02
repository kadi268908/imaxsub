const sessions = new Map();

const toKey = (telegramId) => String(telegramId || '');

const startBroadcastSession = (telegramId, session) => {
    const key = toKey(telegramId);
    if (!key) return;
    sessions.set(key, {
        action: 'broadcast',
        ...session,
        startedAt: new Date(),
    });
};

const getBroadcastSession = (telegramId) => {
    const key = toKey(telegramId);
    if (!key) return null;
    return sessions.get(key) || null;
};

const isBroadcastSessionActive = (telegramId) => {
    return Boolean(getBroadcastSession(telegramId));
};

const consumeBroadcastSession = (telegramId) => {
    const key = toKey(telegramId);
    if (!key) return null;
    const session = sessions.get(key) || null;
    if (session) sessions.delete(key);
    return session;
};

const clearBroadcastSession = (telegramId) => {
    const key = toKey(telegramId);
    if (!key) return false;
    return sessions.delete(key);
};

module.exports = {
    startBroadcastSession,
    getBroadcastSession,
    isBroadcastSessionActive,
    consumeBroadcastSession,
    clearBroadcastSession,
};
