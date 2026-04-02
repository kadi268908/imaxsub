// src/services/supportService.js
// Simple external support: users use SUPPORT_CONTACT (Telegram @username or https URL).
// In-bot forum topics / tickets are not used.

const SUPPORT_CONTACT = process.env.SUPPORT_CONTACT || '@RahiImax_Bot';

/** No forum group — kept for backward compatibility; always falsy. */
const SUPPORT_GROUP_ID = undefined;

/**
 * Resolve SUPPORT_CONTACT to a clickable URL (https://… or t.me/…).
 */
const getSupportContactUrl = () => {
  const value = String(SUPPORT_CONTACT || '').trim();
  if (!value) return 'https://t.me/';
  if (/^https?:\/\//i.test(value)) return value;
  const username = value.startsWith('@') ? value.slice(1) : value;
  return `https://t.me/${username}`;
};

// ── Stubs: legacy forum-ticket API (nothing to do) ─────────────────────────

const getActiveTicket = async () => null;

const getTicketByTopicId = async () => null;

const getOpenTickets = async () => [];

const openTicket = async () => {
  throw new Error('In-bot support tickets are disabled. Set SUPPORT_CONTACT for external support.');
};

const forwardUserMessage = async () => {};

const forwardAdminReply = async () => {};

const closeTicket = async () => null;

module.exports = {
  getSupportContactUrl,
  openTicket,
  forwardUserMessage,
  forwardAdminReply,
  closeTicket,
  getActiveTicket,
  getTicketByTopicId,
  getOpenTickets,
  SUPPORT_CONTACT,
  SUPPORT_GROUP_ID,
};
