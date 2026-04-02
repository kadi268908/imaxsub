/**
 * Telegram Bot API legacy Markdown (parse_mode: 'Markdown') escaping.
 * Use for any user-controlled or DB text interpolated into messages.
 */

const escapeMarkdown = (value) => {
  return String(value ?? '').replace(/([_*`\[])/g, '\\$1');
};

/** Safe inside legacy Markdown `inline code` spans. */
const escapeMarkdownInlineCode = (value) => {
  return String(value ?? '')
    .replace(/\\/g, '\\\\')
    .replace(/`/g, '\\`');
};

module.exports = {
  escapeMarkdown,
  escapeMarkdownInlineCode,
};
