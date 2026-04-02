const { escapeMarkdown, escapeMarkdownInlineCode } = require('../src/utils/markdownEscape');

describe('markdownEscape', () => {
  it('escapeMarkdown escapes legacy Markdown special chars', () => {
    expect(escapeMarkdown('a_b*c`d[e')).toBe('a\\_b\\*c\\`d\\[e');
  });

  it('escapeMarkdown handles nullish', () => {
    expect(escapeMarkdown(null)).toBe('');
    expect(escapeMarkdown(undefined)).toBe('');
  });

  it('escapeMarkdownInlineCode escapes backticks and backslashes', () => {
    expect(escapeMarkdownInlineCode('a\\b`c')).toBe('a\\\\b\\`c');
  });
});
