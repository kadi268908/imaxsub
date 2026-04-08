const { escapeMarkdown, escapeMarkdownInlineCode } = require('../src/utils/markdownEscape');

describe('markdownEscape', () => {
  it('escapeMarkdown escapes legacy Markdown special chars', () => {
    expect(escapeMarkdown('a\\b_c*d`e[f')).toBe('a\\\\b\\_c\\*d\\`e\\[f');
  });

  it('escapeMarkdown handles nullish', () => {
    expect(escapeMarkdown(null)).toBe('');
    expect(escapeMarkdown(undefined)).toBe('');
  });

  it('escapeMarkdownInlineCode escapes backticks and backslashes', () => {
    expect(escapeMarkdownInlineCode('a\\b`c')).toBe('a\\\\b\\`c');
  });
});
