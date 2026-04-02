const { spawnSync } = require('child_process');
const path = require('path');

/**
 * Reuses scripts/verify-syntax.js — validates every file under src/ parses.
 */
describe('project syntax (src/**/*.js)', () => {
  it('passes node --check for all source files', () => {
    const script = path.join(__dirname, '..', 'scripts', 'verify-syntax.js');
    const r = spawnSync(process.execPath, [script], {
      encoding: 'utf8',
      cwd: path.join(__dirname, '..'),
    });
    expect(r.status).toBe(0);
    expect(r.stdout || '').toContain('OK:');
  });
});
