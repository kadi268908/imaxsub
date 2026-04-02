#!/usr/bin/env node
/**
 * Cross-platform syntax check for all JS under src/
 */
const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');

const srcDir = path.join(__dirname, '..', 'src');

const walk = (dir, out = []) => {
  for (const name of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, name.name);
    if (name.isDirectory()) walk(full, out);
    else if (name.isFile() && name.name.endsWith('.js')) out.push(full);
  }
  return out;
};

const files = walk(srcDir);
let failed = false;
for (const file of files) {
  const r = spawnSync(process.execPath, ['--check', file], { encoding: 'utf8' });
  if (r.status !== 0) {
    console.error(`Syntax error: ${file}`);
    if (r.stderr) console.error(r.stderr);
    failed = true;
  }
}
if (failed) process.exit(1);
console.log(`OK: ${files.length} file(s) under src/`);
