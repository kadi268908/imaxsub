/**
 * PM2 process file (optional).
 * Start: pm2 start ecosystem.config.cjs
 *
 * On Windows you may still see "spawn wmic ENOENT" in pm2.log — that comes from
 * PM2's CPU/RAM monitoring, not from this app. Safe to ignore if the app is online.
 */
module.exports = {
  apps: [
    {
      name: 'telegram-sub-bot',
      script: 'src/index.js',
      cwd: __dirname,
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '512M',
      env: {
        NODE_ENV: 'production',
      },
    },
  ],
};
