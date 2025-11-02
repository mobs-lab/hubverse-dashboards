/**
 * Development Logger Utility
 *
 * Provides logging functions that only output when in development mode.
 * This helps keep production builds clean and improves performance.
 */

import { isDevMode } from '@/config/devMode';

type LogLevel = 'log' | 'info' | 'warn' | 'error' | 'debug';

/**
 * Internal logging function
 */
const log = (level: LogLevel, ...args: any[]) => {
  if (!isDevMode()) return;

  const timestamp = new Date().toISOString().split('T')[1].split('.')[0];
  const prefix = `[${timestamp}]`;

  switch (level) {
    case 'log':
      console.log(prefix, ...args);
      break;
    case 'info':
      console.info(prefix, 'ℹ️', ...args);
      break;
    case 'warn':
      console.warn(prefix, '⚠️', ...args);
      break;
    case 'error':
      console.error(prefix, '❌', ...args);
      break;
    case 'debug':
      console.debug(prefix, '🐛', ...args);
      break;
  }
};

/**
 * Development Logger
 *
 * Usage:
 * - logger.log('Starting initialization...')
 * - logger.info('Config loaded successfully')
 * - logger.warn('Missing optional field')
 * - logger.error('Failed to load data', error)
 * - logger.debug('Intermediate state:', state)
 */
export const logger = {
  /**
   * General logging
   */
  log: (...args: any[]) => log('log', ...args),

  /**
   * Informational messages
   */
  info: (...args: any[]) => log('info', ...args),

  /**
   * Warning messages
   */
  warn: (...args: any[]) => log('warn', ...args),

  /**
   * Error messages (always logged, even in production)
   */
  error: (...args: any[]) => {
    const timestamp = new Date().toISOString().split('T')[1].split('.')[0];
    const prefix = `[${timestamp}]`;
    console.error(prefix, '❌', ...args);
  },

  /**
   * Debug messages (verbose, only in dev mode)
   */
  debug: (...args: any[]) => log('debug', ...args),

  /**
   * Group logging (for organizing related logs)
   */
  group: (label: string, callback: () => void) => {
    if (!isDevMode()) return;
    console.group(label);
    callback();
    console.groupEnd();
  },

  /**
   * Collapsed group (starts collapsed)
   */
  groupCollapsed: (label: string, callback: () => void) => {
    if (!isDevMode()) return;
    console.groupCollapsed(label);
    callback();
    console.groupEnd();
  },

  /**
   * Table logging (for arrays/objects)
   */
  table: (data: any) => {
    if (!isDevMode()) return;
    console.table(data);
  },

  /**
   * Time measurement
   */
  time: (label: string) => {
    if (!isDevMode()) return;
    console.time(label);
  },

  /**
   * End time measurement
   */
  timeEnd: (label: string) => {
    if (!isDevMode()) return;
    console.timeEnd(label);
  },
};

/**
 * Convenience function for conditional logging
 */
export const logIf = (condition: boolean, ...args: any[]) => {
  if (condition && isDevMode()) {
    console.log(...args);
  }
};
