/**
 * Development Mode Configuration
 *
 * This utility manages development mode settings for the application.
 * Development mode can be enabled in two ways:
 * 1. NODE_ENV === 'development' (npm run dev)
 * 2. User sets developmentMode: true in their config YAML (persists in production build)
 */

// Check if running in Next.js development mode
const isNextDevMode = process.env.NODE_ENV === 'development';

// State for user-configured dev mode (will be set from metadata)
let userDevModeEnabled = false;

/**
 * Initialize dev mode from metadata
 * Should be called after metadata is loaded
 */
export const initializeDevMode = (metadata: any) => {
  userDevModeEnabled = metadata?.features?.developmentMode ?? false;
};

/**
 * Check if development mode is enabled
 * Returns true if either Next.js dev mode OR user-configured dev mode is active
 */
export const isDevMode = (): boolean => {
  return isNextDevMode || userDevModeEnabled;
};

/**
 * Get the appropriate data path based on dev mode
 * In dev mode, uses /test-data-output, otherwise uses /data
 */
export const getDataPath = (): string => {
  return isDevMode() ? '/test-data-output' : '/data';
};

/**
 * Check if running in Next.js development mode specifically
 */
export const isNextJsDevMode = (): boolean => {
  return isNextDevMode;
};

/**
 * Check if user-configured development mode is enabled
 */
export const isUserDevMode = (): boolean => {
  return userDevModeEnabled;
};

/**
 * Get development mode status details
 */
export const getDevModeStatus = () => {
  return {
    isDevMode: isDevMode(),
    isNextJsDevMode: isNextDevMode,
    isUserDevMode: userDevModeEnabled,
    dataPath: getDataPath(),
  };
};
