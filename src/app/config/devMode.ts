/**
 * Development Mode Configuration
 *
 * This utility manages development mode settings for the application.
 * Development mode is determined by the metadata.json flag set during 
 * the Python data processing step, allowing users to test production builds 
 * locally with `npm run dev` before deploying.
 */


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
 * Returns the development mode flag from metadata
 */
export const isDevMode = (): boolean => {
  return userDevModeEnabled;
};

/**
 * Get the appropriate data path based on dev mode
 * In dev mode, uses /test-data-output, otherwise uses /data
 */
export const getDataPath = (): string => {
  return isDevMode() ? '/test-data-output' : '/data';
};
