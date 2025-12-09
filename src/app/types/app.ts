// ============================================
// Application Loading States
// ============================================

/**
 * Tracks the loading state of different data slices in the application
 * Used by: DataProvider to manage async data loading
 */
export interface LoadingStates {
  /** Target data (ground truth) loading state */
  targetData: boolean;
  /** Model output (predictions) loading state */
  modelOutput: boolean;
  /** Location mapping loading state */
  locations: boolean;
  /** Historical target data loading state (lazy loaded) */
  historicalTargetData: boolean;
  /** Forecast period options loading state */
  forecastPeriodOptions: boolean;
  /** Evaluation scores loading state (lazy loaded) */
  evaluationScores: boolean;
  /** Detailed coverage data loading state (lazy loaded) */
  evaluationDetailedCoverage: boolean;
  /** Map shape data (GeoJSON/TopoJSON) loading state */
  locationShapeData: boolean;
}

// ============================================
// Common Type Aliases
// ============================================

/**
 * Location code identifier
 * @example "US" | "01" | "06" | "48"
 */
export type LocationCode = string;

/**
 * ISO date string format
 * @example "2024-01-15"
 */
export type IsoDateString = string;

/**
 * Forecast period identifier
 * @example "season-2023-2024" | "last-2-weeks" | "round-1"
 */
export type ForecastPeriodId = string;

/**
 * Target identifier
 * @example "covid19-admission" | "flu-hosp"
 */
export type TargetId = string;

/**
 * Model name identifier
 * @example "MOBS-GLEAM_COVID" | "CovidHub-baseline"
 */
export type ModelName = string;

/**
 * Horizon value (in time_unit units)
 * Negative = nowcast, 0 = same-day, positive = future forecast
 * @example -1 | 0 | 1 | 2 | 3
 */
export type Horizon = number;
