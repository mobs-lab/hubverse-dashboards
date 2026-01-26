// ============================================
// Forecast Periods
// ============================================

/**
 * Collection of available forecast periods indexed by their ID
 * Used by: Date range selector, time filtering
 */
export interface ForecastPeriodOptions {
  [forecastPeriodId: string]: ForecastPeriod;
}

/**
 * Individual forecast period configuration
 * Represents a specific time window for forecasting (e.g., "Season 2023-2024", "Last 4 Weeks")
 */
export interface ForecastPeriod {
  /** Unique identifier (redundantly stored for selector efficiency) */
  forecastPeriodId: string;
  /** Whether this period should be selected by default */
  isDefaultSelected?: boolean;
  /** Human-readable name shown in UI */
  displayString: string;
  /** Formatted time value for display (e.g., "2023-08-01/2024-05-31") */
  timeValue: string;
  /** Period start date */
  startDate: Date;
  /** Period end date */
  endDate: Date;
}

/**
 * Forecast period option for dropdowns and selectors
 * Simplified version with index for array-based operations
 */
export interface ForecastPeriodOption {
  /** Unique identifier matching ForecastPeriod.forecastPeriodId */
  forecastPeriodID: string;
  /** Human-readable display name */
  displayString: string;
  /** Formatted time value (e.g., "2023-08-01/2024-05-31") */
  timeValue: string;
  /** Period start date */
  startDate: Date;
  /** Period end date */
  endDate: Date;
  /** Array index for UI ordering */
  index: number;
}

// ============================================
// Location (Spatial) Data
// ============================================

/**
 * Location mapping data indexed by location code
 * Maps location codes (e.g., "US", "01") to their human-readable names
 */
export interface LocationMappingData {
  [locationCode: string]: {
    /** Primary location name (e.g., "United States", "Alabama") */
    locationName: string;
    /** Alternative location name (e.g., abbreviations) */
    locationNameAlt?: string;
  };
}

// ============================================
// Modelling Task Targets
// ============================================

/**
 * Modelling task target configuration
 * Defines what outcome is being forecasted (e.g., hospital admissions, deaths)
 */
export interface ModellingTaskTarget {
  [targetId: string]: {
    /** Display string for this target in UI */
    taskTargetDisplayString: string;
  };
}

// ============================================
// Target Data (Ground Truth)
// ============================================

/**
 * Current target data (ground truth / observed values)
 * 
 * Structure: location → date → target → observation data
 * This is the primary data structure used by forecast visualizations
 */
export interface TargetData {
  [locationCode: string]: {
    [date: string]: {
      /** ISO date string (YYYY-MM-DD) */
      [targetId: string]: {
        /** Observed value (-1 indicates missing data) */
        observation: number | null;
        /** Optional location name for convenience */
        location_name?: string;
      };
    };
  };
}

/**
 * Historical target data collection organized by "as_of" date
 * Enables viewing what the data looked like at a specific point in time
 * 
 * Structure: as_of_date → date → location → target → observation data
 * Used by: Historical Target Data toggle feature
 */
export interface HistoricalTargetDataCollection {
  [asOfDate: string]: {
    /** ISO date string (YYYY-MM-DD) */
    [date: string]: {
      [locationCode: string]: {
        [targetId: string]: {
          /** Observed value at this as_of snapshot */
          observation: number | null;
          /** Optional location name */
          location_name?: string;
          /** Target ID (redundant but kept for data integrity) */
          target: string;
        };
      };
    };
  };
}

// ============================================
// Model Output (Predictions)
// ============================================

/**
 * A single prediction interval's bounds
 */
export interface SinglePredictionIntervalInfo {
  /** Upper bound of this prediction interval */
  pi_value_high: number;
  /** Lower bound of this prediction interval */
  pi_value_low: number;
}

/**
 * A single prediction data point from a model
 * Contains the median forecast and all configured prediction intervals
 */
export interface PredictionPointInterval {
  /** Forecast horizon (in time_unit units from reference_date) */
  horizon: number | null;
  /** KEPT FOR REFERENCE Target being predicted (for multi-target dashboards) */
  targetId?: string;
  /** Median (point) prediction value */
  value_median: number;
  /** 
   * Available prediction intervals for this forecast
   * Keyed by interval level (e.g., "25", "50", "90")
   */
  prediction_intervals: {
    [prediction_interval_name: string]: SinglePredictionIntervalInfo;
  };
}

/**
 * Complete collection of all model output data
 * 
 * Structure: model → location → reference_date → predictions → target_end_date → targetId
 * This nested structure enables efficient lookup by the forecast visualization components
 * and supports multiple targets for the same prediction date
 */
export interface ModelOutputCollection {
  [modelName: string]: {
    [locationCode: string]: {
      [referenceDate: string]: {
        /** ISO date string (YYYY-MM-DD) */
        predictions: {
          /** 
           * Predictions keyed by target_end_date, then by targetId
           * This allows multiple targets for the same date
           */
          [targetDate: string]: {
            [targetId: string]: PredictionPointInterval;
          };
        };
      };
    };
  };
}
