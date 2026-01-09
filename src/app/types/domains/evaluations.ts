// ============================================
// Season Overview Time Range
// ============================================

/**
 * Time range options for Season Overview evaluations
 * Includes both static periods (e.g., "Season 2023-2024") and dynamic periods (e.g., "Last 4 Weeks")
 */
export interface EvaluationSeasonOverviewTimeRangeOption {
  /** Unique identifier for this time range */
  name: string;
  /** Human-readable display string */
  displayString: string;
  /** Whether this is a dynamic/relative time period */
  isDynamic: boolean;
  /** Optional sub-display text (e.g., actual date range for dynamic periods) */
  subDisplayValue?: string;
  /** Start date of this evaluation period */
  startDate: Date;
  /** End date of this evaluation period */
  endDate: Date;
}

// ============================================
// Boxplot Statistics
// ============================================

/**
 * Statistical summary for boxplot visualization
 * 
 * These statistics are computed from per-location averages:
 * 1. For each location, calculate average score (sum/count) across forecasts
 * 2. Collect all location averages into a list
 * 3. Compute percentiles from that list
 * 
 * This represents the distribution of model performance across locations.
 */
export interface BoxplotStats {
  /** 5th percentile of location averages */
  q05: number;
  /** 25th percentile (first quartile) of location averages */
  q25: number;
  /** Median (50th percentile) of location averages */
  median: number;
  /** 75th percentile (third quartile) of location averages */
  q75: number;
  /** 95th percentile of location averages */
  q95: number;
  /** Minimum location average */
  min: number;
  /** Maximum location average */
  max: number;
  /** Mean of location averages */
  mean: number;
  /** Number of locations in this distribution */
  count: number;
}

// ============================================
// Pre-calculated Evaluation Data
// ============================================

/**
 * Pre-calculated and aggregated evaluation data structure
 * This is the main interface for evaluation data consumed by the frontend
 * 
 * Data is organized hierarchically: Season → Target → Metric → Model → Aggregates
 * This structure allows efficient lookup for different visualization contexts
 */
export interface AppDataEvaluationsPrecalculated {
  /**
   * IQR and boxplot statistics for score distributions
   * Used by: Season Overview boxplot charts (WIS/Baseline, MAPE)
   * 
   * Structure: season → target → metric → model → horizonKey → BoxplotStats
   * horizonKey examples: "0,1,2,3" (all horizons), "0" (single horizon)
   */
  iqr: {
    [seasonId: string]: {
      [targetId: string]: {
        [metric: string]: {
          // metric: "WIS/Baseline" | "MAPE"
          [model: string]: {
            [horizonKey: string]: BoxplotStats;
          };
        };
      };
    };
  };

  /**
   * Location-level score aggregates for geographic map visualization
   * Used by: Season Overview Location Map (hot map)
   * 
   * Structure: season → target → metric → model → locationCode → horizon → {sum, count}
   * Allows computing average scores per location for map coloring
   * 
   * Note: Coverage uses 95% prediction interval level by default.
   */
  locationMap_aggregates: {
    [seasonId: string]: {
      [targetId: string]: {
        [metric: string]: {
          // metric: "WIS/Baseline" | "MAPE" | "Coverage"
          [model: string]: {
            [locationCode: string]: {
              [horizon: string]: { 
                sum: number; 
                count: number 
              };
            };
          };
        };
      };
    };
  };

  /**
   * Detailed coverage data aggregated by horizon and prediction interval level
   * Used by: Season Overview Coverage chart
   * 
   * Structure: season → target → model → horizon → piLevel → {sum, count}
   * piLevel examples: "50", "95" (representing 50% PI, 95% PI)
   * 
   * Note: the "sum" is a Percentage, transformed during Python data processing
   */
  detailedCoverage_aggregates: {
    [seasonId: string]: {
      [targetId: string]: {
        [model: string]: {
          [horizon: number]: {
            [pi_level: string]: { 
              sum: number; 
              count: number 
            };
          };
        };
      };
    };
  };
}

// ============================================
// Raw Score Data (Single Model View)
// ============================================

/**
 * Individual score record for a specific forecast instance
 */
export interface ScoreRecord {
  /** When the forecast was made (ISO date string) */
  referenceDate: string;
  /** What date was being predicted (ISO date string) */
  targetEndDate: string;
  /** The calculated score value (WIS, WIS/Baseline, or MAPE) */
  score: number;
}

/**
 * Raw, unaggregated evaluation scores for Single Model detailed view
 * Used by: Single Model Score Line Chart
 * 
 * This provides granular, time-series score data for plotting individual scores over time
 * Structure: season → target → metric → model → stateCode → horizon → ScoreRecord[]
 */
export interface AppDataEvaluationsSingleModelRawScores {
  [seasonId: string]: {
    [targetId: string]: {
      [metric: string]: {
        // metric: "WIS/Baseline" | "MAPE"
        [model: string]: {
          [stateNum: string]: {
            [horizon: number]: ScoreRecord[];
          };
        };
      };
    };
  };
}
