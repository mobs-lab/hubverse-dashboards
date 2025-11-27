// Forecast Periods Interfaces
// ---------------------------

// Coming from user-specified forecast periods, the options available for selecting,
// Filtering data to be within the time range.
export interface ForecastPeriodOptions {
  [forecastPeriodId: string]: ForecastPeriod;
}

export interface ForecastPeriod {
  forecastPeriodId: string; // Redundantly stored for data selector to use
  isDefaultSelected?: boolean;
  displayString: string;
  timeValue: string;
  startDate: Date;
  endDate: Date;
}

// Location (Spatial) Data Interfaces
// ---------------------------------------
export interface LocationMappingData {
  [locationCode: string]: {
    locationNameAlt?: string;
    locationName: string;
  };
}

// Modelling Task Target Interfaces
// ---------------------------------
export interface ModellingTaskTarget {
  [targetId: string]: {
    taskTargetDisplayString: string;
  };
}

// Target-Data Interfaces
// ---------------------------
export interface TargetData {
  [locationCode: string]: {
    [date: string]: {
      [targetId: string]: {
        observation: number | null;
        location_name?: string;
      };
    };
  };
}

// Historical Target-Data: Entire Collection organized by as_of date
// Structure: as_of -> date -> location -> target -> {observation, location_name}
export interface HistoricalTargetDataCollection {
  [asOfDate: string]: {
    [date: string]: {
      [locationCode: string]: {
        [targetId: string]: {
          observation: number | null;
          location_name?: string;
          target: string; // The target_id (redundant but kept for safety)
        };
      };
    };
  };
}

// Model Output Interfaces
// ---------------------------

// A single prediction data point from a model
export interface PredictionPointInterval {
  horizon: number | null;
  targetId?: string;
  value_median: number; // The median value, always needed, calculated by Python using quantiles and put directly here
  prediction_intervals: {
    [
      prediction_interval_name: string // "25", "50", "75", "90", used to display the PI level name in the visualization
    ]: SinglePredictionIntervalInfo;
  };
}

// Collection of all model output data, structured for the frontend
export interface ModelOutputCollection {
  [modelName: string]: {
    [locationCode: string]: {
      [referenceDate: string]: {
        predictions: {
          // Each date's prediction contains that day's median and the available PI information.
          [targetDate: string]: PredictionPointInterval;
        };
      };
    };
  };
}

export interface SinglePredictionIntervalInfo {
  pi_value_high: number; // The high value of the PI, upper bound for the shaded interval regions in visualizations
  pi_value_low: number; // The low value of the PI, lower bound for the same
}
