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

// Target Data Collection partitioned by forecast period
export interface TargetDataCollection {
  [forecastPeriodId: string]: TargetData;
}

// Historical Target-Data: Entire Collection organized by as_of date
// Structure: as_of -> date -> location -> {observation, target}
export interface HistoricalTargetDataCollection {
  [asOfDate: string]: {
    [date: string]: {
      [locationCode: string]: {
        observation: number | null;
        location_name?: string;
        target?: string;
      };
    };
  };
}

// Model Output Interfaces
// ---------------------------

// A single prediction data point from a model
export interface PredictionPoint {
  horizon: number | null;
  targetId?: string;
  // Allows for dynamic quantile keys like 'q0_025', 'q0_5', 'q0_975'
  [key: string]: number | string | null | undefined;
}

// Collection of all model output data, structured for the frontend
export interface ModelOutputCollection {
  [forecastPeriodId: string]: {
    [modelName: string]: {
      [locationCode: string]: {
        [referenceDate: string]: {
          predictions: {
            [targetDate: string]: PredictionPoint;
          };
        };
      };
    };
  };
}
