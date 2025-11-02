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
        observation: number;
      };
    };
  };
}

// Target Data Collection partitioned by forecast period
export interface TargetDataCollection {
  [forecastPeriodId: string]: TargetData;
}

// Historical Target-Data: Entire Collection organized by associated Date (as_of date)
export interface HistoricalTargetDataCollection {
  [asOfDate: string]: TargetData;
}

// Model Output Interfaces
// ---------------------------
// A single round's model output containing all default value and the user-specified prediction intervals.
export interface ModelOutputRoundDataPoint {
  value: number; // The default value to display as dots, usually median
  predictionIntervalData: ModelOutputPredictionInterval[];
}

// One prediction interval's info
export interface ModelOutputPredictionInterval {
  predictionIntervalName: string;
  predictionIntervalQuantileLow: number;
  predictionIntervalQuantileHigh: number;
}

// Collection of all model output data
export interface ModelOutputCollection {
  [forecastPeriodId: string]: {
    [modelName: string]: {
      [locationCode: string]: {
        [referenceDate: string]: {
          [horizon: number]: {
            [targetId: string]: ModelOutputRoundDataPoint;
          };
        };
      };
    };
  };
}
