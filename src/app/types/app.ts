export interface LoadingStates {
  targetData: boolean;
  modelOutput: boolean;
  locations: boolean;
  historicalTargetData: boolean;
  forecastPeriodOptions: boolean;
  evaluationScores: boolean;
  evaluationDetailedCoverage: boolean;
  locationShapeData: boolean;
}

export type locationCode = string; // "US" or "01".."56"
export type IsoDate = string; // "YYYY-MM-DD"
export type forecastPeriodId = string; // "season-2023-2024" | "last-2-weeks" etc.
