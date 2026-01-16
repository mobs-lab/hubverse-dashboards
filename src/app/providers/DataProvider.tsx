'use client';

import { getDataPath, initializeDevMode } from '@/config/devMode';
import { setMapData } from '@/store/data-slices/domains/auxiliaryDataSlice';
import { DashboardConfig, setDashboardConfig } from '@/store/data-slices/domains/configSlice';
import { setAllCoreData } from '@/store/data-slices/domains/coreDataSlice';
import { setHistoricalTargetData } from '@/store/data-slices/domains/historicalTargetDataSlice';
import { initializeEvaluationSeasonOverviewSettings } from '@/store/data-slices/settings/SettingsSliceEvaluationSeasonOverview';
import { initializeEvaluationSingleModelSettings } from '@/store/data-slices/settings/SettingsSliceEvaluationSingleModel';
import { initializeForecastSettings } from '@/store/data-slices/settings/SettingsSliceForecastPage';
import { useAppDispatch } from '@/store/hooks';
import { LoadingStates } from '@/types/app';
import { ForecastPeriodOptions } from '@/types/domains/forecasting';
import { parseUTCDate } from '@/utils/date';
import { logger } from '@/utils/logger';
import React, { createContext, useCallback, useContext, useEffect, useRef, useState } from 'react';

interface DataContextType {
  loadingStates: LoadingStates;
  isFullyLoaded: boolean;
  updateLoadingState: (key: keyof LoadingStates, value: boolean) => void;
  initializationError: string | null;
  loadHistoricalDataIfNeeded: () => Promise<void>;
  currentSeasonId: string;
}

const DataContext = createContext<DataContextType | undefined>(undefined);

export const DataProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const dispatch = useAppDispatch();
  const [initializationError, setInitializationError] = useState<string | null>(null);
  const [currentSeasonId, setCurrentSeasonId] = useState<string>('');
  const initStartedRef = useRef(false);
  
  // Track loading attempts to prevent repetitive fetches
  const historicalDataAttemptedRef = useRef(false);

  const [loadingStates, setLoadingStates] = useState<LoadingStates>({
    forecastPeriodOptions: true,
    locations: true,
    targetData: true,
    modelOutput: true,
    locationShapeData: true,
    historicalTargetData: false, // Lazy load
    evaluationScores: false, // Lazy load
    evaluationDetailedCoverage: false, // Lazy load
  });

  const updateLoadingState = useCallback((key: keyof LoadingStates, value: boolean) => {
    setLoadingStates((prev) => ({ ...prev, [key]: value }));
  }, []);

  /**
   * Lazy load historical target data when toggle is enabled
   * Uses a ref to prevent repetitive fetch attempts on failure
   */
  const loadHistoricalDataIfNeeded = useCallback(async () => {
    // Guard: Already attempted (success or failure)
    if (historicalDataAttemptedRef.current) {
      return;
    }

    // Guard: Currently loading
    if (loadingStates.historicalTargetData) {
      return;
    }

    // Mark as attempted immediately to prevent concurrent calls
    historicalDataAttemptedRef.current = true;

    try {
      updateLoadingState('historicalTargetData', true);
      logger.info('Loading historical target data...');

      const historicalData = await loadHistoricalTargetData();
      
      // Validate data
      if (historicalData && typeof historicalData === 'object') {
        dispatch(setHistoricalTargetData(historicalData));
        logger.info('Historical target data loaded successfully');
      } else {
        logger.warn('Historical target data is empty or invalid format');
      }
    } catch (error) {
      // Log error but don't break the app - historical data is optional
      logger.error('Failed to load historical target data:', error);
      // Continue with empty data - components will handle null gracefully
    } finally {
      updateLoadingState('historicalTargetData', false);
    }
  }, [dispatch, loadingStates.historicalTargetData, updateLoadingState]);

  /**
   * Safe JSON fetch helper - handles network errors and JSON parsing errors
   */
  const safeFetch = async (url: string, errorContext: string): Promise<any> => {
    try {
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      const data = await response.json();
      return data;
    } catch (error) {
      if (error instanceof SyntaxError) {
        throw new Error(`${errorContext}: Invalid JSON response`);
      }
      throw new Error(`${errorContext}: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  };

  /**
   * Load and parse dashboard metadata from Python processor
   */
  const loadMetadata = async (): Promise<any> => {
    const dataPath = getDataPath();
    return safeFetch(`${dataPath}/metadata.json`, 'Failed to load metadata');
  };

  /**
   * Load map shape data (TopoJSON/GeoJSON)
   */
  const loadMapShapeData = async (shapeFileName?: string): Promise<any> => {
    try {
      const fileName = shapeFileName || 'states-10m.json';
      const response = await fetch(`/${fileName}`);
      if (!response.ok) {
        logger.warn(`Failed to load map data: ${response.statusText}`);
        return null;
      }
      return response.json();
    } catch (error) {
      logger.warn('Failed to load map shape data:', error);
      return null; // Map data is optional, don't break the app
    }
  };

  /**
   * Load target data
   */
  const loadTargetData = async (): Promise<any> => {
    const dataPath = getDataPath();
    return safeFetch(`${dataPath}/forecast/targetData.json`, 'Failed to load target data');
  };

  /**
   * Load model output
   */
  const loadModelOutput = async (): Promise<any> => {
    const dataPath = getDataPath();
    return safeFetch(`${dataPath}/forecast/modelOutputData.json`, 'Failed to load model output');
  };

  /**
   * Load historical target data (lazy loaded when toggle is enabled)
   */
  const loadHistoricalTargetData = async (): Promise<any> => {
    const dataPath = getDataPath();
    return safeFetch(`${dataPath}/forecast/historical-target-data.json`, 'Failed to load historical target data');
  };

  /**
   * Build config object from metadata
   */
  const buildConfigFromMetadata = (metadata: any): DashboardConfig => {
    // Parse forecast periods
    const forecastPeriodOptions: ForecastPeriodOptions = {};
    let defaultForecastPeriodId = '';

    if (metadata.forecastPeriods) {
      metadata.forecastPeriods.forEach((period: any) => {
        forecastPeriodOptions[period.forecastPeriodId] = {
          forecastPeriodId: period.forecastPeriodId,
          displayString: period.displayString,
          timeValue: period.timeValue,
          startDate: parseUTCDate(period.startDate),
          endDate: parseUTCDate(period.endDate),
          isDefaultSelected: period.isDefaultSelected,
        };

        if (period.isDefaultSelected) {
          defaultForecastPeriodId = period.forecastPeriodId;
        }
      });
    }

    // Parse the list of model names and associated color into a mapping
    const modelColorMap: Record<string, string> = {};
    if (Array.isArray(metadata.models?.list)) {
      metadata.models.list.forEach((m: { modelName: string; color: string }) => {
        modelColorMap[m.modelName] = m.color;
      });
    }
    
    // Parse model availability per period
    const modelAvailabilityPerPeriod = metadata.models?.availabilityPerPeriod || {};

    // Parse targets from new nested structure (includes per-target date ranges)
    const targets =
      metadata.targets?.list?.map((t: any) => ({
        targetId: t.targetId,
        targetKeyInData: t.targetKeyInData,
        displayString: t.displayString,
        dataValueProcessing: t.dataValueProcessing,
        // Include target-specific date ranges
        earliestDate: t.earliestDate,
        latestDate: t.latestDate,
      })) || [];

    // Parse prediction intervals from new nested structure
    const predictionIntervals =
      metadata.predictionIntervals?.available?.map((pi: any) => ({
        level: String(pi.level), // Ensure level is a string for consistency
        quantiles: pi.quantiles,
      })) || [];

    // Parse default selections from metadata
    const defaults = metadata.defaults || {};

    // Extract default location (handle dict or string format for safety)
    let defaultLocation = defaults.location;
    if (typeof defaultLocation === 'object' && defaultLocation !== null) {
      // If it's a dict like {"US": "US"}, extract the key
      defaultLocation = Object.keys(defaultLocation)[0];
    }
    // Fallback to single location code or "US"
    if (!defaultLocation) {
      defaultLocation = metadata.spatial?.isSingleLocation
        ? metadata.spatial?.singleLocationCode
        : 'US';
    }

    const defaultHorizon =
      defaults.horizon !== undefined
        ? defaults.horizon
        : metadata.temporal?.horizons?.[metadata.temporal.horizons.length - 1] || 1;
        
    // Ensure all prediction interval values are strings for consistency
    const defaultPredictionIntervals = defaults.predictionIntervals
      ? defaults.predictionIntervals.map((pi: any) => String(pi))
      : predictionIntervals.map((pi: any) => String(pi.level));

    // Parse UI customization from metadata
    const uiCustomization = metadata.uiCustomization || {
      header: {
        titleName: 'FluForecast',
        navButtons: [],
      },
      forecastPage: {
        chartHeaderName: 'Weekly Hospital Admissions Forecast',
        histTdToggleText: 'Show Admissions at Time of Forecast',
        disableLocationInfo: false,
        infoButtons: {
          headerInfo: undefined,
          horizonInfo: undefined,
        },
      },
      evaluationsPage: {
        tabNames: {
          overviewTab: 'Season Overview',
          singleModelTab: 'Single Model',
        },
        chartLogModeIndicatorText: 'Use Log Scale',
        overviewLocationMapTitle: 'Location-Specific',
        infoButtons: {
          overviewInfo: undefined,
          singleModelInfo: undefined,
          overviewHorizonInfo: undefined,
          singleModelHorizonInfo: undefined,
        },
        locationMapColorScale: {
          colorTop: '#00495F',
          colorBase: '#E9E9E9',
          colorBottom: '#6A9629',
          colorNull: '#363b43',
        },
      },
    };

    return {
      // Feature flags from metadata.features
      evaluationsEnabled: metadata.features?.evaluationsEnabled ?? false,
      historicalTargetDataEnabled: metadata.features?.historicalTargetDataEnabled ?? false,
      nowcastEnabled: false, // Explicitly disabled for generalized version

      // Spatial configuration from metadata.spatial
      isSingleLocation: metadata.spatial?.isSingleLocation ?? false,
      singleLocationCode: metadata.spatial?.singleLocationCode,
      disableMapInDashboard: metadata.spatial?.disableMapInDashboard ?? false,

      // Temporal configuration from metadata.temporal
      
      timeUnit: metadata.temporal?.timeUnit || 7,
      horizons: metadata.temporal?.horizons || [],
      defaultSelectedDate: metadata.temporal?.defaultSelectedDate,
      earliestDateAcrossTargets: metadata.temporal?.earliestDateAcrossTargets,
      latestDateAcrossTargets: metadata.temporal?.latestDateAcrossTargets,

      // Forecast periods
      forecastPeriodOptions,
      defaultForecastPeriodId,

      // Location mapping - will be loaded separately
      locationMapping: {},

      // Models from metadata.models
      modelColorMap,
      modelAvailabilityPerPeriod,

      // Targets from metadata.targets
      targets,
      defaultTargetId: metadata.targets?.defaultTargetId || targets[0]?.targetId || '',

      // Prediction intervals from metadata.predictionIntervals
      predictionIntervals,
      defaultPredictionIntervals,

      // Evaluation Configuration
      evaluationCoverageLevels: metadata.evaluations?.coverageLevels || [],
      evaluationAvailablePeriodIds: metadata.evaluations?.availablePeriodIds || [],

      // Default selections
      defaultLocation,
      defaultHorizon,

      // UI Customization
      uiCustomization,
    };
  };

  /**
   * Main initialization function
   */
  const initializeData = useCallback(async () => {
    if (initStartedRef.current) return;
    initStartedRef.current = true;

    try {
      logger.info('Starting data initialization...');

      // Step 1: Load metadata
      logger.log('Loading metadata...');
      const metadata = await loadMetadata();

      // Step 1.5: Initialize development mode from metadata
      initializeDevMode(metadata);
      logger.info('Development mode initialized:', metadata.features?.developmentMode ?? false);
      logger.info('Using data path:', getDataPath());

      // Step 2: Build config from metadata
      logger.log('Building configuration...');
      const config = buildConfigFromMetadata(metadata);

      // Step 3: Load location mapping
      logger.log('Loading locations...');
      // Convert locationMappingList array to LocationMappingData object
      const locationMappingList = metadata.spatial?.locationMappingList || [];
      const locationMapping: any = {};
      locationMappingList.forEach((loc: any) => {
        locationMapping[loc.location] = {
          locationName: loc.location_name,
          locationNameAlt: loc.location_name_alt,
        };
      });
      config.locationMapping = locationMapping;

      // Step 4: Dispatch config to Redux
      dispatch(setDashboardConfig(config));
      
      // Set current season ID (default forecast period)
      setCurrentSeasonId(config.defaultForecastPeriodId || '');

      updateLoadingState('forecastPeriodOptions', false);
      updateLoadingState('locations', false);

      // Step 5: Load map data (if not disabled)
      if (!config.disableMapInDashboard) {
        logger.log('Loading map data...');
        const mapData = await loadMapShapeData(metadata.spatial?.customShapeFileName);
        if (mapData) {
          dispatch(setMapData(mapData));
        }
      }
      updateLoadingState('locationShapeData', false);

      // Step 6: Load all target-data and model-output
      logger.log('Loading all core data...');
      const [targetData, modelOutput] = await Promise.all([loadTargetData(), loadModelOutput()]);

      // Dispatch to Redux
      dispatch(
        setAllCoreData({
          targetData,
          modelOutput,
        })
      );

      updateLoadingState('targetData', false);
      updateLoadingState('modelOutput', false);

      // Step 7: Initialize forecast settings with config defaults
      // Use the default period from config to set initial time filters
      const defaultPeriod = config.forecastPeriodOptions[config.defaultForecastPeriodId];

      if (defaultPeriod) {
        logger.log('Initializing forecast settings...');
        dispatch(
          initializeForecastSettings({
            locationCode:
              config.defaultLocation ||
              (config.isSingleLocation ? config.singleLocationCode : '25'),
            models: Object.keys(config.modelColorMap),
            target: config.defaultTargetId,
            horizon:
              config.defaultHorizon !== undefined
                ? config.defaultHorizon
                : config.horizons[config.horizons.length - 1],
            forecastPeriod: defaultPeriod,
            predictionIntervals: config.defaultPredictionIntervals,
            selectedDate: config.defaultSelectedDate
              ? new Date(config.defaultSelectedDate)
              : new Date(),
          })
        );
      }

      // Step 8: Initialize evaluation settings if evaluations are enabled
      if (config.evaluationsEnabled) {
        logger.log('Initializing evaluation settings...');
        
        const models = Object.keys(config.modelColorMap);
        const defaultLocation = config.defaultLocation || 
          (config.isSingleLocation ? config.singleLocationCode : 'US') || 'US';
        const locationName = config.locationMapping[defaultLocation]?.locationName || defaultLocation;
        
        // Build target options for evaluation target selector
        const targetOptions = config.targets.map(t => ({
          targetId: t.targetId,
          displayString: t.displayString,
        }));
        
        // Build time range options from forecast periods
        const timeRangeOptions: any[] = Object.values(config.forecastPeriodOptions).map(period => ({
          name: period.forecastPeriodId,
          displayString: period.displayString,
          isDynamic: false,
          startDate: period.startDate,
          endDate: period.endDate,
        }));
        
        // Initialize Season Overview settings
        dispatch(
          initializeEvaluationSeasonOverviewSettings({
            models,
            timeRangeOptions,
            defaultModel: models[0],
            targets: targetOptions,
            defaultTargetId: config.defaultTargetId,
            defaultPeriodId: config.defaultForecastPeriodId,
            horizons: config.horizons,
          })
        );
        
        // Build season options for Single Model view
        const seasonOptions = Object.values(config.forecastPeriodOptions).map((period, index) => ({
          forecastPeriodID: period.forecastPeriodId,
          displayString: period.displayString,
          timeValue: period.timeValue,
          startDate: period.startDate,
          endDate: period.endDate,
          index,
        }));
        
        // Initialize Single Model settings
        dispatch(
          initializeEvaluationSingleModelSettings({
            locationCode: defaultLocation,
            locationName,
            defaultModel: models[0],
            seasonOptions,
            defaultSeasonId: config.defaultForecastPeriodId || seasonOptions[0]?.forecastPeriodID || '',
            targets: targetOptions,
            defaultTargetId: config.defaultTargetId,
            defaultHorizon: config.defaultHorizon,
          })
        );
      }

      logger.info('Initialization complete!');
    } catch (error) {
      logger.error('Failed to initialize data:', error);
      setInitializationError(error instanceof Error ? error.message : 'Unknown error');

      // Reset loading states on error
      Object.keys(loadingStates).forEach((key) => {
        updateLoadingState(key as keyof LoadingStates, false);
      });
    }
  }, [dispatch, updateLoadingState]);

  useEffect(() => {
    initializeData();
  }, [initializeData]);

  const isFullyLoaded = Object.values(loadingStates).every((state) => !state);

  return (
    <DataContext.Provider
      value={{
        loadingStates,
        isFullyLoaded,
        updateLoadingState,
        initializationError,
        loadHistoricalDataIfNeeded,
        currentSeasonId,
      }}
    >
      {/* Show error message about initialization process and offer for user to reload */}
      {initializationError ? (
        <div className="flex items-center justify-center h-screen text-white">
          <div className="text-center">
            <h2 className="text-xl mb-2">Failed to load application data</h2>
            <p className="text-sm text-gray-400">{initializationError}</p>
            <button
              onClick={() => window.location.reload()}
              className="mt-4 px-4 py-2 bg-blue-500 rounded hover:bg-blue-600"
            >
              Reload Page
            </button>
          </div>
        </div>
      ) : (
        children
      )}
    </DataContext.Provider>
  );
};

export const useDataContext = () => {
  const context = useContext(DataContext);
  if (context === undefined) {
    throw new Error('useDataContext must be used within a DataProvider');
  }
  return context;
};
