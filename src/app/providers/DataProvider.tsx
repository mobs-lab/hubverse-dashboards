'use client';

import { getDataPath, initializeDevMode } from '@/config/devMode';
import { setMapData } from '@/store/data-slices/domains/auxiliaryDataSlice';
import { DashboardConfig, setDashboardConfig } from '@/store/data-slices/domains/configSlice';
import { setAllCoreData } from '@/store/data-slices/domains/coreDataSlice';
import { setHistoricalTargetData } from '@/store/data-slices/domains/historicalTargetDataSlice';
import { initializeForecastSettings } from '@/store/data-slices/settings/SettingsSliceForecastPage';
import { useAppDispatch } from '@/store/hooks';
import { LoadingStates } from '@/types/app';
import { ForecastPeriodOptions } from '@/types/domains/forecasting';
import { logger } from '@/utils/logger';
import React, { createContext, useCallback, useContext, useEffect, useRef, useState } from 'react';

interface DataContextType {
  loadingStates: LoadingStates;
  isFullyLoaded: boolean;
  updateLoadingState: (key: keyof LoadingStates, value: boolean) => void;
  initializationError: string | null;
  loadHistoricalDataIfNeeded: () => Promise<void>;
}

const DataContext = createContext<DataContextType | undefined>(undefined);

export const DataProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const dispatch = useAppDispatch();
  const [initializationError, setInitializationError] = useState<string | null>(null);
  const initStartedRef = useRef(false);

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
   */
  const loadHistoricalDataIfNeeded = useCallback(async () => {
    // Check if already loaded
    if (loadingStates.historicalTargetData) {
      logger.info('Historical data is already loading...');
      return;
    }

    try {
      updateLoadingState('historicalTargetData', true);
      logger.info('Loading historical target data...');
      
      const historicalData = await loadHistoricalTargetData();
      dispatch(setHistoricalTargetData(historicalData));
      
      logger.info('Historical target data loaded successfully');
      updateLoadingState('historicalTargetData', false);
    } catch (error) {
      logger.error('Failed to load historical target data:', error);
      updateLoadingState('historicalTargetData', false);
      // Don't throw error - historical data is optional
    }
  }, [dispatch, loadingStates.historicalTargetData, updateLoadingState]);

  /**
   * Load and parse dashboard metadata from Python processor
   */
  const loadMetadata = async (): Promise<any> => {
    const dataPath = getDataPath();
    const response = await fetch(`${dataPath}/auxiliary/metadata.json`);
    if (!response.ok) {
      throw new Error(`Failed to load metadata: ${response.statusText}`);
    }
    return response.json();
  };

  /**
   * Load map shape data (TopoJSON/GeoJSON)
   */
  const loadMapShapeData = async (shapeFileName?: string): Promise<any> => {
    const fileName = shapeFileName || 'states-10m.json';
    const response = await fetch(`/${fileName}`);
    if (!response.ok) {
      logger.warn(`Failed to load map data: ${response.statusText}`);
      return null;
    }
    return response.json();
  };

  /**
   * Load target data for a forecast period
   */
  const loadTargetData = async (forecastPeriodId: string): Promise<any> => {
    const dataPath = getDataPath();
    const response = await fetch(`${dataPath}/${forecastPeriodId}/targetData.json`);
    if (!response.ok) {
      throw new Error(`Failed to load target data for ${forecastPeriodId}`);
    }
    return response.json();
  };

  /**
   * Load model output for a forecast period
   */
  const loadModelOutput = async (forecastPeriodId: string): Promise<any> => {
    const dataPath = getDataPath();
    const response = await fetch(`${dataPath}/${forecastPeriodId}/modelOutputData.json`);
    if (!response.ok) {
      throw new Error(`Failed to load model output for ${forecastPeriodId}`);
    }
    return response.json();
  };

  /**
   * Load historical target data (lazy loaded when toggle is enabled)
   */
  const loadHistoricalTargetData = async (): Promise<any> => {
    const dataPath = getDataPath();
    const response = await fetch(`${dataPath}/historical-target-data/historical-target-data.json`);
    if (!response.ok) {
      throw new Error(`Failed to load historical target data: ${response.statusText}`);
    }
    return response.json();
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
          startDate: new Date(period.startDate),
          endDate: new Date(period.endDate),
          isDefaultSelected: period.isDefaultSelected,
        };

        if (period.isDefaultSelected) {
          defaultForecastPeriodId = period.forecastPeriodId;
        }
      });
    }

    // Parse model configurations with colors from new nested structure
    const models = metadata.models?.list || [];
    const modelColorMap: Record<string, string> = metadata.models?.colors || {};

    // Parse targets from new nested structure
    const targets =
      metadata.targets?.list?.map((t: any) => ({
        targetId: t.targetId,
        targetKeyInData: t.targetKeyInData,
        displayString: t.displayString,
      })) || [];

    // Parse prediction intervals from new nested structure
    const predictionIntervals =
      metadata.predictionIntervals?.available?.map((pi: any) => ({
        level: pi.level,
        quantiles: pi.quantiles,
      })) || [];

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
      earliestDate: metadata.temporal?.earliestDate,
      latestDate: metadata.temporal?.latestDate,

      // Forecast periods
      forecastPeriodOptions,
      defaultForecastPeriodId,

      // Location mapping - will be loaded separately
      locationMapping: {},

      // Models from metadata.models
      models,
      modelColorMap,

      // Targets from metadata.targets
      targets,
      defaultTargetId: metadata.targets?.defaultTargetId || targets[0]?.targetId || '',

      // Prediction intervals from metadata.predictionIntervals
      predictionIntervals,
      defaultPredictionIntervals: metadata.predictionIntervals?.defaults || ['90'],
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
      logger.debug('Config loaded:', config);
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

      // Step 6: Load data for default forecast period
      const defaultPeriod = config.forecastPeriodOptions[config.defaultForecastPeriodId];
      if (defaultPeriod) {
        logger.log(`Loading data for period: ${config.defaultForecastPeriodId}`);

        const [targetData, modelOutput] = await Promise.all([
          loadTargetData(config.defaultForecastPeriodId),
          loadModelOutput(config.defaultForecastPeriodId),
        ]);

        // Dispatch to Redux
        dispatch(
          setAllCoreData({
            targetData: {
              [config.defaultForecastPeriodId]: targetData,
            },
            modelOutput: {
              [config.defaultForecastPeriodId]: modelOutput,
            },
          })
        );

        updateLoadingState('targetData', false);
        updateLoadingState('modelOutput', false);

        // Step 7: Initialize forecast settings with config defaults
        logger.log('Initializing forecast settings...');
        dispatch(
          initializeForecastSettings({
            locationCode: config.isSingleLocation ? config.singleLocationCode : '25',
            models: config.models.map((m) => m.modelName),
            target: config.defaultTargetId,
            horizons: config.horizons,
            forecastPeriod: defaultPeriod,
            predictionIntervals: config.defaultPredictionIntervals,
            selectedDate: config.defaultSelectedDate
              ? new Date(config.defaultSelectedDate)
              : new Date(),
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
