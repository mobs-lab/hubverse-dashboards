'use client';

import { setMapData } from '@/store/data-slices/domains/auxiliaryDataSlice';
import { DashboardConfig, setDashboardConfig } from '@/store/data-slices/domains/configSlice';
import { setAllCoreData } from '@/store/data-slices/domains/coreDataSlice';
import { initializeForecastSettings } from '@/store/data-slices/settings/SettingsSliceForecastPage';
import { useAppDispatch } from '@/store/hooks';
import { LoadingStates } from '@/types/app';
import { ForecastPeriodOptions } from '@/types/domains/forecasting';
import React, { createContext, useCallback, useContext, useEffect, useRef, useState } from 'react';

interface DataContextType {
  loadingStates: LoadingStates;
  isFullyLoaded: boolean;
  updateLoadingState: (key: keyof LoadingStates, value: boolean) => void;
  initializationError: string | null;
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
   * Load and parse dashboard metadata from Python processor
   */
  const loadMetadata = async (): Promise<any> => {
    const response = await fetch('/data/auxiliary/metadata.json');
    if (!response.ok) {
      throw new Error(`Failed to load metadata: ${response.statusText}`);
    }
    return response.json();
  };

  /**
   * Load location mapping data
   */
  const loadLocationMapping = async (): Promise<any> => {
    const response = await fetch('/data/auxiliary/locations.json');
    if (!response.ok) {
      throw new Error(`Failed to load locations: ${response.statusText}`);
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
      console.warn(`Failed to load map data: ${response.statusText}`);
      return null;
    }
    return response.json();
  };

  /**
   * Load target data for a forecast period
   */
  const loadTargetData = async (forecastPeriodId: string): Promise<any> => {
    const response = await fetch(`/data/${forecastPeriodId}/targetData.json`);
    if (!response.ok) {
      throw new Error(`Failed to load target data for ${forecastPeriodId}`);
    }
    return response.json();
  };

  /**
   * Load model output for a forecast period
   */
  const loadModelOutput = async (forecastPeriodId: string): Promise<any> => {
    const response = await fetch(`/data/${forecastPeriodId}/modelOutput.json`);
    if (!response.ok) {
      throw new Error(`Failed to load model output for ${forecastPeriodId}`);
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

    // Parse model configurations with colors
    const models =
      metadata.modelNames?.map((modelName: string) => ({
        modelName,
        color: metadata.modelColors?.[modelName],
      })) || [];

    const modelColorMap: Record<string, string> = {};
    const defaultPalette = [
      '#9ceb94',
      '#3fc49e',
      '#45cded',
      '#0292d1',
      '#7bb1ff',
      '#5f5fd6',
      '#d36f54',
      '#e89c31',
      '#a855f7',
      '#ec4899',
    ];

    models.forEach((model: any, index: number) => {
      modelColorMap[model.modelName] = model.color || defaultPalette[index % defaultPalette.length];
    });

    return {
      evaluationsEnabled: metadata.evaluationsEnabled ?? false,
      nowcastEnabled: false, // Explicitly disabled for generalized version
      isSingleLocation: metadata.isSingleLocation ?? false,
      singleLocationCode: metadata.singleLocationCode,
      disableMapInDashboard: metadata.disableMapInDashboard ?? false,
      timeUnit: metadata.timeUnit || 7,
      horizons: metadata.horizons || [],
      forecastPeriodOptions,
      defaultForecastPeriodId,
      locationMapping: {}, // Will be loaded separately
      models,
      modelColorMap,
      targets:
        metadata.targets?.map((t: any) => ({
          targetId: t.targetId,
          displayString: t.displayString,
        })) || [],
      defaultTargetId: metadata.targets?.[0]?.targetId || '',
      predictionIntervals:
        metadata.predictionIntervals?.map((pi: any) => ({
          level: pi.level,
          quantiles: pi.quantiles,
        })) || [],
      defaultPredictionIntervals: metadata.defaultPredictionIntervals || ['90'],
      defaultSelectedDate: metadata.defaultSelectedDate,
      earliestDate: metadata.earliestDate,
      latestDate: metadata.latestDate,
    };
  };

  /**
   * Main initialization function
   */
  const initializeData = useCallback(async () => {
    if (initStartedRef.current) return;
    initStartedRef.current = true;

    try {
      console.log('Starting data initialization...');

      // Step 1: Load metadata
      console.log('Loading metadata...');
      const metadata = await loadMetadata();

      // Step 2: Build config from metadata
      console.log('Building configuration...');
      const config = buildConfigFromMetadata(metadata);

      // Step 3: Load location mapping
      console.log('Loading locations...');
      const locationMapping = await loadLocationMapping();
      config.locationMapping = locationMapping;

      // Step 4: Dispatch config to Redux
      dispatch(setDashboardConfig(config));
      console.log('Config loaded:', config);
      updateLoadingState('forecastPeriodOptions', false);
      updateLoadingState('locations', false);

      // Step 5: Load map data (if not disabled)
      if (!config.disableMapInDashboard) {
        console.log('Loading map data...');
        const mapData = await loadMapShapeData(metadata.customShapeFileName);
        if (mapData) {
          dispatch(setMapData(mapData));
        }
      }
      updateLoadingState('locationShapeData', false);

      // Step 6: Load data for default forecast period
      const defaultPeriod = config.forecastPeriodOptions[config.defaultForecastPeriodId];
      if (defaultPeriod) {
        console.log(`Loading data for period: ${config.defaultForecastPeriodId}`);

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
        console.log('Initializing forecast settings...');
        dispatch(
          initializeForecastSettings({
            locationCode: config.singleLocationCode || 'US',
            models: config.models.map((m) => m.modelName),
            targets: [config.defaultTargetId],
            horizons: config.horizons,
            forecastPeriod: defaultPeriod,
            predictionIntervals: config.defaultPredictionIntervals,
            selectedDate: config.defaultSelectedDate
              ? new Date(config.defaultSelectedDate)
              : new Date(),
          })
        );
      }

      console.log('Initialization complete!');
    } catch (error) {
      console.error('Failed to initialize data:', error);
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
      }}
    >
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
