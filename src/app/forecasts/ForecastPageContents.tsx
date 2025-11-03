// File Path: src/app/forecasts/page.tsx

'use client';

import React from 'react';
import { useDataContext } from '@/providers/DataProvider';
import ForecastChart from './forecasts-components/ForecastChart';
import SettingsPanel from './forecasts-components/SettingsPanel';
import ForecastChartHeader from './forecasts-components/ForecastChartHeader';

import '../css/component_styles/forecast-page.css';

const ForecastPage: React.FC = () => {
  const { loadingStates, isFullyLoaded } = useDataContext();

  return (
    <div className="layout-grid-forecasts-page w-full h-full pl-4">
      {!loadingStates.locations && (
        <div className="settings-panel">
          <SettingsPanel />
        </div>
      )}
      {!loadingStates.targetData && !loadingStates.modelOutput && (
        <>
          <div className="chart-header">
            <ForecastChartHeader />
          </div>
          <div className="forecast-graph overflow-scroll util-no-sb-length">
            <ForecastChart />
          </div>
        </>
      )}
      {!isFullyLoaded && (
        <div className="fixed bottom-4 right-4 bg-gray-800 text-white px-4 py-2 rounded-md">
          Loading additional data...
        </div>
      )}
    </div>
  );
};

export default ForecastPage;
