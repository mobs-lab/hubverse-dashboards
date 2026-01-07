// File Path: /src/app/evaluations/page.tsx
/* Page Component for displaying tab layout for:
 *   - Season Overview
 *   - Single Model
 * */

'use client';

import { useEvaluationsData } from '@/evaluations/hooks/useEvaluationsData';
import { useDataContext } from '@/providers/DataProvider';
import InfoButton from '@/shared-components/InfoButton';
import {
  setMapeChartScaleType,
  setWisChartScaleType,
} from '@/store/data-slices/settings/SettingsSliceEvaluationSeasonOverview';
import { useAppDispatch, useAppSelector } from '@/store/hooks';
import { Card } from '@/styles/material-tailwind-wrapper';
import React, { useEffect, useRef, useState } from 'react';
import { seasonOverviewInfo, singleModelInfo } from 'types/infobutton-content';
import SeasonOverviewAllLocationBoxPlot from './evaluations-components/SeasonOverview/SeasonOverviewAllLocationBoxPlot';
import SeasonOverviewLocationHotMap from './evaluations-components/SeasonOverview/SeasonOverviewLocationHotMap';
import SeasonOverviewPIChart from './evaluations-components/SeasonOverview/SeasonOverviewPIChart';
import { SeasonOverviewSettings } from './evaluations-components/SeasonOverview/SeasonOverviewSettingsPanel';
import SingleModelHorizonPlot from './evaluations-components/SingleModel/SingleModelHorizonPlot';
import SingleModelScoreLineChart from './evaluations-components/SingleModel/SingleModelScoreLineChart';
import SingleModelSettingsPanel from './evaluations-components/SingleModel/SingleModelSettingsPanel';

const SeasonOverviewContent: React.FC = () => {
  const dispatch = useAppDispatch();
  const { loadingStates } = useDataContext();
  const { wisChartScaleType, mapeChartScaleType } = useAppSelector(
    (state) => state.evaluationsSeasonOverviewSettings
  );

  // Get UI customization from config
  const uiConfig = useAppSelector((state) => state.configStore.config?.uiCustomization);
  const overviewInfoConfig = uiConfig?.evaluationsPage?.infoButtons?.overviewInfo;
  const logModeText = uiConfig?.evaluationsPage?.chartLogModeIndicatorText || 'Use Log Scale';

  return (
    <div className="flex flex-col h-full gap-4 overflow-y-auto overflow-x-hidden util-no-sb-length">
      <div className="items-center self-end">
        <InfoButton
          content={overviewInfoConfig?.content || seasonOverviewInfo}
          title={overviewInfoConfig?.title || "Season Overview"}
          displayStyle="icon"
          size="md"
          dialogSize="lg"
        ></InfoButton>
      </div>
      {/* Top charts section - 3 charts in a row */}
      <div className="grid grid-cols-3 gap-4 min-h-[480px]">
        <Card className="bg-mobs-lab-color text-white overflow-hidden" placeholder="">
          <div className="p-1 border-b border-gray-700 flex justify-between items-center">
            <h3 className="text-lg font-medium"> Weighted Interval Score / Baseline </h3>
            <button
              onClick={() =>
                dispatch(setWisChartScaleType(wisChartScaleType === 'log' ? 'linear' : 'log'))
              }
              className="bg-blue-500 hover:bg-blue-600 text-white text-xs py-1 px-2 rounded"
            >
              {wisChartScaleType === 'log' ? `Use Linear Scale` : logModeText}
            </button>
          </div>
          <div className="w-full h-[92%]">
            <SeasonOverviewAllLocationBoxPlot type="wis" />
          </div>
        </Card>

        <Card className="bg-mobs-lab-color text-white overflow-hidden" placeholder="">
          <div className="p-1 border-b border-gray-700 flex justify-between items-center">
            <h3 className="text-lg font-medium">Mean Absolute Percentage Error</h3>
            <button
              onClick={() =>
                dispatch(setMapeChartScaleType(mapeChartScaleType === 'log' ? 'linear' : 'log'))
              }
              className="bg-blue-500 hover:bg-blue-600 text-white text-xs py-1 px-2 rounded"
            >
              {mapeChartScaleType === 'log' ? `Use Linear Scale` : logModeText}
            </button>
          </div>
          <div className="w-full h-[92%]">
            <SeasonOverviewAllLocationBoxPlot type="mape" />
          </div>
        </Card>

        <Card className="bg-mobs-lab-color text-white overflow-hidden" placeholder="">
          <div className="p-1 border-b border-gray-700 flex-row flex-nowrap align-end justify-center items-center">
            <h3 className="text-lg font-medium flex-shrink">Coverage</h3>
          </div>
          <div className="w-full h-[92%]">
            <SeasonOverviewPIChart />
          </div>
        </Card>
      </div>

      {/* US Map section - full width */}
      <Card className="bg-mobs-lab-color text-white mt-4" placeholder="">
        <div className="w-full aspect-[16/9] min-h-[360px] max-h-[480px]">
          <SeasonOverviewLocationHotMap />
        </div>
      </Card>
    </div>
  );
};

const SingleModelContent = () => {
  const { loadingStates } = useDataContext();
  // Granular loading for single model
  const { loadRawScores, areRawScoresLoaded, isLoading } = useEvaluationsData();

  const { evaluationsSingleModelViewSelectedLocationName: evaluationsSingleModelViewSelectedStateName, evaluationSingleModelViewScoresOption } =
    useAppSelector((state) => state.evaluationsSingleModelSettings);

  // Get UI customization from config
  const uiConfig = useAppSelector((state) => state.configStore.config?.uiCustomization);
  const singleModelInfoConfig = uiConfig?.evaluationsPage?.infoButtons?.singleModelInfo;

  // Trigger load when component mounts
  useEffect(() => {
    if (!areRawScoresLoaded && !isLoading) {
      loadRawScores();
    }
  }, [loadRawScores, areRawScoresLoaded, isLoading]);

  if (isLoading && !areRawScoresLoaded) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-white">Loading raw scores data...</p>
      </div>
    );
  }

  return (
    <div className="flex-1 grid grid-rows-[auto,1fr,1fr] gap-4 min-h-0 overflow-hidden">
      <div className="flex flex-row flex-nowrap align-middle justify-between">
        {/* Dynamic Title that shows the name of the state selected */}
        <h1 className="sm:text-sm md:text-base lg:text-2xl xl:text-3xl 2xl:text-4xl font-light util-text-limit max-h-8">
          {evaluationsSingleModelViewSelectedStateName}
        </h1>
        <div className="items-center">
          <InfoButton
            content={singleModelInfoConfig?.content || singleModelInfo}
            title={singleModelInfoConfig?.title || "Single Model Evaluations"}
            displayStyle="icon"
          ></InfoButton>
        </div>
      </div>
      <div className="min-h-0 w-full h-full">
        <div className="p-[0.05rem] border-b border-gray-700 flex justify-between items-center">
          Hospitalization Forecasts by Horizon
        </div>
        <SingleModelHorizonPlot />
      </div>
      <div className="min-h-0 w-full h-full">
        <div className="p-[0.05rem] border-b border-gray-700 flex justify-between items-center">
          {evaluationSingleModelViewScoresOption}
        </div>
        <SingleModelScoreLineChart />
      </div>
    </div>
  );
};

const EvaluationsPage = () => {
  const evaluationsEnabled = useAppSelector(
    (state) => state.configStore.config?.evaluationsEnabled ?? false
  );
  const configLoaded = useAppSelector((state) => state.configStore.isLoaded);

  // Get UI customization from config
  const uiConfig = useAppSelector((state) => state.configStore.config?.uiCustomization);
  const overviewTabName = uiConfig?.evaluationsPage?.tabNames?.overviewTab || 'Season Overview';
  const singleModelTabName = uiConfig?.evaluationsPage?.tabNames?.singleModelTab || 'Single Model';

  const defaultTab = 'season-overview';
  const [activeTab, setActiveTab] = useState(defaultTab);

  // useDataContext and useEvaluationsData must always be called
  const { loadingStates, isFullyLoaded } = useDataContext();

  const {
    isLoading: isEvaluationsLoading,
    areAggregatesLoaded,
    error: evaluationsError,
    loadAggregates,
  } = useEvaluationsData();

  // Load aggregates data when the page mounts (if enabled and config loaded)
  useEffect(() => {
    if (configLoaded && evaluationsEnabled && !areAggregatesLoaded && !isEvaluationsLoading) {
      loadAggregates();
    }
  }, [configLoaded, evaluationsEnabled, areAggregatesLoaded, isEvaluationsLoading, loadAggregates]);

  // Show loading while config is being loaded
  if (!configLoaded) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center p-8">
          <p className="text-white text-lg">Loading configuration...</p>
        </div>
      </div>
    );
  }

  // If evaluations are not enabled, show a message
  if (!evaluationsEnabled) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center p-8 max-w-2xl">
          <h2 className="text-3xl text-white font-semibold mb-4">Evaluations Not Available</h2>
          <p className="text-gray-300 mb-4 text-lg">
            This dashboard has been configured without evaluation features.
          </p>
          <p className="text-gray-400 text-sm mb-6">
            To enable evaluations, update your configuration YAML file with{' '}
            <code className="bg-gray-700 px-2 py-1 rounded">skip_evaluations: false</code> and
            ensure evaluation data is being processed.
          </p>
        </div>
      </div>
    );
  }

  const renderContent = () => {
    // Show error if evaluations data failed to load
    if (evaluationsError) {
      return (
        <div className="text-white p-4">Error loading evaluations data: {evaluationsError}</div>
      );
    }

    if (activeTab === 'season-overview') {
      if (isEvaluationsLoading && !areAggregatesLoaded) {
        return <div className="text-white p-4">Loading season overview data...</div>;
      }
      // If we finished loading but aggregates are not there (error case or empty), we might want to handle it.
      // But assuming loadAggregates handles error state, we can render content which might be empty or show placeholder.
      return <SeasonOverviewContent />;
    } else {
      // Single Model content handles its own data loading
      return <SingleModelContent />;
    }
  };

  return (
    <div className="evaluations-page">
      <div className="evaluations-settings">
        {!loadingStates.locations &&
          (activeTab === 'season-overview' ? (
            <SeasonOverviewSettings />
          ) : (
            <SingleModelSettingsPanel />
          ))}
      </div>

      <div className="evaluations-content">
        <div>
          <div className="flex bg-gray-800 border-b border-gray-700">
            <button
              onClick={() => setActiveTab('season-overview')}
              className={`px-6 py-2 text-sm relative ${
                activeTab === 'season-overview'
                  ? 'text-white hover:text-white bg-mobs-lab-color border-t border-l border-r border-gray-700'
                  : 'text-gray-300 hover:text-white'
              }`}
              style={{
                marginBottom: activeTab === 'season-overview' ? '-1px' : '0',
                zIndex: activeTab === 'season-overview' ? 1 : 0,
              }}
            >
              {overviewTabName}
            </button>

            <button
              onClick={() => setActiveTab('single-model')}
              className={`px-6 py-2 text-sm relative ${
                activeTab === 'single-model'
                  ? 'text-white hover:text-white bg-mobs-lab-color border-t border-l border-r border-gray-700'
                  : 'text-gray-300 hover:text-white border-r border-gray-700'
              }`}
              style={{
                marginBottom: activeTab === 'single-model' ? '-1px' : '0',
                zIndex: activeTab === 'single-model' ? 1 : 0,
              }}
            >
              {singleModelTabName}
            </button>
          </div>
        </div>

        <div className="tab-container">
          <Card className="p-4 flex-1 bg-mobs-lab-color text-white min-h-0" placeholder="">
            {renderContent()}
          </Card>
        </div>
      </div>

      {!isFullyLoaded && isEvaluationsLoading && (
        <div className="fixed bottom-4 right-4 bg-gray-800 text-white px-4 py-2 rounded-md">
          Loading additional data...
        </div>
      )}
    </div>
  );
};

export default EvaluationsPage;
