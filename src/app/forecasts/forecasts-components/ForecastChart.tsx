// src/app/components/forecasts-components/ForecastChart.tsx
'use client';

import * as d3 from 'd3';
import { Axis, NumberValue } from 'd3';
import React, { useCallback, useEffect, useRef } from 'react';
import { PredictionPointInterval, TargetData } from '@/types/domains/forecasting';
import { useChartMargins } from '@/utils/chart-margin-utils';
import { isUTCDateEqual, generateAlignedDateTicks } from '@/utils/date';
import { useResponsiveSVG } from '@/utils/responsiveSVG';
import { updateUserSelectedDate } from '@/store/data-slices/settings/SettingsSliceForecastPage';
import { useAppDispatch, useAppSelector } from '@/store/hooks';
import {
  selectHistoricalTargetData,
  selectModelColorMap,
  selectModelOutputFiltered,
  selectPredictionIntervalOptions,
  selectTargetDataFiltered,
  selectTimeUnit,
  selectCurrentTargetDataProcessing,
  selectHistoricalTimeSeries,
} from '@/store/selectors';
import { useHistoricalTargetData } from '../hooks/useHistoricalTargetData';

type PredictionDataForRender = {
  [modelName: string]: { [targetDate: string]: PredictionPointInterval };
};

const ForecastChart: React.FC = () => {
  const dispatch = useAppDispatch();
  const svgRef = useRef<SVGSVGElement>(null);
  const { containerRef, dimensions, isResizing } = useResponsiveSVG();
  const margins = useChartMargins(dimensions.width, dimensions.height, 'default');

  // Initialize historical target data hook (handles loading and syncing)
  useHistoricalTargetData();

  // Get config-driven data
  const allPredictionIntervalOptions = useAppSelector(selectPredictionIntervalOptions);
  // Get model color map from config
  const modelColorMap = useAppSelector(selectModelColorMap);
  const timeUnit = useAppSelector(selectTimeUnit);
  const dataProcessingConfig = useAppSelector(selectCurrentTargetDataProcessing);

  // Get all settings variables from Redux
  const {
    userSelectedDate,
    selectedLocationCode,
    selectedModels,
    selectedHorizon,
    timeFilterRangeStart,
    timeFilterRangeEnd,
    yAxisScale,
    selectedPredictionIntervals,
    historicalTargetDataMode,
    selectedTargetId,
  } = useAppSelector((state) => state.forecastSettings);

  // Get data using new selectors
  const groundTruthData = useAppSelector(selectTargetDataFiltered);
  const allModelPredictions = useAppSelector(selectModelOutputFiltered);

  // Get pre-filtered historical series
  const historicalTimeSeries = useAppSelector(selectHistoricalTimeSeries);

  const getHistoricalDataPoint = useCallback(
    (date: Date): number | null => {
      if (!historicalTimeSeries.length) return null;
      const point = historicalTimeSeries.find((d) => isUTCDateEqual(d.date, date));
      return point ? point.observation : null;
    },
    [historicalTimeSeries]
  );

  const createScalesAndAxes = useCallback(
    (
      ground: { date: Date; observation: number | null }[],
      predictions: PredictionDataForRender,
      chartWidth: number,
      chartHeight: number,
      yAxisScale: string
    ) => {
      // Find the maximum date in the ground truth data, but within the `dateStart` and `dateEnd` range, to avoid showing horizon-ahead data
      // const maxGroundTruthDate = d3.max(ground, (d) => d.date) as Date;
      const maxGroundTruthDate = d3.max(ground, (d) => d.date) as Date;

      // Find the maximum date in the prediction data
      let maxPredictionDate = new Date(0);
      if (predictions && Object.keys(predictions).length > 0) {
        Object.values(predictions).forEach((modelPredictions) => {
          Object.keys(modelPredictions).forEach((targetDateISO) => {
            const targetEndDate = new Date(targetDateISO);
            if (targetEndDate > maxPredictionDate) {
              maxPredictionDate = targetEndDate;
            }
          });
        });
      }

      // Use the maximum date from both sources
      const maxDate = d3.max([maxGroundTruthDate, maxPredictionDate]) as Date;

      const xScale = d3.scaleUtc().domain([timeFilterRangeStart, maxDate]).range([0, chartWidth]);

      // Generate tick dates aligned to actual data dates
      // This ensures the tick grid matches the natural data grid regardless of user-selected start date
      const actualDataDates = ground.map(d => d.date);
      const tickDates = generateAlignedDateTicks(
        timeFilterRangeStart,
        maxDate,
        timeUnit,
        actualDataDates
      );

      // Determine ideal tick count based on chart width
      const getIdealTickCount = (width: number, totalTicks: number) => {
        if (width < 500) {
          // Short width mode: 6-12 ticks
          return Math.min(Math.max(6, Math.min(totalTicks, 12)), 12);
        } else {
          // Normal width mode: 8-18 ticks
          return Math.min(Math.max(8, Math.min(totalTicks, 18)), 18);
        }
      };

      const idealTickCount = getIdealTickCount(chartWidth, tickDates.length);

      // Select evenly spaced on-grid dates if we have too many
      let selectedTicks = tickDates;
      if (tickDates.length > idealTickCount) {
        const tickInterval = Math.max(1, Math.floor(tickDates.length / idealTickCount));
        selectedTicks = tickDates.filter((_, i) => i % tickInterval === 0);

        // Always ensure the first and last ticks are included
        if (!isUTCDateEqual(selectedTicks[0], tickDates[0])) {
          selectedTicks.unshift(tickDates[0]);
        }
      }

      const xAxis = d3
        .axisBottom(xScale)
        .tickValues(selectedTicks)
        .tickFormat((date, i) => {
          const dateObj = date instanceof Date ? date : new Date(date as number);
          const year = d3.timeFormat('%Y')(dateObj);
          const month = d3.timeFormat('%b')(dateObj);
          const day = d3.timeFormat('%d')(dateObj);

          // First tick always displays full info
          if (i === 0) {
            return `${year}\n${month}\n${day}`;
          }

          const prevDate = selectedTicks[i - 1];
          // Ensure both date and prevDate are Date objects (d3 may pass number timestamps)
          const prevDateObj = prevDate instanceof Date ? prevDate : new Date(prevDate as number);
          const isNewYear = dateObj.getUTCFullYear() > prevDateObj.getUTCFullYear();
          const isNewMonth = dateObj.getUTCMonth() !== prevDateObj.getUTCMonth();

          if (chartWidth < 500) {
            // Compact mode for narrow charts
            if (isNewYear) {
              return `${year}\n${month}`;
            } else if (isNewMonth) {
              return month;
            }
            return ''; // Hide other labels to reduce clutter
          } else {
            // Full mode for wider charts
            if (isNewYear) {
              return `${year}\n${month}\n${day}`;
            } else if (isNewMonth) {
              return `${month}\n${day}`;
            }
            return day;
          }
        });

      xAxis.tickSize(14); // Increase tick size to accommodate multi-line labels

      const subTicks = tickDates.filter(
        (date) => !selectedTicks.some((majorTick) => isUTCDateEqual(date, majorTick as Date))
      );

      const xAxisWithSubTicks = d3
        .axisBottom(xScale)
        .tickValues(subTicks)
        .tickFormat(() => '')
        .tickSize(6);

      // Initialize yScale with a default linear scale
      // Update yScale
      let yScale: d3.ScaleSymLog<number, number> | d3.ScaleLinear<number, number>;

      const maxGroundTruthValue = d3.max(
        ground.filter((d) => d.observation !== null && d.observation >= 0),
        (d) => d.observation
      ) as number;

      let maxPredictionValue = 0;
      if (predictions && Object.keys(predictions).length > 0) {
        Object.values(predictions).forEach((modelPredictions) => {
          Object.values(modelPredictions).forEach((prediction) => {
            Object.values(prediction.prediction_intervals).forEach((interval) => {
              if (interval.pi_value_high > maxPredictionValue) {
                maxPredictionValue = interval.pi_value_high;
              }
            });
          });
        });
      }

      let maxValue = Math.max(maxGroundTruthValue || 0, maxPredictionValue);

      let minValue = d3.min(
        ground.filter((d) => d.observation !== null && d.observation >= 0),
        (d) => d.observation
      ) as number;

      // Check if minValue is undefined
      if (minValue === undefined || minValue === null) {
        minValue = 0;
      }

      if (maxValue === minValue) {
        maxValue = maxValue + 1;
        minValue = Math.max(0, minValue - 1);
      }

      const isLogScale = yAxisScale === 'log';

      if (!isLogScale) {
        yScale = d3
          .scaleLinear()
          .domain([0, maxValue * 1.1])
          .range([chartHeight, 0]);
      } else {
        const constant = minValue > 0 ? minValue / 2 : 1;
        yScale = d3
          .scaleSymlog()
          .domain([0, maxValue * 1.2])
          .constant(constant)
          .range([chartHeight, 0]);
      }

      const ticks = generateYAxisTicks(minValue, maxValue, isLogScale);

      const yAxis = d3
        .axisLeft(yScale)
        .tickValues(ticks)
        .tickFormat((d) => {
          const val = d.valueOf();
          if (val === 0) return '0';

          // Use a smart formatter that handles different magnitudes
          if (Math.abs(val) < 1 && val !== 0) return d3.format('.2~f')(val); // e.g., 0.25
          if (Math.abs(val) < 1000) return d3.format(',.2~f')(val); // e.g., 1.2, 12, 123
          return d3.format('.2~s')(val); // e.g., 1.2k, 12k, 1.2M
        });

      yAxis.tickSize(-chartWidth);

      return { xScale, yScale, xAxis, yAxis, xAxisWithSubTicks };
    },
    [timeFilterRangeStart, timeUnit]
  );

  function generateYAxisTicks(minValue: number, maxValue: number, isLogScale: boolean): number[] {
    const desiredTickCount = 8;

    if (isLogScale) {
      // Keep the existing logarithmic scale logic
      const minExp = minValue <= 0 ? 0 : Math.floor(Math.log10(minValue));
      const maxExp = Math.ceil(Math.log10(maxValue));
      let ticks: number[] = [];

      for (let exp = minExp; exp <= maxExp; exp++) {
        const base = Math.pow(10, exp);
        ticks.push(base);
        if (exp < maxExp - 1 || (exp === maxExp - 1 && maxValue / base > 5)) {
          ticks.push(2.5 * base);
          ticks.push(5 * base);
        }
      }
      ticks = ticks.filter((tick) => tick >= minValue && tick <= maxValue);
      return ticks;
    } else {
      // Improved linear scale logic
      const range = maxValue - minValue;
      const roughStep = range / (desiredTickCount - 1);

      // Find the nearest nice step value
      const magnitude = Math.pow(10, Math.floor(Math.log10(roughStep)));
      let step = magnitude;
      if (roughStep / magnitude >= 5) step *= 5;
      else if (roughStep / magnitude >= 2) step *= 2;

      // Calculate the start and end values
      const start = Math.floor(minValue / step) * step;
      const end = Math.ceil(maxValue / step) * step;

      let ticks: number[] = [];
      for (let tick = start; tick <= end; tick += step) {
        ticks.push(Number(tick.toFixed(2)));
      }

      // If we have too many ticks, reduce them
      if (ticks.length > desiredTickCount) {
        const stride = Math.ceil(ticks.length / desiredTickCount);
        ticks = ticks.filter((_, index) => index % stride === 0);
      }

      // If we still don't have enough ticks, add intermediate values
      while (ticks.length < desiredTickCount && ticks.length > 0) {
        const newTicks = [];
        for (let i = 0; i < ticks.length - 1; i++) {
          newTicks.push(ticks[i]);
          const middleValue = (ticks[i] + ticks[i + 1]) / 2;
          newTicks.push(Number(middleValue.toFixed(2)));
        }
        newTicks.push(ticks[ticks.length - 1]);
        ticks = newTicks;
      }

      // If we have too many ticks after adding intermediate values, reduce them again
      if (ticks.length > desiredTickCount) {
        const stride = Math.ceil(ticks.length / desiredTickCount);
        ticks = ticks.filter((_, index) => index % stride === 0);
      }

      return ticks;
    }
  }

  function renderGroundTruthData(
    svg: d3.Selection<SVGSVGElement, unknown, null, undefined>,
    surveillanceData: { date: Date; observation: number | null }[],
    xScale: d3.ScaleTime<number, number>,
    yScale: d3.ScaleLogarithmic<number, number> | d3.ScaleLinear<number, number>,
    marginLeft: number,
    marginTop: number
  ) {
    // Remove existing ground truth data-slices paths and circles
    svg.selectAll('.ground-truth-path, .ground-truth-dot').remove();

    const line = d3
      .line<{ date: Date; observation: number | null }>()
      .defined((d) => d.observation !== null && d.observation >= 0) // Include placeholder points
      .x((d) => xScale(d.date))
      .y((d) => (d.observation !== null ? yScale(d.observation) : yScale.range()[0])); // Use bottom of chart for placeholders

    svg
      .append('path')
      .datum(surveillanceData)
      .attr('class', 'ground-truth-path')
      .attr('fill', 'none')
      .attr('stroke', 'white')
      .attr('stroke-width', 1.5)
      .attr('d', line)
      .attr('transform', `translate(${marginLeft}, ${marginTop})`);

    // Add circles for ground truth data-slices points (including placeholders)
    svg
      .selectAll('.ground-truth-dot')
      .data(surveillanceData)
      .enter()
      .append('circle')
      .attr('class', 'ground-truth-dot')
      .attr('cx', (d) => xScale(d.date))
      .attr('cy', (d) =>
        d.observation !== null && d.observation >= 0 ? yScale(d.observation) : yScale.range()[0]
      )
      .attr('r', 3)
      .attr('fill', (d) => (d.observation !== null && d.observation >= 0 ? 'white' : 'transparent'))
      .attr('stroke', (d) =>
        d.observation !== null && d.observation >= 0 ? 'white' : 'transparent'
      )
      .attr('transform', `translate(${marginLeft}, ${marginTop})`);
  }

  const renderHistoricalData = useCallback(
    (
      svg: d3.Selection<SVGSVGElement, unknown, null, undefined>,
      historicalData: { date: Date; observation: number | null }[],
      xScale: d3.ScaleTime<number, number>,
      yScale: d3.ScaleLinear<number, number> | d3.ScaleLogarithmic<number, number>,
      marginLeft: number,
      marginTop: number
    ) => {
      if (!historicalData || historicalData.length === 0) {
        return;
      }

      // Filter to show only historical data up to the selected date (as_of date)
      // This shows what was known AS OF the selected reference date
      const historicalDataToDraw = historicalData.filter(
        (d) => d.date < userSelectedDate && d.date >= timeFilterRangeStart
      );

      const historicalLine = d3
        .line<{ date: Date; observation: number | null }>()
        .defined((d) => d.observation !== null && !isNaN(d.observation))
        .x((d) => xScale(d.date))
        .y((d) => (d.observation !== null ? yScale(d.observation) : 0));

      svg
        .append('path')
        .datum(historicalDataToDraw.filter((d) => d.observation !== null && !isNaN(d.observation)))
        .attr('class', 'historical-ground-truth-path')
        .attr('fill', 'none')
        .attr('stroke', '#FFA500') // Orange color for historical data-slices
        .attr('stroke-width', 3)
        .attr('d', historicalLine)
        .attr('transform', `translate(${marginLeft}, ${marginTop})`);

      svg
        .selectAll('.historical-ground-truth-dot')
        .data(historicalDataToDraw.filter((d) => d.observation !== null && !isNaN(d.observation)))
        .enter()
        .append('circle')
        .attr('class', 'historical-ground-truth-dot')
        .attr('cx', (d) => xScale(d.date))
        .attr('cy', (d) => (d.observation !== null ? yScale(d.observation) : 0))
        .attr('r', 6) // Slightly larger than current ground truth dots
        .attr('fill', '#FFA500')
        .attr('transform', `translate(${marginLeft}, ${marginTop})`);
    },
    [timeFilterRangeStart, userSelectedDate]
  );

  function renderPredictionData(
    svg: d3.Selection<SVGSVGElement, unknown, null, undefined>,
    predictionData: PredictionDataForRender,
    xScale: d3.ScaleTime<number, number, never>,
    yScale: d3.ScaleLinear<number, number, never>,
    marginLeft: number,
    marginTop: number,
    confidenceInterval: string[],
    isGroundTruthDataPlaceHolderOnly: boolean
  ) {
    // Remove existing prediction data-slices paths and circles
    svg.selectAll('.prediction-path, .prediction-dot, .confidence-area').remove();

    // Check if predictionData is not empty
    if (Object.keys(predictionData).length > 0) {
      Object.entries(predictionData).forEach(([modelName, modelPredictions], index) => {
        const predictionsArray = Object.entries(modelPredictions).map(([targetDate, pred]) => ({
          targetEndDate: new Date(targetDate),
          ...pred,
        }));

        if (predictionsArray.length > 0) {
          const modelColor = modelColorMap[modelName] || `hsl(${index * 60}, 100%, 50%)`;

          // Render prediction data-slices points
          const line = d3
            .line<any>()
            .x((d) => xScale(new Date(d.targetEndDate)))
            .y((d) => yScale(d.value_median));

          // Separate predictions into past (negative horizon) and future (positive horizon)
          const zeroDate = new Date(userSelectedDate);
          const pastPredictions = predictionsArray
            .filter((d) => d.targetEndDate < zeroDate)
            .sort((a, b) => a.targetEndDate.getTime() - b.targetEndDate.getTime()); // Sort ascending for path
          const futurePredictions = predictionsArray
            .filter((d) => d.targetEndDate >= zeroDate)
            .sort((a, b) => a.targetEndDate.getTime() - b.targetEndDate.getTime()); // Sort ascending for path

          // Find the point closest to the zero date to connect the paths
          const connectionPoint = futurePredictions[0]; // Should be the zeroDate point if it exists

          const pastPathData = connectionPoint
            ? [...pastPredictions, connectionPoint]
            : pastPredictions;
          const futurePathData = futurePredictions;

          if (pastPathData.length > 0) {
            svg
              .append('path')
              .datum(pastPathData)
              .attr('class', 'prediction-path-past')
              .attr('fill', 'none')
              .attr('stroke', modelColor)
              .attr('stroke-width', 1.5)
              .attr('d', line)
              .attr('transform', `translate(${marginLeft}, ${marginTop})`);
          }

          if (futurePathData.length > 0) {
            svg
              .append('path')
              .datum(futurePathData)
              .attr('class', 'prediction-path-future')
              .attr('fill', 'none')
              .attr('stroke', modelColor)
              .attr('stroke-width', 1.5)
              .attr('d', line)
              .attr('transform', `translate(${marginLeft}, ${marginTop})`);
          }

          // Add circles for prediction data-slices points
          svg
            .selectAll(`.prediction-dot-${index}`)
            .data(predictionsArray)
            .enter()
            .append('circle')
            .attr('class', `prediction-dot prediction-dot-${index}`)
            .attr('cx', (d: any) => xScale(new Date(d.targetEndDate)))
            .attr('cy', (d: any) => yScale(d.value_median))
            .attr('r', 3)
            .attr('fill', modelColor)
            .attr('transform', `translate(${marginLeft}, ${marginTop})`);

          // Render confidence intervals separately
          // Create a stable opacity mapping based on all available intervals
          const sortedAvailableLevels = allPredictionIntervalOptions
            .map((opt) => opt.level)
            .sort((a, b) => parseInt(a) - parseInt(b));

          const opacityScale = d3
            .scaleLinear()
            .domain([0, sortedAvailableLevels.length])
            .range([0.5, 0.1]);

          const opacityMap = new Map<string, number>();
          sortedAvailableLevels.forEach((level, i) => {
            opacityMap.set(level, opacityScale(i));
          });

          selectedPredictionIntervals.forEach((level) => {
            const area = d3
              .area<any>()
              .x((d) => xScale(new Date(d.targetEndDate)))
              .y0((d) => {
                const interval = d.prediction_intervals?.[level];
                return interval ? yScale(interval.pi_value_low) : yScale(0);
              })
              .y1((d) => {
                const interval = d.prediction_intervals?.[level];
                return interval ? yScale(interval.pi_value_high) : yScale(0);
              });

            const opacity = opacityMap.get(level) ?? 0.2;

            const color = d3.color(modelColor);
            if (!color) return;
            color.opacity = opacity;

            // Helper: Draw a vertical capsule for isolated points
            // This is critical for the "Negative Horizon" case (e.g., Horizon -1) where
            // the data chain might terminate without connecting to a 'zero' or 'future' point.
            const drawIsolatedInterval = (dataPoint: any, className: string) => {
              const interval = dataPoint.prediction_intervals?.[level];
              if (!interval) return;

              svg
                .append('line')
                .attr('class', className)
                .attr('x1', xScale(new Date(dataPoint.targetEndDate)))
                .attr('x2', xScale(new Date(dataPoint.targetEndDate)))
                .attr('y1', yScale(interval.pi_value_low))
                .attr('y2', yScale(interval.pi_value_high))
                .attr('stroke', color.toString())
                .attr('stroke-width', 6) // Visible width for the interval
                .attr('stroke-linecap', 'round') // Rounded caps for better UI
                .style('opacity', opacity)
                .attr('transform', `translate(${marginLeft}, ${marginTop})`)
                .attr('pointer-events', 'none');
            };

            // Render Past PIs
            if (pastPathData.length > 1) {
              svg
                .append('path')
                .datum(pastPathData)
                .attr('class', 'confidence-area-past')
                .attr('fill', color.toString())
                .attr('d', area)
                .attr('transform', `translate(${marginLeft}, ${marginTop})`)
                .attr('pointer-events', 'none');
            } else if (pastPathData.length === 1) {
              // Ensure when only -1 horizon is available so no possible area, we draw a line indicating where intervals are at
              drawIsolatedInterval(pastPathData[0], 'confidence-area-past-single');
            }

            // Render Future PIs
            // Standard rendering for 0 and positive horizons
            if (futurePathData.length > 1) {
              svg
                .append('path')
                .datum(futurePathData)
                .attr('class', 'confidence-area-future')
                .attr('fill', color.toString())
                .attr('d', area)
                .attr('transform', `translate(${marginLeft}, ${marginTop})`)
                .attr('pointer-events', 'none');
            } else if (futurePathData.length === 1) {
              // Fallback for single-point future data (e.g. only Horizon 0 exists)
              drawIsolatedInterval(futurePathData[0], 'confidence-area-future-single');
            }
          });
        }
      });
    }
  }

  function createVerticalIndicator(
    svg: d3.Selection<SVGSVGElement, unknown, null, undefined>,
    xScale: d3.ScaleTime<number, number>,
    marginLeft: number,
    marginTop: number,
    height: number,
    marginBottom: number
  ) {
    const group = svg.append('g').attr('class', 'vertical-indicator-group');

    const line = group
      .append('line')
      .attr('class', 'vertical-indicator')
      .attr('stroke', 'gray')
      .attr('stroke-width', 0.8)
      .attr('y1', marginTop)
      .attr('y2', height - marginBottom);

    const tooltip = group
      .append('text')
      .attr('class', 'line-tooltip')
      .attr('fill', 'white')
      .attr('font-size', 12)
      .attr('text-anchor', 'end')
      .style('font-family', 'var(--font-dm-sans)')
      .attr('y', marginTop + 5);

    /* Change the accompaning tooltip text to DM Sans*/

    return { group, line, tooltip };
  }

  const updateVerticalIndicator = useCallback(
    (
      date: Date,
      xScale: d3.ScaleTime<number, number>,
      marginLeft: number,
      chartWidth: number,
      group: d3.Selection<SVGGElement, unknown, null, undefined>,
      tooltip: d3.Selection<SVGTextElement, unknown, null, undefined>
    ) => {
      const xPosition = xScale(date);
      const epiweek = getEpiweek(date);
      const isLeftSide = xPosition < chartWidth / 5;

      group.attr('transform', `translate(${xPosition + marginLeft}, 0)`);

      group.select('line').attr('stroke', 'lightgray').attr('stroke-width', 2);

      tooltip
        .attr('x', isLeftSide ? 5 : -5)
        .attr('text-anchor', isLeftSide ? 'start' : 'end')
        // .text(`${date.toLocaleDateString()} (Week ${epiweek})`)
        .text(`${date.toUTCString().slice(0, 16)} (Week ${epiweek})`)
        .attr('fill', 'white')
        .style('font-family', 'var(--font-dm-sans), sans-serif');
    },
    []
  );

  function getEpiweek(date: Date): number {
    const startOfYear = new Date(date.getUTCFullYear(), 0, 1);
    const days = Math.floor((date.getTime() - startOfYear.getTime()) / (24 * 60 * 60 * 1000));
    return Math.ceil((days + startOfYear.getDay() + 1) / 7);
  }

  function createMouseFollowLine(
    svg: d3.Selection<SVGSVGElement, unknown, null, undefined>,
    marginLeft: number,
    marginTop: number,
    height: number,
    marginBottom: number
  ) {
    return svg
      .append('line')
      .attr('class', 'mouse-follow-line')
      .attr('stroke', 'gray')
      .attr('stroke-width', 1)
      .attr('stroke-dasharray', '5,5')
      .attr('y1', marginTop)
      .attr('y2', height - marginBottom)
      .style('opacity', 0);
  }

  function createCornerTooltip(
    svg: d3.Selection<SVGSVGElement, unknown, null, undefined>,
    marginLeft: number,
    marginTop: number
  ) {
    return svg
      .append('g')
      .attr('class', 'corner-tooltip')
      .attr('transform', `translate(${marginLeft + 40}, ${marginTop})`)
      .style('opacity', 0)
      .attr('pointer-events', 'none');
  }

  const updateCornerTooltip = useCallback(
    (
      data: { date: Date; observation: number | null; targetId: string },
      predictionData: PredictionDataForRender,
      historicalDataPoint: number | null,
      xScale: d3.ScaleTime<number, number>,
      chartWidth: number,
      marginLeft: number,
      marginTop: number,
      cornerTooltip: d3.Selection<SVGGElement, unknown, null, undefined>,
      isHistoricalDataMode: boolean
    ) => {
      // Clear existing content
      cornerTooltip.selectAll('*').remove();

      // Layout constants
      const layout = {
        padding: 10,
        lineHeight: 20,
        sectionGap: 10,
        modelColorBoxSize: 12,
        colGap: 30,
        fontFamily: 'var(--font-dm-sans), sans-serif',
        fontSize: '13px',
        fontColor: 'white',
        bgColor: '#333943',
        // Define column widths for the prediction table
        medianColWidth: 60,
        piColWidth: 110,
      };
      // --- 2. PREPARE DATA ---
      const currentPredictions = findPredictionsForDate(predictionData, data.date);

      // Helper to format numbers based on config
      const formatValue = (
        value: number | null | undefined,
        type: 'observation' | 'prediction'
      ) => {
        if (value === null || value === undefined || Number.isNaN(value)) {
          return 'N/A';
        }
        if (type === 'observation' && value === -1) {
          return 'N/A';
        }

        const roundingConfig = dataProcessingConfig?.rounding_decimals;
        const decimals =
          type === 'observation'
            ? (roundingConfig?.target_data ?? 0)
            : (roundingConfig?.model_output ?? 2);

        return value.toFixed(decimals);
      };

      let maxWidth = 0;
      let currentY = layout.padding + 8;

      // --- 3. BUILD TOOLTIP CONTENT (in a container group) ---
      // This container will hold all text and shapes. We'll measure this group.
      const contentGroup = cornerTooltip.append('g');

      // Helper function to add a line of text and update dimensions
      const addTextLine = (label: string, value: string, yPos: number) => {
        const text = contentGroup
          .append('text')
          .attr('x', layout.padding)
          .attr('y', yPos)
          .attr('fill', layout.fontColor)
          .style('font-family', layout.fontFamily)
          .attr('font-size', layout.fontSize);

        text.append('tspan').text(label);
        text.append('tspan').text(value).attr('font-weight', 'bold');

        // Update the maximum width needed for the tooltip
        const node = text.node();
        if (node) {
          maxWidth = Math.max(maxWidth, node.getComputedTextLength());
        }
        return yPos + layout.lineHeight;
      };

      // A. Add Date and Admissions Info
      currentY = addTextLine('Date: ', data.date.toUTCString().slice(5, 16), currentY);
      currentY = addTextLine(
        'Admissions: ',
        formatValue(data.observation, 'observation'),
        currentY
      );

      // B. Add Historical Admissions Info (if toggled)
      if (isHistoricalDataMode && historicalDataPoint !== null) {
        currentY = addTextLine(
          'Historical: ',
          formatValue(historicalDataPoint, 'observation'),
          currentY
        );
      }

      // C. Add Prediction Data (if available)
      if (currentPredictions && Object.keys(currentPredictions).length > 0) {
        currentY += layout.sectionGap; // Add space before the prediction section

        // Calculate the total width of the prediction table
        const tableWidth =
          layout.medianColWidth +
          selectedPredictionIntervals.length * (layout.piColWidth + layout.colGap);
        maxWidth = Math.max(maxWidth, tableWidth);

        Object.entries(currentPredictions).forEach(
          ([modelName, modelPrediction]: [string, PredictionPointInterval]) => {
            // Model Name and Color Box
            const modelGroup = contentGroup
              .append('g')
              .attr('transform', `translate(${layout.padding}, ${currentY})`);

            modelGroup
              .append('rect')
              .attr('width', layout.modelColorBoxSize)
              .attr('height', layout.modelColorBoxSize)
              .attr('y', -layout.modelColorBoxSize / 1.5) // Center align with text
              .attr('fill', modelColorMap[modelName]);

            const modelText = modelGroup
              .append('text')
              .attr('x', layout.modelColorBoxSize + 6)
              .attr('fill', layout.fontColor)
              .attr('font-weight', 'bold')
              .style('font-family', layout.fontFamily)
              .attr('font-size', layout.fontSize)
              .text(modelName);

            if (selectedPredictionIntervals.length === 0) {
              const node = modelText.node();
              if (node) {
                const textWidth = node.getComputedTextLength();
                const estWidth = textWidth + layout.modelColorBoxSize + layout.padding * 2 + 6;
                maxWidth = Math.max(maxWidth, estWidth);
              }
            }

            currentY += layout.lineHeight;

            // Prediction Table (Headers and Values)
            const tableGroup = contentGroup
              .append('g')
              .attr('transform', `translate(${layout.padding}, ${currentY})`);

            // Headers
            tableGroup
              .append('text')
              .text('Median')
              .attr('x', 0)
              .attr('fill', layout.fontColor)
              .style('font-family', layout.fontFamily)
              .attr('font-size', layout.fontSize);

            selectedPredictionIntervals.forEach((level, i) => {
              tableGroup
                .append('text')
                .text(`${level}% PI`)
                .attr(
                  'x',
                  layout.medianColWidth + layout.colGap + i * (layout.piColWidth + layout.colGap)
                )
                .attr('fill', layout.fontColor)
                .style('font-family', layout.fontFamily)
                .attr('font-size', layout.fontSize);
            });

            currentY += layout.lineHeight;

            // Values
            const valueRow = contentGroup
              .append('g')
              .attr('transform', `translate(${layout.padding}, ${currentY})`);

            valueRow
              .append('text')
              .text(formatValue(modelPrediction.value_median, 'prediction'))
              .attr('x', 0)
              .attr('fill', layout.fontColor)
              .style('font-family', layout.fontFamily)
              .attr('font-size', layout.fontSize)
              .attr('font-weight', 'bold');

            selectedPredictionIntervals.forEach((level, i) => {
              const interval = modelPrediction.prediction_intervals[level];
              const ciText = interval
                ? `[${formatValue(interval.pi_value_low, 'prediction')}, ${formatValue(
                    interval.pi_value_high,
                    'prediction'
                  )}]`
                : 'N/A';
              valueRow
                .append('text')
                .text(ciText)
                .attr(
                  'x',
                  layout.medianColWidth + layout.colGap + i * (layout.piColWidth + layout.colGap)
                )
                .attr('fill', layout.fontColor)
                .style('font-family', layout.fontFamily)
                .attr('font-size', layout.fontSize);
            });

            currentY += layout.lineHeight;
          }
        );
      }

      // --- 4. DRAW BACKGROUND AND POSITION THE TOOLTIP ---
      const EXTRAPADDING = 20;
      const finalWidth = maxWidth + layout.padding * 2 + EXTRAPADDING;
      const finalHeight = currentY + layout.padding - layout.lineHeight / 2;

      // Add the background rectangle now that we have the final dimensions
      contentGroup
        .insert('rect', ':first-child') // Insert behind all content
        .attr('width', finalWidth)
        .attr('height', finalHeight)
        .attr('fill', layout.bgColor)
        .attr('rx', 8)
        .attr('ry', 8);

      // Decide whether to show the tooltip on the left or right
      const cursorX = xScale(data.date);
      const showOnLeftSide = cursorX > chartWidth * 0.5;

      // Calculate the final X position for the entire tooltip group.
      // This is the key to correct alignment. The internal layout is always LTR.
      const tooltipX = showOnLeftSide
        ? marginLeft + 20 // Show on the left side
        : chartWidth + marginLeft - finalWidth; // Show on the right side

      // Apply the final position and make it visible
      cornerTooltip.attr('transform', `translate(${tooltipX}, ${marginTop})`).style('opacity', 1);
    },
    [selectedPredictionIntervals, modelColorMap, dataProcessingConfig]
  );

  function findPredictionsForDate(
    predictionData: PredictionDataForRender,
    date: Date
  ): { [modelName: string]: PredictionPointInterval } | null {
    const foundPredictions: { [modelName: string]: PredictionPointInterval } = {};

    Object.entries(predictionData).forEach(([modelName, modelPredictions]) => {
      const targetDateISO = date.toISOString().split('T')[0];
      const predictionForDate = Object.entries(modelPredictions).find(([d]) =>
        d.startsWith(targetDateISO)
      );

      if (predictionForDate) {
        foundPredictions[modelName] = predictionForDate[1];
      }
    });

    return Object.keys(foundPredictions).length > 0 ? foundPredictions : null;
  }

  function createEventOverlay(
    svg: d3.Selection<SVGSVGElement, unknown, null, undefined>,
    marginLeft: number,
    marginTop: number,
    chartWidth: number,
    chartHeight: number
  ) {
    return svg
      .append('rect')
      .attr('class', 'event-overlay')
      .attr('x', marginLeft)
      .attr('y', marginTop)
      .attr('width', chartWidth)
      .attr('height', chartHeight)
      .style('fill', 'none')
      .style('pointer-events', 'all');
  }

  function appendAxes(
    svg: d3.Selection<d3.BaseType, unknown, HTMLElement, any>,
    xAxis: Axis<NumberValue>,
    yAxis: Axis<NumberValue>,
    xAxisMinor: Axis<NumberValue>,
    xScale: d3.ScaleTime<number, number, never>,
    marginLeft: number,
    marginTop: number,
    chartWidth: number,
    chartHeight: number,
    dateStart: Date,
    dateEnd: Date
  ) {
    // Append x-axis
    const xAxisGroup = svg
      .append('g')
      .attr('transform', `translate(${marginLeft}, ${chartHeight + marginTop})`)
      .style('font-family', 'var(--font-dm-sans)')
      .call(xAxis);

    // Append sub x-axis
    svg
      .append('g')
      .attr('class', 'x-axis-with-sub-ticks')
      .attr('transform', `translate(${marginLeft}, ${chartHeight + marginTop})`)
      .call(xAxisMinor)
      .call((g) => g.select('.domain').remove()); // Remove domain line for minor axis

    function wrap(text: d3.Selection<d3.BaseType, unknown, SVGGElement, any>, width: number) {
      text.each(function (this: d3.BaseType) {
        var text = d3.select(this),
          words = text.text().split(/\n+/).reverse(),
          word,
          line: string[] = [],
          lineNumber = 0,
          lineHeight = 1.0, // ems
          y = text.attr('y'),
          dy = parseFloat(text.attr('dy')),
          tspan = text
            .text(null)
            .append('tspan')
            .attr('x', 0)
            .attr('y', y)
            .attr('dy', dy + 'em');
        while ((word = words.pop())) {
          line.push(word);
          tspan.text(line.join(' '));
          const node = tspan.node();
          if (node && node.getComputedTextLength() > width) {
            line.pop();
            tspan.text(line.join(' '));
            line = [word];
            tspan = text
              .append('tspan')
              .attr('x', 0)
              .attr('y', y)
              .attr('dy', ++lineNumber * lineHeight + dy + 'em')
              .text(word);
          }
        }
      });
    }

    // Style x-axis ticks
    xAxisGroup
      .selectAll('.tick text')
      .style('text-anchor', 'middle')
      .attr('dy', '1em')
      .style('font-size', '13px')
      .call(wrap, 32); // 32 is the minimum width to accommodate year number at 1080p 100% zoom view environment

    // Add year labels if the date range is more than a year
    const timeDiff = dateEnd.getTime() - dateStart.getTime();
    const daysDiff = timeDiff / (1000 * 3600 * 24);

    if (daysDiff > 365) {
      const years = d3.timeYear.range(dateStart, dateEnd);
      years.push(dateEnd); // Add the end date to ensure the last year is labeled

      xAxisGroup
        .selectAll('.year-label')
        .data(years)
        .enter()
        .append('text')
        .attr('class', 'year-label')
        .attr('x', (d) => xScale(d))
        .attr('y', 30)
        .attr('text-anchor', 'middle')
        .text((d) => d.getFullYear());
    }

    // Append y-axis
    const yAxisGroup = svg
      .append('g')
      .attr('transform', `translate(${marginLeft}, ${marginTop})`)
      .style('font-family', 'var(--font-dm-sans)')

      .call(yAxis)
      .call((g) => g.select('.domain').remove())
      .call((g) =>
        g.selectAll('.tick line').attr('stroke-opacity', 0.5).attr('stroke-dasharray', '2,2')
      );

    // Style y-axis ticks
    yAxisGroup
      .selectAll('.tick text')
      //Make the font size always as big as possible
      .style('font-size', '18px');
  }

  function findNearestDataPoint(
    data: { date: Date; observation: number | null; targetId: string }[],
    targetDate: Date
  ): { date: Date; observation: number | null; targetId: string } {
    return data.reduce((prev, curr) => {
      const prevDiff = Math.abs(prev.date.getTime() - targetDate.getTime());
      const currDiff = Math.abs(curr.date.getTime() - targetDate.getTime());
      return currDiff < prevDiff ? curr : prev;
    });
  }

  function renderMessage(
    svg: d3.Selection<null, unknown, null, undefined>,
    message: string,
    chartWidth: number,
    chartHeight: number,
    marginLeft: number,
    marginTop: number
  ) {
    svg.selectAll('.message').remove();

    svg
      .append('text')
      .attr('class', 'message')
      .style('font-family', 'var(--font-dm-sans)')
      .attr('x', chartWidth / 2 + marginLeft)
      .attr('y', chartHeight / 2 + marginTop)
      .attr('text-anchor', 'middle')
      .attr('font-size', '22px')
      .attr('font-weight', 'bold')
      .attr('fill', 'white')
      .text(message);
  }

  function createCombinedDataset(
    groundTruthData: { date: Date; observation: number | null; targetId: string }[],
    predictionData: PredictionDataForRender
  ): { date: Date; observation: number | null; targetId: string }[] {
    // First deconstruct the whole of ground truth data-slices into a new array
    let combinedData = [...groundTruthData];

    // Then iterate over each model's predictions
    Object.values(predictionData).forEach((modelPredictions) => {
      // For each prediction, check if a data-slices point already exists for that
      Object.keys(modelPredictions).forEach((targetDateISO) => {
        const predictionDate = new Date(targetDateISO);
        const existingPoint = combinedData.find((d) => isUTCDateEqual(d.date, predictionDate));
        if (!existingPoint) {
          combinedData.push({
            date: predictionDate,
            observation: null,
            targetId: groundTruthData[0]?.targetId || '',
          });
        }
      });
    });

    return combinedData.sort((a, b) => a.date.getTime() - b.date.getTime());
  }

  /* Bubble the `userSelectedWeek` to Redux so sibling Nowcast components can also use this information in filtering*/
  const bubbleUserSelectedWeek = useCallback(
    (date: Date) => {
      const utcDate = new Date(date.toISOString());
      dispatch(updateUserSelectedDate(new Date(date.toISOString()))); // Ensure UTC
    },
    [dispatch]
  );

  const renderChartComponents = useCallback(
    (
      svg: d3.Selection<SVGSVGElement, unknown, null, undefined>,
      filteredGroundTruthData: { date: Date; observation: number | null; targetId: string }[],
      processedPredictionData: PredictionDataForRender,
      xScale: d3.ScaleTime<number, number>,
      marginLeft: number,
      marginTop: number,
      chartWidth: number,
      chartHeight: number,
      height: number,
      marginBottom: number
    ) => {
      const combinedData = createCombinedDataset(filteredGroundTruthData, processedPredictionData);

      const mouseFollowLine = createMouseFollowLine(
        svg,
        marginLeft,
        marginTop,
        height,
        marginBottom
      );
      const {
        group: verticalIndicatorGroup,
        line: verticalIndicator,
        tooltip: lineTooltip,
      } = createVerticalIndicator(svg, xScale, marginLeft, marginTop, height, marginBottom);
      const cornerTooltip = createCornerTooltip(svg, marginLeft, marginTop);
      const eventOverlay = createEventOverlay(svg, marginLeft, marginTop, chartWidth, chartHeight);

      let isDragging = false;

      function updateFollowLine(event: any) {
        const [mouseX] = d3.pointer(event);
        const date = xScale.invert(mouseX - marginLeft);
        const closestData = findNearestDataPoint(combinedData, date);

        const historicalDataPoint = getHistoricalDataPoint(closestData.date);

        const snappedX = xScale(closestData.date);
        mouseFollowLine
          .attr('transform', `translate(${snappedX + marginLeft}, 0)`)
          .style('opacity', 1);

        updateCornerTooltip(
          closestData,
          processedPredictionData,
          historicalDataPoint,
          xScale,
          chartWidth,
          marginLeft,
          marginTop,
          cornerTooltip,
          historicalTargetDataMode
        );
      }

      function updateVerticalIndicatorPosition(event: any) {
        const [mouseX] = d3.pointer(event);
        const date = xScale.invert(mouseX - marginLeft);
        const closestData = findNearestDataPoint(combinedData, date);

        updateVerticalIndicator(
          closestData.date,
          xScale,
          marginLeft,
          chartWidth,
          verticalIndicatorGroup,
          lineTooltip
        );
      }

      function handleMouseMove(event: any) {
        updateFollowLine(event);
        if (isDragging) {
          updateVerticalIndicatorPosition(event);
        }
      }

      function handleClick(event: any) {
        const [mouseX] = d3.pointer(event);
        const date = xScale.invert(mouseX - marginLeft);
        const closestData = findNearestDataPoint(combinedData, date);

        bubbleUserSelectedWeek(closestData.date);
        updateVerticalIndicator(
          closestData.date,
          xScale,
          marginLeft,
          chartWidth,
          verticalIndicatorGroup,
          lineTooltip
        );
        const historicalDataPoint = getHistoricalDataPoint(closestData.date);
        updateCornerTooltip(
          closestData,
          processedPredictionData,
          historicalDataPoint,
          xScale,
          chartWidth,
          marginLeft,
          marginTop,
          cornerTooltip,
          historicalTargetDataMode
        );
      }

      function handleMouseDown(event: any) {
        const [mouseX] = d3.pointer(event);
        const date = xScale.invert(mouseX - marginLeft);
        const closestData = findNearestDataPoint(combinedData, date);

        isDragging = true;
        updateVerticalIndicatorPosition(event);
        const historicalDataPoint = getHistoricalDataPoint(closestData.date);
        updateCornerTooltip(
          closestData,
          processedPredictionData,
          historicalDataPoint,
          xScale,
          chartWidth,
          marginLeft,
          marginTop,
          cornerTooltip,
          historicalTargetDataMode
        );
      }

      function handleMouseUp() {
        isDragging = false;
      }

      function handleMouseOut() {
        mouseFollowLine.style('opacity', 0);

        if (isDragging) {
          isDragging = false;
        }
      }

      eventOverlay
        .on('mousemove', handleMouseMove)
        .on('mouseout', handleMouseOut)
        .on('click', handleClick)
        .on('mousedown', handleMouseDown)
        .on('mouseup', handleMouseUp);

      return {
        mouseFollowLine,
        verticalIndicatorGroup,
        lineTooltip,
        cornerTooltip,
      };
    },
    [
      bubbleUserSelectedWeek,
      historicalTargetDataMode,
      updateCornerTooltip,
      updateVerticalIndicator,
      getHistoricalDataPoint,
    ]
  );

  /* Main useEffect() */
  useEffect(() => {
    // Console log the change to useEffect dependencies and what they are
    /* console.log('useEffect dependencies changed:', {
      groundTruthData,
      allModelPredictions,
      timeFilterRangeStart,
      timeFilterRangeEnd,
      userSelectedDate,
    }); */

    if (svgRef.current) {
      const svg = d3.select(svgRef.current);
      svg.selectAll('*').remove();

      const chartWidth = dimensions.width - margins.left - margins.right;
      const chartHeight = dimensions.height - margins.top - margins.bottom;
      const marginLeft = margins.left;
      const marginTop = margins.top;
      const marginBottom = margins.bottom;

      if (groundTruthData.length === 0) {
        console.warn('ForecastChart.tsx: groundTruthData is empty. Chart will not be rendered.');
        renderMessage(
          svg as any,
          'No data available for the selected filters.',
          chartWidth,
          chartHeight,
          marginLeft,
          marginTop
        );
        return;
      }

      const allPlaceholders = groundTruthData.every(
        (d) => d.observation === null || d.observation < 0
      );

      if (allPlaceholders && Object.keys(allModelPredictions).length === 0) {
        renderMessage(
          svg as any,
          'Not enough data, please extend date range.',
          chartWidth,
          chartHeight,
          marginLeft,
          marginTop
        );
        return;
      }

      const { xScale, yScale, xAxis, yAxis, xAxisWithSubTicks } = createScalesAndAxes(
        groundTruthData,
        allModelPredictions,
        chartWidth,
        chartHeight,
        yAxisScale
      );
      // Ensure `userSelectedWeek` is always within the current selected date range, snapping it inside if necessary
      let adjustedUserSelectedDate = new Date(userSelectedDate);
      // Snap to left/right bound depending to which direction it was originally out-of-bound
      if (adjustedUserSelectedDate < timeFilterRangeStart) {
        adjustedUserSelectedDate = new Date(groundTruthData[0].date);
        dispatch(updateUserSelectedDate(adjustedUserSelectedDate));
      } else if (adjustedUserSelectedDate > timeFilterRangeEnd) {
        adjustedUserSelectedDate = new Date(groundTruthData[groundTruthData.length - 1].date);
        dispatch(updateUserSelectedDate(adjustedUserSelectedDate));
      }

      // Render historical data if the mode is enabled
      if (historicalTargetDataMode) {
        renderHistoricalData(svg, historicalTimeSeries, xScale, yScale, marginLeft, marginTop);
      }

      renderGroundTruthData(svg, groundTruthData, xScale, yScale, marginLeft, marginTop);
      renderPredictionData(
        svg,
        allModelPredictions,
        xScale,
        yScale,
        marginLeft,
        marginTop,
        selectedPredictionIntervals,
        false
      );
      appendAxes(
        svg as any,
        xAxis,
        yAxis,
        xAxisWithSubTicks,
        xScale,
        marginLeft,
        marginTop,
        chartWidth,
        chartHeight,
        timeFilterRangeStart,
        timeFilterRangeEnd
      );

      const { mouseFollowLine, verticalIndicatorGroup, lineTooltip, cornerTooltip } =
        renderChartComponents(
          svg,
          groundTruthData,
          allModelPredictions,
          xScale,
          marginLeft,
          marginTop,
          chartWidth,
          chartHeight,
          dimensions.height,
          marginBottom
        );

      updateVerticalIndicator(
        adjustedUserSelectedDate,
        xScale,
        marginLeft,
        chartWidth,
        verticalIndicatorGroup,
        lineTooltip
      );
    }
  }, [
    dimensions,
    isResizing,
    margins,
    groundTruthData,
    allModelPredictions,
    selectedLocationCode,
    selectedModels,
    timeFilterRangeStart,
    timeFilterRangeEnd,
    yAxisScale,
    selectedPredictionIntervals,
    userSelectedDate,
    dispatch,
    createScalesAndAxes,
    historicalTargetDataMode,
    renderChartComponents,
    updateVerticalIndicator,
    renderHistoricalData,
    getHistoricalDataPoint,
    allPredictionIntervalOptions,
  ]);

  // Return the SVG object using reference
  return (
    <div ref={containerRef} className="flex w-full h-full">
      <svg
        ref={svgRef}
        width={'100%'}
        height={'100%'}
        className="w-full h-full"
        preserveAspectRatio="xMidYMid meet"
        viewBox={`0 0 ${dimensions.width || 100} ${dimensions.height || 100}`}
      ></svg>
    </div>
  );
};

export default ForecastChart;
