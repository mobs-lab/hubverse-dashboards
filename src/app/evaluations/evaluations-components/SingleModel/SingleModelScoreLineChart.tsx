import * as d3 from 'd3';
import React, { useCallback, useEffect, useRef } from 'react';

import { useAppSelector } from '@/store/hooks';
import { selectSingleModelScoreDataFromJSON } from '@/store/selectors/index';
import { selectSingleModelTimeSeriesData } from '@/store/selectors/singleModelSelectors';
import { selectModelColorMap, selectTimeUnit } from '@/store/selectors';

import { useResponsiveSVG } from '@/utils/responsiveSVG';
import { normalizeToUTCMidDay } from '@/utils/date';

interface ScoreDataPoint {
  referenceDate: Date;
  score: number;
  horizon: number;
}

interface ProcessedScoreDataPoint {
  targetDate: Date;
  referenceDate: Date;
  score: number;
}

const SingleModelScoreLineChart: React.FC = () => {
  const { containerRef, dimensions, isResizing } = useResponsiveSVG();
  const chartRef = useRef<SVGSVGElement>(null);
  const isDraggingRef = useRef(false);

  const scoreDataFromJSON = useAppSelector(selectSingleModelScoreDataFromJSON);
  const timeSeriesData = useAppSelector(selectSingleModelTimeSeriesData);
  const modelColorMap = useAppSelector(selectModelColorMap);
  const timeUnit = useAppSelector(selectTimeUnit);

  const {
    evaluationsSingleModelViewModel,
    evaluationsSingleModelViewSelectedStateCode,
    evaluationSingleModelViewScoresOption,
    evaluationSingleModelViewHorizon,
  } = useAppSelector((state) => state.evaluationsSingleModelSettings);

  function createInteractiveElements(
    svg: d3.Selection<SVGSVGElement, unknown, null, undefined>,
    margin: { top: number; right: number; bottom: number; left: number },
    chartWidth: number,
    chartHeight: number
  ) {
    // Mouse follow line
    const mouseFollowLine = svg
      .append('line')
      .attr('class', 'mouse-follow-line')
      .attr('stroke', 'gray')
      .attr('stroke-width', 1)
      .attr('stroke-dasharray', '5,5')
      .attr('y1', margin.top)
      .attr('y2', chartHeight + margin.top)
      .style('opacity', 0);

    // Vertical indicator group
    const indicatorGroup = svg
      .append('g')
      .attr('class', 'vertical-indicator-group')
      .style('opacity', 0);

    indicatorGroup
      .append('line')
      .attr('class', 'vertical-indicator')
      .attr('stroke', 'lightgray')
      .attr('stroke-width', 2)
      .attr('y1', margin.top)
      .attr('y2', chartHeight + margin.top);

    const dateLabel = indicatorGroup
      .append('text')
      .attr('class', 'date-label')
      .attr('fill', 'white')
      .attr('font-size', '12px')
      .style('font-family', 'var(--font-dm-sans)')
      .attr('y', margin.top + 20);

    // Corner tooltip
    const cornerTooltip = svg.append('g').attr('class', 'corner-tooltip').style('opacity', 0);

    // Event capture area
    const eventOverlay = svg
      .append('rect')
      .attr('class', 'event-overlay')
      .attr('x', margin.left)
      .attr('y', margin.top)
      .attr('width', chartWidth)
      .attr('height', chartHeight)
      .style('fill', 'none')
      .style('pointer-events', 'all');

    return {
      mouseFollowLine,
      indicatorGroup,
      dateLabel,
      cornerTooltip,
      eventOverlay,
    };
  }

  function updateCornerTooltip(
    tooltip: d3.Selection<SVGGElement, unknown, null, undefined>,
    data: ProcessedScoreDataPoint,
    isRightSide: boolean,
    chartWidth: number,
    scoreOption: string
  ) {
    tooltip.selectAll('*').remove();

    const padding = 12;
    const background = tooltip.append('rect').attr('fill', '#333943').attr('rx', 8).attr('ry', 8);

    const dateText = tooltip
      .append('text')
      .attr('x', padding)
      .attr('y', padding + 12)
      .attr('fill', 'white')
      .attr('font-weight', 'bold')
      .style('font-family', 'var(--font-dm-sans)')
      .style('font-size', '15px')
      .style('line-height', '22.5px')
      .text(`Date: ${data.targetDate.toUTCString().slice(5, 16)}`);

    const refDateText = tooltip
      .append('text')
      .attr('x', padding)
      .attr('y', padding + 36)
      .attr('fill', 'white')
      .style('font-family', 'var(--font-dm-sans)')
      .style('font-size', '13px')
      .style('line-height', '22.5px')
      .text(`Forecast Submission Date: ${data.referenceDate.toUTCString().slice(5, 16)}`);

    const scoreText = tooltip
      .append('text')
      .attr('x', padding)
      .attr('y', padding + 60)
      .attr('fill', 'white')
      .style('font-family', 'var(--font-dm-sans)')
      .style('font-size', '13px')
      .style('line-height', '22.5px')
      .text(
        `${scoreOption}: ${scoreOption === 'MAPE' ? `${data.score.toFixed(1)}%` : data.score.toFixed(3)}`
      );

    const textWidth = Math.max(
      dateText.node()!.getComputedTextLength(),
      refDateText.node()!.getComputedTextLength(),
      scoreText.node()!.getComputedTextLength()
    );

    background.attr('width', textWidth + padding * 2).attr('height', 84);

    const tooltipX = isRightSide ? chartWidth - textWidth + padding * 3 : padding * 5;

    tooltip.attr('transform', `translate(${tooltipX}, 10)`).style('opacity', 1);
  }

  function findClosestDataPoint(
    mouseX: number,
    xScale: d3.ScaleBand<string>,
    margin: any,
    filteredData: ProcessedScoreDataPoint[]
  ): ProcessedScoreDataPoint | null {
    if (filteredData.length === 0) return null;

    // Adjust mouseX to account for margin
    const adjustedX = mouseX - margin.left;

    // Get all the dates in our scale
    const dates = xScale.domain().map((dateStr) => new Date(dateStr));

    // Find the closest date based on x position
    const bandWidth = xScale.bandwidth();
    const step = xScale.step();
    const index = Math.floor(adjustedX / step);

    // Ensure we're within bounds
    if (index < 0) return filteredData[0];
    if (index >= dates.length) return filteredData[filteredData.length - 1];

    // Find the actual data point closest to this date
    const targetDate = dates[index];
    return filteredData.find((d) => d.targetDate.getTime() === targetDate.getTime()) || null;
  }

  function createScalesAndAxes(
    allDateTicks: Date[],
    processedData: ProcessedScoreDataPoint[],
    chartWidth: number,
    chartHeight: number,
    scoreOption: string
  ) {
    const getIdealTickCount = (width: number, totalTicks: number) => {
      if (width < 500) {
        return Math.min(Math.max(6, Math.min(totalTicks, 12)), 12);
      } else {
        return Math.min(Math.max(8, Math.min(totalTicks, 18)), 18);
      }
    };

    const idealTickCount = getIdealTickCount(chartWidth, allDateTicks.length);
    let selectedTicks = allDateTicks;

    if (allDateTicks.length > idealTickCount) {
      const tickInterval = Math.max(1, Math.floor(allDateTicks.length / idealTickCount));
      selectedTicks = allDateTicks.filter((_, i) => i % tickInterval === 0);

      // Ensure first and last are included
      if (selectedTicks[0].getTime() !== allDateTicks[0].getTime()) {
        selectedTicks.unshift(allDateTicks[0]);
      }
      if (
        selectedTicks[selectedTicks.length - 1].getTime() !==
        allDateTicks[allDateTicks.length - 1].getTime()
      ) {
        selectedTicks.push(allDateTicks[allDateTicks.length - 1]);
      }
    }
    // Create band scale for x-axis
    const xScale = d3
      .scaleBand()
      .domain(allDateTicks.map((d) => d.toISOString()))
      .range([0, chartWidth])
      .padding(0.08);

    // Calculate y-scale domain
    const scores = processedData.map((d) => d.score);
    const maxScore = scores.length > 0 ? Math.max(...scores) : 1;
    const yDomain = [0, maxScore * 1.02];

    const yScale = d3.scaleLinear().domain(yDomain).range([chartHeight, 0]).nice();

    // Create axes
    const xAxis = d3
    .axisBottom(xScale)
    .tickValues(selectedTicks.map(d => d.toISOString()))
    .tickFormat((d: string, i: number) => {
        const date = new Date(d);
        const year = d3.timeFormat('%Y')(date);
        const month = d3.timeFormat('%b')(date);
        const day = d3.timeFormat('%d')(date);

        const prevTickDate = i > 0 ? new Date(selectedTicks[i-1].toISOString()) : null;
        
        const isNewYear = prevTickDate ? date.getFullYear() > prevTickDate.getFullYear() : true;
        const isNewMonth = prevTickDate ? date.getMonth() !== prevTickDate.getMonth() : true;

        if (chartWidth < 500) {
          if (isNewYear) return `${year}\n${month}`;
          if (isNewMonth) return month;
          return "";
        } else {
          if (isNewYear) return `${year}\n${month}\n${day}`;
          if (isNewMonth) return `${month}\n${day}`;
          return day;
        }
      });

    const yAxis = d3.axisLeft(yScale).tickFormat((d: d3.NumberValue, i: number) => {
      const value = Number(d.valueOf());
      if (scoreOption === 'MAPE') {
        return value >= 10 ? `${value.toFixed(0)}%` : `${value.toFixed(1)}%`;
      }
      return value.toFixed(1);
    });

    return { xScale, yScale, xAxis, yAxis };
  }

  const updateVisuals = useCallback(
    (
      event: any,
      {
        mouseFollowLine,
        indicatorGroup,
        dateLabel,
        cornerTooltip,
        xScale,
        margin,
        chartWidth,
        processedData,
        isDragging,
        scoreOption,
      }: {
        mouseFollowLine: d3.Selection<SVGLineElement, unknown, null, undefined>;
        indicatorGroup: d3.Selection<SVGGElement, unknown, null, undefined>;
        dateLabel: d3.Selection<SVGTextElement, unknown, null, undefined>;
        cornerTooltip: d3.Selection<SVGGElement, unknown, null, undefined>;
        xScale: d3.ScaleBand<string>;
        margin: { top: number; right: number; bottom: number; left: number };
        chartWidth: number;
        processedData: ProcessedScoreDataPoint[];
        isDragging: boolean;
        scoreOption: string;
      }
    ) => {
      const [mouseX] = d3.pointer(event);
      const dataPoint = findClosestDataPoint(mouseX, xScale, margin, processedData);

      if (!dataPoint) return;

      // Calculate position using the band scale
      const xPos =
        (xScale(dataPoint.targetDate.toISOString()) || 0) + xScale.bandwidth() / 2 + margin.left;
      const isRightSide = mouseX < chartWidth / 2;

      mouseFollowLine.attr('transform', `translate(${xPos}, 0)`).style('opacity', 1);

      if (isDragging) {
        indicatorGroup.attr('transform', `translate(${xPos}, 0)`).style('opacity', 1);

        dateLabel
          .attr('x', isRightSide ? 5 : -5)
          .attr('text-anchor', isRightSide ? 'start' : 'end')
          .text(dataPoint.targetDate.toUTCString().slice(5, 16));
      }

      updateCornerTooltip(cornerTooltip, dataPoint, isRightSide, chartWidth, scoreOption);
    },
    []
  );

  function renderVisualElements(
    chart: d3.Selection<SVGGElement, unknown, null, undefined>,
    processedData: ProcessedScoreDataPoint[],
    xScale: d3.ScaleBand<string>,
    yScale: d3.ScaleLinear<number, number>,
    modelName: string,
    scoreOption: string
  ) {
    // Draw reference line at y = 1 for WIS_ratio
    if (scoreOption === 'WIS/Baseline') {
      chart
        .append('line')
        .attr('x1', 0)
        .attr('x2', xScale.range()[1])
        .attr('y1', yScale(1))
        .attr('y2', yScale(1))
        .attr('stroke', 'white')
        .attr('stroke-width', 1)
        .attr('stroke-dasharray', '1,0')
        .attr('opacity', 0.8);
    }

    // Create container for all visual elements
    const visualContainer = chart.append('g').attr('class', 'visual-container');

    // Create specific groups for different visual elements
    const linesGroup = visualContainer.append('g').attr('class', 'lines');
    const pointsGroup = visualContainer.append('g').attr('class', 'points');

    // Sort data by target date to ensure proper line drawing
    const sortedData = [...processedData].sort(
      (a, b) => a.targetDate.getTime() - b.targetDate.getTime()
    );

    // Modified line generator - ensure data is sorted
    const line = d3
      .line<ProcessedScoreDataPoint>()
      .defined((d) => {
        const xPos = xScale(d.targetDate.toISOString());
        const isValid = !isNaN(d.score) && isFinite(d.score) && xPos !== undefined;
        if (!isValid) {
          console.warn(`[renderVisualElements] Skipping invalid point:`, {
            date: d.targetDate.toISOString(),
            score: d.score,
            xPos,
          });
        }
        return isValid;
      })
      .x((d) => (xScale(d.targetDate.toISOString()) || 0) + xScale.bandwidth() / 2)
      .y((d) => yScale(d.score));

    // Draw line with sorted data
    linesGroup
      .append('path')
      .datum(sortedData)
      .attr('fill', 'none')
      .attr('stroke', modelColorMap[modelName])
      .attr('stroke-width', 2)
      .attr('d', line);

    // Draw points with sorted data
    pointsGroup
      .selectAll('circle')
      .data(sortedData)
      .enter()
      .append('circle')
      .attr('cx', (d) => (xScale(d.targetDate.toISOString()) || 0) + xScale.bandwidth() / 2)
      .attr('cy', (d) => yScale(d.score))
      .attr('r', 4)
      .attr('fill', modelColorMap[modelName]);
  }

  /**
   * Helper function to wrap x-axis labels for better readability
   */
  function wrapAxisLabels(text: d3.Selection<d3.BaseType, unknown, SVGGElement, unknown>) {
    text.each(function () {
      const text = d3.select(this);
      const lines = text.text().split(/\n+/);
      const x = text.attr('x') || 0;
      const y = text.attr('y') || 0;
      const dy = parseFloat(text.attr('dy') || '0');

      // Clear existing content
      text.text(null);

      // Calculate appropriate line heights based on number of lines
      // More lines need more spacing to avoid overlap
      const lineHeight = lines.length > 2 ? 1.8 : 1.6;

      // Create a tspan for each line with progressively increasing offsets
      lines.forEach((line, i) => {
        // For 3-line labels, increase vertical spacing between lines 2 and 3
        const currentDy = i === 0 ? dy : i === 2 ? lineHeight * 1.6 : lineHeight;

        text
          .append('tspan')
          .attr('x', x)
          .attr('y', y)
          .attr('dy', (i === 0 ? dy : currentDy) + 'em')
          .text(line);
      });
    });
  }

  /**
   * Generate date ticks for x-axis using configured time unit
   * Generates dates at intervals based on the time unit from config (e.g., 7 days, 14 days, etc.)
   */
  function generateDateTicks(startDate: Date, endDate: Date, timeUnitDays: number): Date[] {
    const dates: Date[] = [];
    const msPerUnit = timeUnitDays * 24 * 60 * 60 * 1000;

    // Normalize start and end dates to UTC midday
    let currentDate = normalizeToUTCMidDay(startDate);
    const normalizedEndDate = normalizeToUTCMidDay(endDate);

    // Generate dates at time unit intervals until end date
    while (currentDate <= normalizedEndDate) {
      dates.push(new Date(currentDate));
      currentDate = new Date(currentDate.getTime() + msPerUnit);
    }

    return dates;
  }

  const renderChart = useCallback(() => {
    if (!chartRef.current || !dimensions.width || !dimensions.height) return;

    const svg = d3.select(chartRef.current);
    svg.selectAll('*').remove();

    if (!scoreDataFromJSON || scoreDataFromJSON.length === 0 || !timeSeriesData) {
      svg
        .append('text')
        .attr('x', dimensions.width / 2)
        .attr('y', dimensions.height / 2)
        .attr('text-anchor', 'middle')
        .attr('fill', 'white')
        .style('font-family', 'var(--font-dm-sans)')
        .text('No score data available for selected criteria');
      return;
    }
    // Get the time range from metadata to sync up with horizon plot
    const { displayStartDate, displayEndDate } = timeSeriesData.metadata;

    // Get actual score data dates and normalize to UTC midday
    const normalizedScoreData = scoreDataFromJSON.map((entry) => ({
      ...entry,
      targetEndDate: normalizeToUTCMidDay(entry.targetEndDate),
      referenceDate: normalizeToUTCMidDay(entry.referenceDate),
    }));

    // Check for duplicate dates or invalid scores
    const seenDates = new Set<string>();
    const duplicates: string[] = [];
    normalizedScoreData.forEach((entry) => {
      const dateKey = entry.targetEndDate.toISOString();
      if (seenDates.has(dateKey)) {
        duplicates.push(dateKey);
      }
      seenDates.add(dateKey);

      if (!isFinite(entry.score) || isNaN(entry.score)) {
        console.warn(`[SingleModelScoreLineChart] Invalid score detected:`, {
          date: dateKey,
          score: entry.score,
          refDate: entry.referenceDate.toISOString(),
        });
      }
    });

    if (duplicates.length > 0) {
      console.warn(`[SingleModelScoreLineChart] Duplicate dates found:`, duplicates);
    }

    // Generate full date range from metadata to ensure x-axis synchronization with horizon plot
    // This creates a consistent date axis regardless of which dates have actual score data
    const allDateTicks = generateDateTicks(
      new Date(displayStartDate),
      new Date(displayEndDate),
      timeUnit
    );

    // Create processed data directly from scoreDataFromJSON, filtering out invalid scores
    const processedData = normalizedScoreData
      .filter((entry) => isFinite(entry.score) && !isNaN(entry.score))
      .map((entry) => ({
        targetDate: entry.targetEndDate,
        referenceDate: entry.referenceDate,
        score: entry.score,
      }));

    console.debug(
      `[SingleModelScoreLineChart] Processed ${processedData.length} valid data points`,
      {
        firstPoint: processedData[0],
        lastPoint: processedData[processedData.length - 1],
      }
    );

    // Setup dimensions
    const width = dimensions.width;
    const height = dimensions.height;
    const margin = {
      top: Math.max(height * 0.018, 20),
      right: Math.max(width * 0.005, 5),
      bottom: Math.max(height * 0.215, 20),
      left: Math.max(width * 0.005, 50),
    };
    const chartWidth = width - margin.left - margin.right;
    const chartHeight = height - margin.top - margin.bottom;

    // Handle when no data is present
    if (processedData.length === 0) {
      svg
        .append('text')
        .attr('x', width / 2)
        .attr('y', height / 2)
        .attr('text-anchor', 'middle')
        .attr('fill', 'white')
        .style('font-family', 'var(--font-dm-sans)')
        .text('No score data available for selected criteria');
      return;
    }

    // Create scales and axes
    const { xScale, yScale, xAxis, yAxis } = createScalesAndAxes(
      allDateTicks, // Use actual data dates for x-axis
      processedData, // Use only actual data for y-scale
      chartWidth,
      chartHeight,
      evaluationSingleModelViewScoresOption
    );

    // Create main chart group
    const chart = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

    // Render visual elements
    renderVisualElements(
      chart,
      processedData,
      xScale,
      yScale,
      evaluationsSingleModelViewModel,
      evaluationSingleModelViewScoresOption
    );

    // Add axes with styling
    chart
      .append('g')
      .attr('transform', `translate(0,${chartHeight})`)
      .style('font-family', 'var(--font-dm-sans)')
      .call(xAxis as any)
      .selectAll('.tick text')
      .style('text-anchor', 'middle')
      .style('font-size', '13px')
      .each(function () {
        // Ensure dy is a string for wrapAxisLabels
        const t = d3.select(this);
        if (t.attr('dy') === null) t.attr('dy', '0');
      })
      .call(wrapAxisLabels, 20);

    chart
      .append('g')
      .style('font-family', 'var(--font-dm-sans)')
      .call(yAxis as any)
      .call((g) => g.select('.domain').remove())
      .call((g) =>
        g
          .selectAll('.tick line')
          .attr('stroke-opacity', 0.5)
          .attr('stroke-dasharray', '2,2')
          .attr('x2', chartWidth)
      )
      .style('font-size', '18px');

    // Add interactivity
    const interactiveElements = createInteractiveElements(svg, margin, chartWidth, chartHeight);
    const { mouseFollowLine, indicatorGroup, dateLabel, cornerTooltip, eventOverlay } =
      interactiveElements;

    // Add interaction handlers
    let isDragging = isDraggingRef.current;

    eventOverlay
      .on('mousemove', (event) => {
        const params = {
          mouseFollowLine,
          indicatorGroup,
          dateLabel,
          cornerTooltip,
          xScale,
          margin,
          chartWidth,
          processedData,
          isDragging,
          scoreOption: evaluationSingleModelViewScoresOption,
        };
        updateVisuals(event, params);
      })
      .on('mouseout', () => {
        mouseFollowLine.style('opacity', 0);
        isDragging = false;
      })
      .on('mousedown', (event) => {
        isDraggingRef.current = true;
        isDragging = true;
        const params = {
          mouseFollowLine,
          indicatorGroup,
          dateLabel,
          cornerTooltip,
          xScale,
          margin,
          chartWidth,
          processedData,
          isDragging,
          scoreOption: evaluationSingleModelViewScoresOption,
        };
        updateVisuals(event, params);
        indicatorGroup.style('opacity', 1);
      })
      .on('mouseup', () => {
        isDragging = false;
      })
      .on('mouseleave', () => {
        isDragging = false;
      });

    // Ensure tooltip is always on top
    cornerTooltip.raise();
  }, [
    dimensions.width,
    dimensions.height,
    scoreDataFromJSON,
    timeSeriesData,
    evaluationSingleModelViewScoresOption,
    evaluationsSingleModelViewModel,
    updateVisuals,
  ]);

  useEffect(() => {
    if (!isResizing && dimensions.width > 0 && dimensions.height > 0) {
      renderChart();
    }
  }, [
    dimensions,
    isResizing,
    scoreDataFromJSON,
    timeSeriesData,
    evaluationsSingleModelViewModel,
    evaluationsSingleModelViewSelectedStateCode,
    evaluationSingleModelViewScoresOption,
    evaluationSingleModelViewHorizon,
    renderChart,
  ]);

  return (
    <div ref={containerRef} className="w-full h-full">
      <svg
        ref={chartRef}
        width="100%"
        height="100%"
        className="w-full h-full"
        viewBox={`0 0 ${dimensions.width || 100} ${dimensions.height || 100}`}
        preserveAspectRatio="xMidYMid meet"
      />
    </div>
  );
};

export default SingleModelScoreLineChart;
