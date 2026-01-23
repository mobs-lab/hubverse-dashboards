"""
Forecast Period Utilities for Hubverse Dashboard

Helper functions for computing ongoing and special forecast period metadata.
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

from yaml_config_processor_pydantic import ForecastPeriodConfig, SpecialForecastPeriodConfig
from data_utils import to_utc_iso_string

logger = logging.getLogger(__name__)


def compute_ongoing_period_metadata(
    period: ForecastPeriodConfig, 
    target_data_df: pd.DataFrame,
    model_output_df: pd.DataFrame
) -> dict:
    """
    Compute metadata for an ongoing forecast period.
    
    An ongoing period has an end_date in the future. We need to:
    1. Determine if the period is ongoing (end date > today)
    2. Find the actual latest date in data (both target and model output)
    3. Find the anchor date (latest date with actual target-data)
    
    Args:
        period: Forecast period configuration
        target_data_df: Target data
        model_output_df: Model output data
    
    Returns:
        dict with keys:
            - isOngoing (bool)
            - configuredEndDate (str): User-defined end date
            - actualEndDate (str): Latest date in data so far
            - anchorDate (str): Latest date with target-data
    """
    result = {
        "isOngoing": False,
        "configuredEndDate": to_utc_iso_string(period.end_date),
        "actualEndDate": None,
        "anchorDate": None,
    }
    
    # Check if ongoing (end date in future)
    today = datetime.now().date()
    if period.end_date.date() <= today:
        # Static period - fully in the past
        result["actualEndDate"] = result["configuredEndDate"]
        return result
    
    result["isOngoing"] = True
    
    # Find actual latest date in data within this period
    period_start = pd.to_datetime(period.start_date)
    period_end = pd.to_datetime(period.end_date)
    
    latest_dates = []
    
    # Check target data
    if not target_data_df.empty:
        target_dates = target_data_df["date"]
        if not pd.api.types.is_datetime64_any_dtype(target_dates):
            target_dates = pd.to_datetime(target_dates)
        
        period_target = target_dates[
            (target_dates >= period_start) & (target_dates <= period_end)
        ]
        if not period_target.empty:
            latest_target_date = period_target.max()
            latest_dates.append(latest_target_date)
            # Anchor is the latest target-data date
            result["anchorDate"] = to_utc_iso_string(latest_target_date)
    
    # Check model output
    if not model_output_df.empty:
        model_dates = model_output_df["target_end_date"]
        if not pd.api.types.is_datetime64_any_dtype(model_dates):
            model_dates = pd.to_datetime(model_dates)
        
        period_model = model_dates[
            (model_dates >= period_start) & (model_dates <= period_end)
        ]
        if not period_model.empty:
            latest_dates.append(period_model.max())
    
    # Actual end date is the latest of all dates in data
    if latest_dates:
        actual_end = max(latest_dates)
        result["actualEndDate"] = to_utc_iso_string(actual_end)
    else:
        # No data yet for this period
        result["actualEndDate"] = to_utc_iso_string(period_start)
        result["anchorDate"] = to_utc_iso_string(period_start)
    
    return result


def compute_special_period_date_range(
    special_period: SpecialForecastPeriodConfig,
    ongoing_period_metadata: dict,
    time_unit: int
) -> dict:
    """
    Compute date range for a special forecast period anchored to an ongoing period.
    
    Special periods are defined relative to the anchor date of their parent ongoing period.
    For example: "Last 4 Weeks" = anchor_date + (range_calculation * time_unit * -1 days)
    
    Args:
        special_period: Special period configuration
        ongoing_period_metadata: Metadata for the ongoing period it anchors to
        time_unit: Time unit in days from config
    
    Returns:
        dict with startDate, endDate, anchorDate, isDynamic
    """
    result = {
        "startDate": None,
        "endDate": None,
        "anchorDate": None,
        "isDynamic": True,
        "anchoredTo": special_period.time_anchor.anchor_on,
    }
    
    # Get anchor date from ongoing period
    anchor_str = ongoing_period_metadata.get("anchorDate")
    if not anchor_str:
        logger.warning(f"Special period '{special_period.special_period_id}' cannot find anchor date")
        return result
    
    anchor_date = pd.to_datetime(anchor_str)
    result["anchorDate"] = anchor_str
    result["endDate"] = anchor_str  # End at anchor
    
    # Calculate start date based on range_calculation
    # range_calculation is negative (e.g., -2 means 2 time units back)
    range_calc = special_period.time_anchor.range_calculation
    
    # Calculate how many days to go back
    days_back = abs(range_calc) * time_unit
    start_date = anchor_date - timedelta(days=days_back)
    result["startDate"] = to_utc_iso_string(start_date)
    
    logger.info(f"  Special period '{special_period.special_period_id}': "
               f"{start_date.date()} to {anchor_date.date()} "
               f"({abs(range_calc)} time units from anchor)")
    
    return result
