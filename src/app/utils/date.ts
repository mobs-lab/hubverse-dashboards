export const isUTCDateEqual = (a: Date, b: Date) => {
  return (
    a.getUTCFullYear() === b.getUTCFullYear() &&
    a.getUTCMonth() === b.getUTCMonth() &&
    a.getUTCDate() === b.getUTCDate()
  );
};

/**
 * Parse a date string as UTC, handling both formats:
 * - "YYYY-MM-DD" (incomplete ISO - would be interpreted as local time by JS)
 * - "YYYY-MM-DDTHH:mm:ssZ" (full ISO UTC)
 *
 * This ensures consistent UTC parsing regardless of the format stored in JSON.
 *
 * @param dateStr - A date string in either format
 * @returns Date object in UTC
 */
export function parseUTCDate(dateStr: string): Date {
  // If already has time component (includes 'T'), parse directly
  if (dateStr.includes('T')) {
    return new Date(dateStr);
  }
  // For date-only strings, append UTC time to avoid local timezone interpretation
  return new Date(dateStr + 'T00:00:00Z');
}

/**
 * Convert a Date to UTC date key string for consistent key lookup in JSON data.
 *
 * Output format: "YYYY-MM-DDTHH:mm:ssZ" (e.g., "2023-04-01T00:00:00Z")
 *
 * This matches the format output by Python's to_utc_iso_string() function,
 * ensuring frontend can correctly look up data by date keys.
 *
 * @param date - A Date object
 * @returns UTC ISO string suitable for use as a key in date-indexed data
 */
export function toUTCDateKey(date: Date): string {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  return `${year}-${month}-${day}T00:00:00Z`;
}

/**
 * Convert a UTC date to a local date representing the same calendar day.
 * 
 * This is used when passing dates TO react-date-picker. For this dashboard it means the setting up of min/max selectable date from configurations, and refreshing displayed values when Redux updates via other channels (forecast period selection)
 * 
 * Problem Description: When we have a Date object representing "2025-10-18T00:00:00Z" (Oct 18 UTC),
 * and user is in EST (UTC-5), the browser displays this as "Oct 17, 7:00 PM EST".
 * The date picker then shows Oct 17 instead of Oct 18.
 * 
 * Solution: Extract the UTC year/month/day and create a new Date in LOCAL time
 * with those same values. So "2025-10-18T00:00:00Z" becomes "Oct 18 00:00 AM EST".
 * 
 * @param utcDate - Date object representing UTC midnight (e.g., from metadata)
 * @returns Date object representing local midnight with same calendar day
 * 
 */
export function utcToLocalDateSameDay(utcDate: Date): Date {
  // Extract UTC date components
  const year = utcDate.getUTCFullYear();
  const month = utcDate.getUTCMonth();
  const day = utcDate.getUTCDate();
  
  // Create new Date in LOCAL timezone with same calendar day
  return new Date(year, month, day, 0, 0, 0, 0);
}

/**
 * Convert a local date to a UTC date representing the same calendar day.
 * 
 * This is used when sending dates FROM react-date-picker (via selection).
 * 
 * Problem Description: When user selects "Oct 18" in the picker, it creates a Date representing
 * "Oct 18 00:00 AM EST" which is "2025-10-18T05:00:00Z" in UTC (5 hours ahead).
 * This doesn't match our data keys which are at UTC midnight "2025-10-18T00:00:00Z".
 * 
 * Solution: Extract the LOCAL year/month/day and create a new Date in UTC
 * with those same values. So "Oct 18 00:00 AM EST" becomes "2025-10-18T00:00:00Z".
 * 
 * @param localDate - Date object from date picker (local midnight)
 * @returns Date object representing UTC midnight with same calendar day
 */
export function localToUTCDateSameDay(localDate: Date): Date {
  // Extract LOCAL date components
  const year = localDate.getFullYear();
  const month = localDate.getMonth();
  const day = localDate.getDate();
  
  // Create new Date in UTC timezone with same calendar day
  return new Date(Date.UTC(year, month, day, 0, 0, 0, 0));
}

/**
 * Generate date ticks aligned to actual data dates
 *
 * This ensures the tick grid matches the natural grid of the data, preventing
 * misalignment issues when user-selected dates don't fall on data points.
 *
 * Strategy:
 * 1. Find the first actual data date >= user's start date (this becomes the "grid anchor")
 * 2. Generate ticks backward from anchor to cover range before it
 * 3. Generate ticks forward from anchor to cover full range
 *
 * This guarantees the tick grid will always align with actual data points.
 *
 * @param startDate - User-selected start date (may not be on data grid)
 * @param endDate - User-selected end date (may not be on data grid)
 * @param timeUnitDays - Interval between ticks in days (e.g., 7 for weekly)
 * @param actualDataDates - Array of actual dates that exist in the data (already normalized)
 * @returns Array of Date objects representing the tick grid, aligned to data
 */
export function generateAlignedDateTicks(
  startDate: Date,
  endDate: Date,
  timeUnitDays: number,
  actualDataDates: Date[]
): Date[] {
  const dates: Date[] = [];
  const msPerUnit = timeUnitDays * 24 * 60 * 60 * 1000;

  const normalizedStart = startDate;
  const normalizedEnd = endDate;

  // If no actual data, fall back to generating from start date
  if (!actualDataDates || actualDataDates.length === 0) {
    console.warn(
      '[generateAlignedDateTicks] No actual data dates provided, falling back to simple generation'
    );
    let currentDate = new Date(normalizedStart);
    while (currentDate <= normalizedEnd) {
      dates.push(new Date(currentDate));
      currentDate = new Date(currentDate.getTime() + msPerUnit);
    }
    return dates;
  }

  // Sort data dates to ensure we can find the proper anchor
  const sortedDataDates = actualDataDates.map(d => new Date(d)).sort((a, b) => a.getTime() - b.getTime());

  // Find the first data date that is >= start date to use as grid anchor
  // This ensures our tick grid aligns with actual data points
  const gridAnchor = sortedDataDates.find((d) => d >= normalizedStart) || sortedDataDates[0];

  // Generate ticks forward from the grid anchor to end date
  let currentDate = gridAnchor;
  while (currentDate <= normalizedEnd) {
    dates.push(currentDate);
    currentDate = new Date(currentDate.getTime() + msPerUnit);
  }

  return dates;
}
