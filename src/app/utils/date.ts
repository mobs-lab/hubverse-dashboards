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
 * Normalize a user-selected Date (which may be in local time from date picker)
 * to UTC midnight. This is useful when user picks a date and we need to
 * store/compare it as UTC.
 *
 * @param date - A Date object (possibly from a date picker in local time)
 * @returns Date object normalized to UTC midnight
 */
export function normalizeToUTCMidnight(date: Date): Date {
  return new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate(), 0, 0, 0, 0));
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
