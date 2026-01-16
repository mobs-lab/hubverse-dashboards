import React, { useCallback } from 'react';
import DatePicker from 'react-date-picker';
import 'react-date-picker/dist/DatePicker.css';
import 'react-calendar/dist/Calendar.css';
import '../../css/component_styles/StyledDatePicker.css';
import {  normalizeToUTCMidnight } from '@/utils/date';

interface StyledDatePickerProps {
    value: Date | null;
    onChange: (date: Date | null) => void;
    minDate?: Date;
    maxDate?: Date;
    className?: string;
}

/**
 * Date picker component that normalizes all dates to UTC midnight.
 * 
 * The react-date-picker returns dates in local timezone when user picks a date.
 * This wrapper ensures the date is converted to UTC midnight (00:00:00Z) before
 * being passed to the onChange handler, maintaining consistency with UTC-based
 * date keys in the data.
 */
const SettingsStyledDatePicker: React.FC<StyledDatePickerProps> = ({value, onChange, minDate, maxDate, className}) => {
    /**
     * Handle date change by normalizing to UTC midnight
     * This prevents timezone-related issues when comparing dates
     */
    const handleDateChange = useCallback((date: Date | null) => {
        if (date) {
            // Normalize the local date to UTC midnight
            // This ensures consistent date handling regardless of user's timezone
            const utcDate = normalizeToUTCMidnight(date);
            onChange(utcDate);
        } else {
            onChange(null);
        }
    }, [onChange]);

    return (
        <div className={`styled-date-picker ${className}`}>
            <DatePicker
                onChange={handleDateChange}
                value={value}
                minDate={minDate}
                maxDate={maxDate}
                format="y-MM-dd"
                className="custom-date-picker"
                calendarClassName="custom-calendar"
                clearIcon={null}
                calendarIcon={null}
                
            />
        </div>
    );
};

export default SettingsStyledDatePicker;