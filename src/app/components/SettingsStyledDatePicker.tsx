import React, { useCallback, useMemo } from 'react';
import DatePicker, { DatePickerProps } from 'react-date-picker';
import 'react-date-picker/dist/DatePicker.css';
import 'react-calendar/dist/Calendar.css';
import '@/styles/component_styles/StyledDatePicker.css';
import { utcToLocalDateSameDay, localToUTCDateSameDay } from '@/utils/date';

interface StyledDatePickerProps {
    value: Date | null;
    onChange: (date: Date | null) => void;
    minDate?: Date;
    maxDate?: Date;
    className?: string;
}

/**
 * Date picker component that properly handles UTC dates with react-date-picker.
 * - Convert UTC dates TO local dates (same calendar day) before passing to picker
 * - Convert local dates FROM picker back to UTC dates (same calendar day)
 * - User sees correct calendar dates regardless of timezone
 * - Data filtering works correctly with UTC keys in the Redux backend
 */
const SettingsStyledDatePicker: React.FC<StyledDatePickerProps> = ({
    value, 
    onChange, 
    minDate, 
    maxDate, 
    className
}) => {
    /**
     * Convert UTC dates to local dates for the picker
     * This ensures the picker displays the correct calendar day
     */
    const localValue = useMemo(() => 
        value ? utcToLocalDateSameDay(value) : null,
        [value]
    );
    
    const localMinDate = useMemo(() => 
        minDate ? utcToLocalDateSameDay(minDate) : undefined,
        [minDate]
    );
    
    const localMaxDate = useMemo(() => 
        maxDate ? utcToLocalDateSameDay(maxDate) : undefined,
        [maxDate]
    );

    /**
     * Handle outgoing date change by converting local date back to UTC
     */
    const handleDateChange = useCallback((value: Date | null) => {
        // react-date-picker can return Date, null, or Range (for range picker)
        // We only use single date mode, so value should be Date | null
        if (value && value instanceof Date) {
            // Convert local date back to UTC with same calendar day
            const utcDate = localToUTCDateSameDay(value);
            onChange(utcDate);
        } else {
            onChange(null);
        }
    }, [onChange]);

    return (
        <div className={`styled-date-picker ${className}`}>
            <DatePicker
                onChange={handleDateChange as any}
                value={localValue}
                minDate={localMinDate}
                maxDate={localMaxDate}
                format="y-MM-dd"
                className="custom-date-picker"
                clearIcon={null}
                calendarIcon={null}
            />
        </div>
    );
};

export default SettingsStyledDatePicker;