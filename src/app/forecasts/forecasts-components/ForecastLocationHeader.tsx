import { useAppSelector } from '@/store/hooks';
import { selectSelectedLocationName, selectConfig } from '@/store/selectors/forecastSelectors';
import React from 'react';

const ForecastLocationHeader: React.FC = () => {
  const selectedLocationName = useAppSelector(selectSelectedLocationName);
  const config = useAppSelector(selectConfig);
  const isSingleLocation = config?.isSingleLocation ?? false;

  // Format location name to title case
  // Note: if 2 letters or less, capitalize all (e.g., "US" stays "US", not "Us")
  const formattedLocationName = String(selectedLocationName || 'US')
    .split(' ')
    .map((word) => {
      if (word.length <= 2) {
        return word.toUpperCase();
      }
      return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
    })
    .join(' ');

  return (
    <div className="w-full h-full text-nowrap flex flex-shrink flex-col justify-evenly flex-nowrap px-4 pt-2 pb-4 util-responsive-text">
      <h1 className="sm:text-sm md:text-base lg:text-2xl xl:text-3xl 2xl:text-4xl font-light util-text-limit">
        {formattedLocationName}
      </h1>
      <div className="w-full bg-[#5d636a]">
        <svg className="w-full h-0.5">
          <line x1="0" y1="0" x2="100%" y2="0" stroke="#5d636a" strokeWidth="1" />
        </svg>
      </div>
    </div>
  );
};

export default ForecastLocationHeader;
