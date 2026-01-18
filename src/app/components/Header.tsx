"use client";
import Link from "next/link";
import React, { useEffect, useRef } from "react";
import { useAppSelector } from "@/store/hooks";
import { selectEvaluationsEnabled } from "@/store/selectors";

const Header: React.FC = () => {
  const headerRef = useRef<HTMLElement>(null);
  const evaluationsEnabled = useAppSelector(selectEvaluationsEnabled);
  const uiConfig = useAppSelector((state) => state.configStore.config?.uiCustomization);

  useEffect(() => {
    const updateHeaderHeight = () => {
      if (headerRef.current) {
        const headerHeight = headerRef.current.offsetHeight;
        document.documentElement.style.setProperty("--header-height", `${headerHeight}px`);
      }
    };

    updateHeaderHeight();
    window.addEventListener("resize", updateHeaderHeight);

    return () => window.removeEventListener("resize", updateHeaderHeight);
  }, []);

  // Parse the title to apply bold styling for "Abcd...Forecast" pattern
  const renderTitle = () => {
    const title = uiConfig?.header.titleName || "FluForecast";
    
    // Check if title follows "Abcd...Forecast" pattern (case-insensitive)
    const forecastMatch = title.match(/^(.+)(Forecast)$/i);
    
    if (forecastMatch) {
      // Apply special styling: bold first part, light weight "Forecast"
      return (
        <>
          {forecastMatch[1]}<span className="font-light text-5xl">{forecastMatch[2]}</span>
        </>
      );
    }
    
    // Otherwise, just bold everything
    return title;
  };

  // Render navigation buttons from config
  const renderNavButtons = () => {
    const navButtons = uiConfig?.header.navButtons;
    
    // If no nav buttons configured, use fallback with evaluations check
    if (!navButtons || navButtons.length === 0) {
      return (
        <>
          <Link href='/' className='text-2xl text-dashboard-background-color hover:text-teal-900'>
            Forecasts
          </Link>
          {evaluationsEnabled && (
            <Link href='/evaluations/' className='text-2xl text-dashboard-background-color hover:text-teal-900'>
              Evaluations
            </Link>
          )}
        </>
      );
    }

    // Render configured nav buttons
    return navButtons.map((btn, idx) => {
      // External link
      if (btn.navToExternal && btn.navToLink) {
        return (
          <Link
            key={idx}
            href={btn.navToLink}
            className='text-2xl text-dashboard-background-color hover:text-teal-900'
            target="_blank"
            rel="noopener noreferrer"
          >
            {btn.text}
          </Link>
        );
      }
      
      // Internal page link
      if (btn.navToPage) {
        const href = btn.navToPage === 'Evaluation' ? '/evaluations/' : '/';
        
        // Hide Evaluation link if evaluations are disabled
        if (btn.navToPage === 'Evaluation' && !evaluationsEnabled) {
          return null;
        }
        
        return (
          <Link
            key={idx}
            href={href}
            className='text-2xl text-dashboard-background-color hover:text-teal-900'
          >
            {btn.text}
          </Link>
        );
      }
      
      return null;
    });
  };

  return (
    <header ref={headerRef} className='bg-white text-dashboard-background-color shadow-md w-full'>
      <div className='container min-w-[100vw] px-4 py-3 flex w-full justify-between items-center max-h-[8vh]'>
        <div className='flex items-center'>
          <Link href='/' className='text-5xl font-bold mr-6 ml-4'>
            {renderTitle()}
          </Link>
        </div>
        <nav className='flex space-x-6 pr-4'>
          {renderNavButtons()}
        </nav>
      </div>
    </header>
  );
};

export default Header;
