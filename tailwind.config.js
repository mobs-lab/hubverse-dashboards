/** @type {import('tailwindcss').Config} */

import withMT from "@material-tailwind/react/utils/withMT";

export default withMT({

    content: [// Using `src` directory:
        './src/**/*.{js,ts,jsx,tsx,mdx}',],

    theme: {
        extend: {
            fontFamily: {
                sans: ['var(--font-dm-sans)'],
            }, colors: {
                'dashboard-background-color': '#252a33',
                'dashboard-settings-panel-color': '#323944',
            },
        },
    },
    plugins: [],
    corePlugins: {
        preflight: true,
    },
})

;

