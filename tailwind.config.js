/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        'vv-cream': '#F9F6F0',
        'vv-burgundy': '#800020'
      },
      fontFamily: {
        sans: ['"DM Sans"', 'sans-serif'],
        mono: ['"DM Mono"', 'monospace'],
        serif: ['"Cormorant Garamond"', 'serif']
      }
    },
  },
  plugins: [],
}
