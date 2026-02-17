#!/bin/bash

# build_dashboard.sh
# Main entry point for Hubverse Dashboard setup and data processing
# This script provides an interactive menu for users to build the dashboard
# or update data.

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RD='\033[0m' # Reset to default color

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Function to print colored messages
print_error() {
    echo -e "${RED}✗ ERROR: $1${RD}"
}

print_success() {
    echo -e "${GREEN}✓ $1${RD}" 
}

print_warning() {
    echo -e "${YELLOW}⚠ WARNING: $1${RD}"
}

print_info() {
    echo -e "${BLUE}ℹ $1${RD}"
}

# Function to print header
print_header() {
    echo ""
    echo "=========================================================================="
    echo "==========              HUBVERSE DASHBOARD BUILDER              =========="
    echo "=========================================================================="
    echo ""
}

# Function to check if npm is available
check_npm() {
    if ! command -v npm &> /dev/null; then
        print_error "npm is not installed or not in PATH"
        echo ""
        echo "npm is required to run the dashboard. Please install Node.js and npm:"
        echo "  - Visit https://nodejs.org/"
        echo ""
        exit 1
    fi
    print_success "Found npm ($(npm --version))"
}

# Function to check if config.yaml exists
check_config() {
    if [ ! -f "config.yaml" ]; then
        print_error "config.yaml not found in project root"
        echo ""
        echo "Please create a config.yaml file before proceeding."
        echo "You can copy an example configuration and customize it:"
        echo "  cp config-file-examples/config.yaml.example config.yaml"
        echo ""
        exit 1
    fi
    print_success "Found config.yaml"
}

# Function to build and serve Sphinx documentation
build_and_serve_docs() {
    echo ""
    print_info "Building HTML documentation with Sphinx..."
    echo ""

    # Check if Python is available
    if ! command -v python3 &> /dev/null; then
        print_error "python3 is not installed or not in PATH"
        echo "  Python 3.9+ is required to build the documentation."
        echo "  Visit https://www.python.org/downloads/"
        return 1
    fi
    print_success "Found python3 ($(python3 --version 2>&1 | awk '{print $2}'))"

    # Install dev dependencies
    print_info "Installing documentation dependencies from requirements-dev.txt..."
    if ! pip install -r requirements-dev.txt; then
        print_error "Failed to install documentation dependencies."
        echo ""
        echo "  Make sure you have activated your virtual environment:"
        echo "    source .venv/bin/activate"
        echo "  Then try again."
        return 1
    fi
    print_success "Documentation dependencies installed"

    # Build the docs
    print_info "Running Sphinx build..."
    if ! sphinx-build -b html docs/source docs/build/html; then
        print_error "Sphinx build failed. Check the errors above."
        return 1
    fi
    print_success "Documentation built successfully at docs/build/html/"

    # Serve locally
    echo ""
    print_info "Starting local documentation server on http://localhost:8080"
    print_info "Press Ctrl+C to stop the server."
    echo ""
    python3 -m http.server 8080 --directory docs/build/html
}

# Main menu function
show_menu() {
    print_header
    echo "Please select an option:"
    echo ""
    echo "  === Documentation ==="
    echo "  0) Build & Serve HTML Documentation (Sphinx)"
    echo ""
    echo "  === Initial Dashboard Build ==="
    echo "  1) Build Dashboard - Full (with evaluations)"
    echo ""
    echo "  2) Build Dashboard - Without Evaluations (disables Evaluations page)"
    echo ""
    echo "  3) Build Dashboard - Dev Mode (with evaluations)"
    echo ""
    echo "  4) Build Dashboard - Dev Mode (without evaluations)"
    echo ""
    echo "  === Data Update ==="
    echo "  5) Update Data - Production Mode"
    echo ""
    echo "  6) Update Data - Dev Mode"
    echo ""
    echo "  7) Exit"
    echo ""
}

# Function to prompt user to choose how to run the dashboard
show_run_options_menu() {
    local is_dev_mode=$1
    
    echo ""
    echo "=========================================================================="
    echo "Data processing complete! Choose how to run the dashboard:"
    echo "=========================================================================="
    echo ""
    echo "  1) Run Development Server (npm run dev)"
    echo "     - Hot reload enabled for development"
    echo "     - Runs on http://localhost:3000"
    echo "     - Press Ctrl+C to stop"
    echo ""
    echo "  2) Build and Serve Production (npm run build + npm run start)"
    echo "     - Optimized production build"
    echo "     - Runs on http://localhost:3000"
    echo "     - Press Ctrl+C to stop"
    echo ""
    echo "  3) Exit (Run it manually later)"
    echo ""
    
    # Show warning if dev mode + production build
    if [ "$is_dev_mode" = "true" ]; then
        print_warning "Development mode is enabled - data is in public/test-data-output/"
        echo ""
    fi
}

# Function to run npm development server
run_dev_server() {
    local is_dev_mode=$1
    
    echo ""
    print_info "Starting development server..."
    echo ""
    
    if [ "$is_dev_mode" = "true" ]; then
        print_info "Development mode: Frontend will load data from /test-data-output"
    else
        print_info "Production mode: Frontend will load data from /data"
    fi
    
    echo ""
    print_info "Installing/updating npm dependencies..."
    if ! npm install; then
        print_error "Failed to install npm dependencies"
        exit 1
    fi
    
    npm run dev
}

# Function to build and serve production
run_production_build() {
    local is_dev_mode=$1
    
    echo ""
    
    if [ "$is_dev_mode" = "true" ]; then
        print_warning "You are building for production with development mode enabled!"
        echo ""
        echo "This means:"
        echo "  - Data is in public/test-data-output/ (will be included in build)"
        echo "  - Frontend expects data at /test-data-output"
        echo ""
        echo "For true production deployment, run without --dev flag to use public/data/"
        echo ""
        read -p "Continue anyway? (yes/no): " confirm
        if [[ ! "$confirm" =~ ^[Yy][Ee]?[Ss]?$ ]]; then
            print_info "Build cancelled"
            exit 0
        fi
        echo ""
    fi
    
    print_info "Building production bundle..."
    echo ""
    
    print_info "Installing/updating npm dependencies..."
    if ! npm install; then
        print_error "Failed to install npm dependencies"
        exit 1
    fi
    
    echo ""
    print_info "Running production build (this may take a minute)..."
    if ! npm run build; then
        print_error "Production build failed"
        exit 1
    fi
    
    npm run start
}

# Function to handle run options menu (reusable for all build options)
handle_run_options() {
    local is_dev_mode=$1
    
    while true; do
        show_run_options_menu "$is_dev_mode"
        read -p "Enter your choice (1-3): " run_choice
        
        case $run_choice in
            1)
                run_dev_server "$is_dev_mode"
                exit 0
                ;;
            2)
                run_production_build "$is_dev_mode"
                exit 0
                ;;
            3)
                echo ""
                print_info "Exiting. You can run the dashboard later with:"
                echo "  npm run dev    (development server)"
                echo "  npm run build && npm run start    (production)"
                exit 0
                ;;
            *)
                echo ""
                print_error "Invalid choice. Please enter 1, 2, or 3."
                echo ""
                ;;
        esac
    done
}

# Main script execution
main() {
    # Check prerequisites
    check_npm
    # check_python
    check_config

    # Show menu and get user input
    while true; do
        show_menu

        read -p "Enter your choice (0-7): " choice

        case $choice in
            0)
                build_and_serve_docs
                echo ""
                read -p "Press Enter to return to menu..."
                ;;

            1)
                echo ""
                print_info "Starting Dashboard Build Process (Full - with evaluations)..."
                echo ""

                # Run the Python workflow
                if python3 scripts/dashboard_builder_workflow.py --config config.yaml; then
                    print_success "Dashboard build completed successfully!"
                    handle_run_options "false"
                else
                    print_error "Dashboard build failed. Please check the errors above."
                    exit 1
                fi
                ;;

            2)
                echo ""
                print_info "Starting Dashboard Build Process (WITHOUT evaluations)..."
                print_warning "The Evaluations page will be DISABLED in the dashboard."
                echo ""

                # Run the Python workflow with --skip-evaluations flag
                if python3 scripts/dashboard_builder_workflow.py --config config.yaml --skip-evaluations; then
                    print_success "Dashboard build completed successfully!"
                    print_info "Note: Evaluation generation was skipped. The dashboard Evaluations page is disabled."
                    handle_run_options "false"
                else
                    print_error "Dashboard build failed. Please check the errors above."
                    exit 1
                fi
                ;;

            3)
                echo ""
                print_info "Starting Dashboard Build Process (Dev Mode - with evaluations)..."
                echo ""

                # Run the Python workflow with --dev flag
                if python3 scripts/dashboard_builder_workflow.py --config config.yaml --dev; then
                    print_success "Dashboard build completed successfully!"
                    handle_run_options "true"
                else
                    print_error "Dashboard build failed. Please check the errors above."
                    exit 1
                fi
                ;;

            4)
                echo ""
                print_info "Starting Dashboard Build Process (Dev Mode - WITHOUT evaluations)..."
                print_warning "The Evaluations page will be DISABLED in the dashboard."
                echo ""

                # Run the Python workflow with --dev and --skip-evaluations flags
                if python3 scripts/dashboard_builder_workflow.py --config config.yaml --dev --skip-evaluations; then
                    print_success "Dashboard build completed successfully!"
                    print_info "Note: Evaluation generation was skipped. The dashboard Evaluations page is disabled."
                    handle_run_options "true"
                else
                    print_error "Dashboard build failed. Please check the errors above."
                    exit 1
                fi
                ;;
            
            5)
                echo ""
                print_info "Starting Data Update Process (Production Mode)..."
                print_info "This will intelligently detect and process only changed data."
                echo ""

                # Run the Python workflow for data update (no --dev flag, with --update)
                if python3 scripts/dashboard_builder_workflow.py --config config.yaml --update; then
                    print_success "Data update completed successfully!"
                    print_info "Changes detected and processed incrementally."
                    print_info ""
                    print_info "Updated data is now in public/data/"
                    print_info "Restart your dashboard server to see the changes."
                    echo ""
                    read -p "Press Enter to return to menu..."
                else
                    print_error "Data update failed. Please check the errors above."
                    echo ""
                    read -p "Press Enter to return to menu..."
                fi
                ;;

            6)
                echo ""
                print_info "Starting Data Update Process (Dev Mode)..."
                print_info "This will intelligently detect and process only changed data."
                echo ""

                # Run the Python workflow with --dev and --update flags for data update
                if python3 scripts/dashboard_builder_workflow.py --config config.yaml --dev --update; then
                    print_success "Data update completed successfully!"
                    print_info "Changes detected and processed incrementally."
                    print_info ""
                    print_info "Updated data is now in public/test-data-output/"
                    print_info "Restart your dashboard server to see the changes."
                    echo ""
                    read -p "Press Enter to return to menu..."
                else
                    print_error "Data update failed. Please check the errors above."
                    echo ""
                    read -p "Press Enter to return to menu..."
                fi
                ;;

            7)
                echo ""
                print_info "Exiting..."
                exit 0
                ;;

            *)
                echo ""
                print_error "Invalid choice. Please enter a number from 0 to 7."
                echo ""
                read -p "Press Enter to continue..."
                ;;
        esac
    done
}

# Run main function
main