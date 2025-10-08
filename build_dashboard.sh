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

# TODO: Ask if this is needed at all
# Function to check if Python is installed
# check_python() {
#     if ! command -v python3 &> /dev/null; then
#         print_error "Python 3 is not installed or not in PATH"
#         echo "Please install Python 3.8 or higher to continue."
#         exit 1
#     fi

#     # Check Python version
#     PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
#     print_info "Using Python $PYTHON_VERSION"
# }

# Function to check if config.yaml exists
check_config() {
    if [ ! -f "config.yaml" ]; then
        print_error "config.yaml not found in project root"
        echo ""
        echo "Please create a config.yaml file before proceeding."
        echo "You can copy config.yaml.example and customize it:"
        echo "  cp config.yaml.example config.yaml"
        echo ""
        exit 1
    fi
    print_success "Found config.yaml"
}

# Main menu function
show_menu() {
    print_header
    echo "Please select an option:"
    echo ""
    echo "  1) Build Dashboard (select this if building for the first time)"
    echo ""
    echo "  2) Check for New Data "
    echo ""
    echo "  3) Exit"
    echo ""
}

# Main script execution
main() {
    # Check prerequisites
    # check_python
    check_config

    # Show menu and get user input
    while true; do
        show_menu

        read -p "Enter your choice (1, 2, or 3): " choice

        case $choice in
            1)
                echo ""
                print_info "Starting Dashboard Build Process..."
                echo ""

                # Run the Python workflow
                if python3 scripts/dashboard_builder_workflow.py --config config.yaml; then
                    print_success "Dashboard build completed successfully!"
                    exit 0
                else
                    print_error "Dashboard build failed. Please check the errors above."
                    exit 1
                fi
                ;;

            2)
                echo ""
                print_info "Data Update Feature"
                echo ""
                print_warning "This feature is not yet implemented."
                echo ""
                read -p "Press Enter to return to menu..."
                ;;

            3)
                echo ""
                print_info "Exiting..."
                exit 0
                ;;

            *)
                echo ""
                print_error "Invalid choice. Please enter 1, 2, or 3."
                echo ""
                read -p "Press Enter to continue..."
                ;;
        esac
    done
}

# Run main function
main
