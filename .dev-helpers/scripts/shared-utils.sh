#!/bin/bash

# Shared utilities for pipeline dev scripts
# Provides common functions for service management, IP detection, and status reporting

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Print status messages with colors
print_status() {
    local level="$1"
    local message="$2"

    case "$level" in
        "header")
            echo -e "${BLUE}================================================================================${NC}"
            echo -e "${BLUE}🚀 $message${NC}"
            echo -e "${BLUE}================================================================================${NC}"
            ;;
        "success")
            echo -e "${GREEN}✅ $message${NC}"
            ;;
        "info")
            echo -e "${CYAN}ℹ️  $message${NC}"
            ;;
        "warning")
            echo -e "${YELLOW}⚠️  $message${NC}"
            ;;
        "error")
            echo -e "${RED}❌ $message${NC}"
            ;;
        *)
            echo "$message"
            ;;
    esac
}

# Get the project root directory
get_project_root() {
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

    # Go up until we find a directory with typical project markers
    local current_dir="$script_dir"
    while [[ "$current_dir" != "/" ]]; do
        if [[ -f "$current_dir/build.gradle" || -f "$current_dir/pom.xml" || -f "$current_dir/package.json" ]]; then
            echo "$current_dir"
            return 0
        fi
        current_dir="$(dirname "$current_dir")"
    done

    # Fallback to current directory
    echo "$(pwd)"
}

# Detect Docker bridge IP address for service registration
detect_docker_bridge_ip() {
    # Try multiple methods to detect the Docker bridge IP

    # Method 1: Check if we're running inside Docker
    if [[ -f /.dockerenv ]]; then
        # Inside Docker, use host.docker.internal for macOS/Windows, or try to detect
        if command -v getent >/dev/null 2>&1; then
            # Linux: try to get Docker bridge IP
            local bridge_ip
            bridge_ip=$(ip route show | grep -oP 'default via \K[\d.]+' 2>/dev/null || echo "")
            if [[ -n "$bridge_ip" ]]; then
                echo "$bridge_ip"
                return 0
            fi
        fi
        # Fallback for Docker Desktop
        echo "host.docker.internal"
        return 0
    fi

    # Method 2: Try to detect Docker bridge network
    if command -v docker >/dev/null 2>&1; then
        local bridge_ip
        bridge_ip=$(docker network inspect bridge --format '{{range .IPAM.Config}}{{.Gateway}}{{end}}' 2>/dev/null | head -1)
        if [[ -n "$bridge_ip" && "$bridge_ip" != "null" ]]; then
            echo "$bridge_ip"
            return 0
        fi
    fi

    # Method 3: Try common Docker bridge IPs
    for ip in "172.17.0.1" "192.168.65.1" "host.docker.internal"; do
        if ping -c 1 -W 1 "$ip" >/dev/null 2>&1; then
            echo "$ip"
            return 0
        fi
    done

    # Fallback to localhost
    print_status "warning" "Could not detect Docker bridge IP, using localhost"
    echo "localhost"
}

# Set registration host environment variable
set_registration_host() {
    local service_name="$1"
    local env_var="$2"

    print_status "info" "Detecting registration host for $service_name..."

    local detected_ip
    detected_ip=$(detect_docker_bridge_ip)

    # Export the environment variable
    export "$env_var"="$detected_ip"

    print_status "success" "Registration host set: $env_var=$detected_ip"
}

# Check if a port is in use
check_port() {
    local port="$1"
    local service_name="$2"

    if lsof -Pi :"$port" -sTCP:LISTEN -t >/dev/null 2>&1; then
        return 0  # Port is in use
    else
        return 1  # Port is free
    fi
}

# Kill process running on a specific port
kill_process_on_port() {
    local port="$1"
    local service_name="$2"

    print_status "info" "Finding process on port $port..."

    local pid
    pid=$(lsof -ti :"$port" 2>/dev/null)

    if [[ -n "$pid" ]]; then
        print_status "warning" "Killing process $pid on port $port ($service_name)"
        kill "$pid" 2>/dev/null || true
        sleep 2

        # Check if it's still running
        if kill -0 "$pid" 2>/dev/null; then
            print_status "warning" "Process still running, force killing..."
            kill -9 "$pid" 2>/dev/null || true
        fi

        print_status "success" "Process killed"
    else
        print_status "info" "No process found on port $port"
    fi
}

# Check if required tools are available
check_dependencies() {
    local missing_tools=()

    for tool in "$@"; do
        if ! command -v "$tool" >/dev/null 2>&1; then
            missing_tools+=("$tool")
        fi
    done

    if [[ ${#missing_tools[@]} -gt 0 ]]; then
        print_status "error" "Missing required tools: ${missing_tools[*]}"
        print_status "info" "Please install them and try again"
        exit 1
    fi
}

# Validate that we're in the correct directory
validate_project_structure() {
    local expected_files=("$@")

    for file in "${expected_files[@]}"; do
        if [[ ! -f "$file" ]]; then
            print_status "error" "Required file not found: $file"
            print_status "info" "Are you in the correct project directory?"
            exit 1
        fi
    done
}