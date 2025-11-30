#!/usr/bin/env bash
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info() { echo -e "${BLUE}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

check_command() {
    command -v "$1" &> /dev/null
}

get_os() {
    case "$(uname -s)" in
        Darwin*) echo "macos" ;;
        Linux*)  echo "linux" ;;
        *)       echo "unknown" ;;
    esac
}

install_homebrew() {
    if ! check_command brew; then
        info "Installing Homebrew..."
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

        if [[ -f /opt/homebrew/bin/brew ]]; then
            eval "$(/opt/homebrew/bin/brew shellenv)"
        elif [[ -f /usr/local/bin/brew ]]; then
            eval "$(/usr/local/bin/brew shellenv)"
        fi
    fi
}

install_docker_macos() {
    if ! check_command docker; then
        info "Installing Docker Desktop for macOS..."
        brew install --cask docker
        warn "Please start Docker Desktop from Applications and wait for it to initialize"
        warn "Then re-run this script or run: vcf-pg-loader doctor"
        return 1
    fi
    return 0
}

install_docker_linux() {
    if ! check_command docker; then
        info "Installing Docker..."
        curl -fsSL https://get.docker.com | sh

        if check_command systemctl; then
            sudo systemctl start docker
            sudo systemctl enable docker
        fi

        if ! groups | grep -q docker; then
            sudo usermod -aG docker "$USER"
            warn "Added $USER to docker group. Please log out and back in, then re-run this script."
            return 1
        fi
    fi
    return 0
}

install_python_macos() {
    if ! check_command python3 || ! python3 -c "import sys; exit(0 if sys.version_info >= (3, 11) else 1)" 2>/dev/null; then
        info "Installing Python 3.11+..."
        brew install python@3.12
    fi
}

install_python_linux() {
    if ! check_command python3 || ! python3 -c "import sys; exit(0 if sys.version_info >= (3, 11) else 1)" 2>/dev/null; then
        info "Installing Python 3.11+..."
        if check_command apt-get; then
            sudo apt-get update
            sudo apt-get install -y python3 python3-pip python3-venv
        elif check_command dnf; then
            sudo dnf install -y python3 python3-pip
        elif check_command yum; then
            sudo yum install -y python3 python3-pip
        else
            error "Could not detect package manager. Please install Python 3.11+ manually."
        fi
    fi
}

install_pipx() {
    if ! check_command pipx; then
        info "Installing pipx..."
        python3 -m pip install --user pipx
        python3 -m pipx ensurepath

        export PATH="$HOME/.local/bin:$PATH"
    fi
}

install_vcf_pg_loader() {
    info "Installing vcf-pg-loader..."

    if check_command pipx; then
        pipx install vcf-pg-loader 2>/dev/null || pipx install . 2>/dev/null || pip install --user vcf-pg-loader 2>/dev/null || pip install --user .
    else
        pip install --user vcf-pg-loader 2>/dev/null || pip install --user .
    fi
}

main() {
    echo ""
    echo "=========================================="
    echo "  vcf-pg-loader Installation Script"
    echo "=========================================="
    echo ""

    OS=$(get_os)
    info "Detected OS: $OS"

    if [[ "$OS" == "unknown" ]]; then
        error "Unsupported operating system. Please install manually."
    fi

    NEED_RESTART=false

    if [[ "$OS" == "macos" ]]; then
        install_homebrew
        install_python_macos
        install_docker_macos || NEED_RESTART=true
    else
        install_python_linux
        install_docker_linux || NEED_RESTART=true
    fi

    success "Python $(python3 --version 2>&1 | cut -d' ' -f2) installed"

    if check_command docker; then
        if docker info &> /dev/null; then
            success "Docker installed and running"
        else
            warn "Docker installed but daemon not running"
            NEED_RESTART=true
        fi
    else
        warn "Docker not available yet"
        NEED_RESTART=true
    fi

    install_pipx
    install_vcf_pg_loader

    if check_command vcf-pg-loader; then
        success "vcf-pg-loader installed successfully!"
    else
        export PATH="$HOME/.local/bin:$PATH"
        if check_command vcf-pg-loader; then
            success "vcf-pg-loader installed successfully!"
            warn "Add to your shell profile: export PATH=\"\$HOME/.local/bin:\$PATH\""
        else
            warn "vcf-pg-loader installed but not in PATH. Try restarting your terminal."
        fi
    fi

    echo ""
    echo "=========================================="

    if [[ "$NEED_RESTART" == true ]]; then
        echo ""
        warn "Some steps require attention:"
        echo "  1. Start Docker Desktop (macOS) or Docker daemon (Linux)"
        echo "  2. Log out and back in if added to docker group"
        echo "  3. Run: vcf-pg-loader doctor"
        echo ""
    fi

    echo ""
    echo "Quick start:"
    echo "  vcf-pg-loader doctor           # Check dependencies"
    echo "  vcf-pg-loader db start         # Start local PostgreSQL"
    echo "  vcf-pg-loader load <file.vcf>  # Load VCF file"
    echo ""
    echo "For more info: vcf-pg-loader --help"
    echo ""
}

main "$@"
