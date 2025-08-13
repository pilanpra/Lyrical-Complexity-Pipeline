#!/usr/bin/env python3
"""
Setup Script for Lyrical Complexity Pipeline
Automates initial project setup and configuration.
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

def print_banner():
    """Print project banner."""
    print("🎵" + "="*58 + "🎵")
    print("🎵           LYRICAL COMPLEXITY PIPELINE SETUP           🎵")
    print("🎵" + "="*58 + "🎵")
    print()

def check_python_version():
    """Check if Python version is compatible."""
    print("🐍 Checking Python version...")
    
    if sys.version_info < (3, 8):
        print("❌ Python 3.8+ is required")
        print(f"   Current version: {sys.version}")
        return False
    
    print(f"✅ Python {sys.version_info.major}.{sys.version_info.minor} detected")
    return True

def create_directories():
    """Create necessary project directories."""
    print("📁 Creating project directories...")
    
    directories = [
        "data",
        "logs",
        "temp"
    ]
    
    for directory in directories:
        dir_path = Path(directory)
        if not dir_path.exists():
            dir_path.mkdir(parents=True, exist_ok=True)
            print(f"   Created: {directory}/")
        else:
            print(f"   Exists: {directory}/")
    
    return True

def setup_virtual_environment():
    """Set up Python virtual environment."""
    print("🔧 Setting up virtual environment...")
    
    venv_path = Path("venv")
    
    if venv_path.exists():
        print("   Virtual environment already exists")
        return True
    
    try:
        subprocess.run([sys.executable, "-m", "venv", "venv"], check=True)
        print("   Virtual environment created successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Failed to create virtual environment: {e}")
        return False

def install_dependencies():
    """Install Python dependencies."""
    print("📦 Installing dependencies...")
    
    requirements_file = Path("requirements/requirements.txt")
    
    if not requirements_file.exists():
        print("   ❌ Requirements file not found")
        return False
    
    # Determine the correct pip command
    if os.name == 'nt':  # Windows
        pip_cmd = "venv\\Scripts\\pip"
    else:  # Unix/Linux/macOS
        pip_cmd = "venv/bin/pip"
    
    try:
        subprocess.run([pip_cmd, "install", "-r", str(requirements_file)], check=True)
        print("   ✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Failed to install dependencies: {e}")
        return False

def setup_environment_file():
    """Set up environment configuration file."""
    print("⚙️  Setting up environment configuration...")
    
    env_example = Path("config/env.example")
    env_file = Path("config/.env")
    
    if not env_example.exists():
        print("   ❌ Environment example file not found")
        return False
    
    if env_file.exists():
        print("   Environment file already exists")
        return True
    
    try:
        shutil.copy(env_example, env_file)
        print("   ✅ Environment file created: config/.env")
        print("   ⚠️  Please edit config/.env with your actual credentials")
        return True
    except Exception as e:
        print(f"   ❌ Failed to create environment file: {e}")
        return False

def check_docker():
    """Check if Docker is available."""
    print("🐳 Checking Docker availability...")
    
    try:
        result = subprocess.run(["docker", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"   ✅ Docker available: {result.stdout.strip()}")
            return True
        else:
            print("   ❌ Docker not available")
            return False
    except FileNotFoundError:
        print("   ❌ Docker not installed")
        return False

def run_tests():
    """Run pipeline validation tests."""
    print("🧪 Running pipeline validation tests...")
    
    test_script = Path("scripts/test_pipeline.py")
    
    if not test_script.exists():
        print("   ❌ Test script not found")
        return False
    
    # Determine the correct python command
    if os.name == 'nt':  # Windows
        python_cmd = "venv\\Scripts\\python"
    else:  # Unix/Linux/macOS
        python_cmd = "venv/bin/python"
    
    try:
        result = subprocess.run([python_cmd, str(test_script)], check=True)
        print("   ✅ Tests completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Tests failed: {e}")
        return False

def print_next_steps():
    """Print next steps for the user."""
    print("\n" + "="*60)
    print("🚀 SETUP COMPLETE! NEXT STEPS:")
    print("="*60)
    
    print("\n1. 🔑 Configure your credentials:")
    print("   Edit config/.env with your Genius API token")
    print("   Get token from: https://genius.com/api-clients")
    
    print("\n2. 🐳 Start with Docker (Recommended):")
    print("   docker-compose up -d")
    print("   Access Airflow at: http://localhost:8080")
    
    print("\n3. 🐍 Or run manually:")
    print("   source venv/bin/activate  # On Windows: venv\\Scripts\\activate")
    print("   python scripts/extract_lyrics.py")
    print("   python scripts/transform_lyrics.py")
    print("   python scripts/load_to_postgres.py")
    
    print("\n4. 📊 View results:")
    print("   Check the data/ directory for output files")
    print("   Use the sample SQL queries in README.md")
    
    print("\n5. 🔄 Schedule with Airflow:")
    print("   The pipeline is configured to run weekly")
    print("   Monitor execution in the Airflow UI")
    
    print("\n📚 For more information, see README.md")
    print("="*60)

def main():
    """Main setup function."""
    print_banner()
    
    # Check prerequisites
    if not check_python_version():
        sys.exit(1)
    
    # Setup steps
    steps = [
        ("Creating directories", create_directories),
        ("Setting up virtual environment", setup_virtual_environment),
        ("Installing dependencies", install_dependencies),
        ("Setting up environment file", setup_environment_file),
        ("Checking Docker", check_docker),
    ]
    
    print("🔄 Running setup steps...\n")
    
    for step_name, step_func in steps:
        print(f"Step: {step_name}")
        if not step_func():
            print(f"❌ Setup failed at: {step_name}")
            sys.exit(1)
        print()
    
    # Run tests if requested
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        if not run_tests():
            print("⚠️  Tests failed, but setup completed")
    
    print_next_steps()

if __name__ == "__main__":
    main()
