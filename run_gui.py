#!/usr/bin/env python3
"""
MySQL Sync Pro GUI Launcher
Simple launcher script for the advanced GUI application.
"""

import sys
import os
from pathlib import Path

# Add the current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

def main():
    """Launch the GUI application"""
    try:
        # Check if customtkinter is installed
        try:
            import customtkinter
        except ImportError:
            print("Error: customtkinter is not installed.")
            print("Please install it with: pip install customtkinter")
            print("Or install all requirements with: pip install -r requirements.txt")
            sys.exit(1)
        
        # Import and run the GUI
        from examples.advanced_gui import main as gui_main
        gui_main()
        
    except Exception as e:
        print(f"Error launching GUI: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
