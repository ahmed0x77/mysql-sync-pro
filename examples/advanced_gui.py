#!/usr/bin/env python3
"""
Advanced MySQL Sync Pro GUI Application
A comprehensive desktop application with modern UI/UX for database synchronization.
"""

import os
import sys
import threading
import time
import tkinter as tk
from typing import Optional, Dict, Any
from pathlib import Path

# Add the parent directory to the path to import our modules
sys.path.append(str(Path(__file__).parent.parent))

try:
    import customtkinter as ctk
    from customtkinter import CTkFrame, CTkLabel, CTkEntry, CTkButton, CTkSwitch, CTkTextbox, CTkTabview
    from customtkinter import CTkScrollableFrame, CTkProgressBar, CTkComboBox
except ImportError:
    print("Error: customtkinter is required. Install it with: pip install customtkinter")
    sys.exit(1)

from dotenv import load_dotenv
from src.sync import sync_mysql
from utils.change_detector import (
    has_database_changes,
    get_quick_signature,
    save_current_signature,
    get_state_file_for_database,
    load_last_signature
)

# Load environment variables
load_dotenv()

# Configure customtkinter appearance
ctk.set_appearance_mode("dark")  # Modes: "System" (standard), "Dark", "Light"
ctk.set_default_color_theme("blue")  # Themes: "blue" (standard), "green", "dark-blue"

class DatabaseSyncGUI:
    def __init__(self):
        self.root = ctk.CTk()
        self.root.title("MySQL Sync Pro - Advanced GUI")
        self.root.geometry("1000x700")
        self.root.minsize(800, 600)
        
        # Initialize variables
        self.sync_thread: Optional[threading.Thread] = None
        self.sync_running = False
        self.auto_sync_running = False
        self.bi_directional_running = False
        self.sync_lock = threading.Lock()
        
        # Load saved settings
        self.settings = self.load_settings()
        
        self.setup_ui()
        self.load_database_urls()
        
    def load_settings(self) -> Dict[str, Any]:
        """Load saved settings from file"""
        settings_file = Path(".state/gui_settings.json")
        if settings_file.exists():
            try:
                import json
                with open(settings_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {
            "dev_db_url": "",
            "prod_db_url": "",
            "auto_sync_interval": 5
        }
    
    def save_settings(self):
        """Save current settings to file"""
        settings_file = Path(".state/gui_settings.json")
        settings_file.parent.mkdir(exist_ok=True)
        
        self.settings.update({
            "dev_db_url": self.dev_url_entry.get(),
            "prod_db_url": self.prod_url_entry.get(),
            "auto_sync_interval": int(self.interval_entry.get())
        })
        
        try:
            import json
            with open(settings_file, 'w') as f:
                json.dump(self.settings, f, indent=2)
        except Exception as e:
            self.log_message(f"Error saving settings: {e}")
    
    def load_database_urls(self):
        """Load database URLs from environment variables"""
        dev_url = os.getenv("DEV_DB_URL", "")
        prod_url = os.getenv("PROD_DB_URL", "")
        
        if dev_url:
            self.dev_url_entry.delete(0, 'end')
            self.dev_url_entry.insert(0, dev_url)
        
        if prod_url:
            self.prod_url_entry.delete(0, 'end')
            self.prod_url_entry.insert(0, prod_url)
    
    def setup_ui(self):
        """Setup the main UI components with modern layout and scrolling"""
        # Main container with better spacing
        main_frame = CTkFrame(self.root)
        main_frame.pack(fill="both", expand=True, padx=15, pady=15)
        
        # Compact header design
        header_frame = CTkFrame(main_frame, fg_color="transparent")
        header_frame.pack(fill="x", pady=(5, 10))
        
        title_label = CTkLabel(
            header_frame, 
            text="MySQL Sync Pro", 
            font=ctk.CTkFont(size=20, weight="bold")
        )
        title_label.pack(side="left", padx=(10, 0))
        
        subtitle_label = CTkLabel(
            header_frame,
            text="Database Synchronization Tool",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        subtitle_label.pack(side="right", padx=(0, 10))
        
        # Create a two-column layout
        content_frame = CTkFrame(main_frame, fg_color="transparent")
        content_frame.pack(fill="both", expand=True, pady=(0, 10))
        
        # Left column - Configuration (with scrolling)
        left_scrollable = CTkScrollableFrame(content_frame)
        left_scrollable.pack(side="left", fill="both", expand=True, padx=(0, 10))
        
        # Configure scrolling speed by setting the scrollbar step
        left_scrollable._parent_canvas.configure(yscrollincrement=5)
        
        # Right column - Log (fixed, no scrolling)
        right_column = CTkFrame(content_frame, fg_color="transparent")
        right_column.pack(side="right", fill="both", expand=True, padx=(10, 0))
        
        # Setup left column components (inside scrollable frame)
        self.setup_database_config(left_scrollable)
        self.setup_sync_controls(left_scrollable)
        
        # Setup right column components (fixed)
        self.setup_log_section(right_column)
        
        # Status bar at bottom (fixed)
        self.setup_status_bar(main_frame)
    
    def setup_database_config(self, parent):
        """Setup database configuration section with modern design"""
        config_frame = CTkFrame(parent)
        config_frame.pack(fill="x", pady=(0, 10))
        
        # Section title with icon-like styling
        config_title = CTkLabel(
            config_frame,
            text="📊 Database Configuration",
            font=ctk.CTkFont(size=18, weight="bold")
        )
        config_title.pack(pady=(15, 20))
        
        # Database URLs in a cleaner layout
        url_frame = CTkFrame(config_frame, fg_color="transparent")
        url_frame.pack(fill="x", padx=20, pady=(0, 20))
        
        # Source Database
        source_label = CTkLabel(url_frame, text="Source Database:", font=ctk.CTkFont(size=14, weight="bold"))
        source_label.pack(anchor="w", pady=(0, 8))
        
        # Source database entry with visibility toggle
        source_entry_frame = CTkFrame(url_frame, fg_color="transparent")
        source_entry_frame.pack(fill="x", pady=(0, 15))
        
        self.dev_url_entry = CTkEntry(
            source_entry_frame,
            placeholder_text="mysql+pymysql://username:password@localhost:3306/database",
            height=40,
            font=ctk.CTkFont(size=13),
            show="*"  # Initially hidden
        )
        self.dev_url_entry.pack(side="left", fill="x", expand=True)
        
        self.dev_visibility_btn = CTkButton(
            source_entry_frame,
            text="👁",
            width=45,
            height=40,
            font=ctk.CTkFont(size=18),
            corner_radius=8,
            command=lambda: self.toggle_password_visibility(self.dev_url_entry, self.dev_visibility_btn)
        )
        self.dev_visibility_btn.pack(side="right", padx=(8, 0))
        
        # Target Database
        target_label = CTkLabel(url_frame, text="Target Database:", font=ctk.CTkFont(size=14, weight="bold"))
        target_label.pack(anchor="w", pady=(0, 8))
        
        # Target database entry with visibility toggle
        target_entry_frame = CTkFrame(url_frame, fg_color="transparent")
        target_entry_frame.pack(fill="x", pady=(0, 15))
        
        self.prod_url_entry = CTkEntry(
            target_entry_frame,
            placeholder_text="mysql+pymysql://username:password@production-host:3306/database",
            height=40,
            font=ctk.CTkFont(size=13),
            show="*"  # Initially hidden
        )
        self.prod_url_entry.pack(side="left", fill="x", expand=True)
        
        self.prod_visibility_btn = CTkButton(
            target_entry_frame,
            text="👁",
            width=45,
            height=40,
            font=ctk.CTkFont(size=18),
            corner_radius=8,
            command=lambda: self.toggle_password_visibility(self.prod_url_entry, self.prod_visibility_btn)
        )
        self.prod_visibility_btn.pack(side="right", padx=(8, 0))
        

    
    def setup_sync_controls(self, parent):
        """Setup sync controls with modern design"""
        controls_frame = CTkFrame(parent)
        controls_frame.pack(fill="x", pady=(0, 15))
        
        # Section title
        controls_title = CTkLabel(
            controls_frame,
            text="⚡ Sync Controls",
            font=ctk.CTkFont(size=18, weight="bold")
        )
        controls_title.pack(pady=(15, 20))
        
        # Sync mode selection in a cleaner layout
        mode_frame = CTkFrame(controls_frame, fg_color="transparent")
        mode_frame.pack(fill="x", padx=20, pady=(0, 20))
        
        mode_label = CTkLabel(mode_frame, text="Sync Mode:", font=ctk.CTkFont(size=14, weight="bold"))
        mode_label.pack(anchor="w", pady=(0, 10))
        
        self.sync_mode = ctk.StringVar(value="one_time")
        
        # Mode buttons in a horizontal layout
        modes_frame = CTkFrame(mode_frame, fg_color="transparent")
        modes_frame.pack(fill="x")
        
        one_time_btn = ctk.CTkRadioButton(
            modes_frame,
            text="One-Time",
            variable=self.sync_mode,
            value="one_time",
            command=self.on_sync_mode_change,
            font=ctk.CTkFont(size=13)
        )
        one_time_btn.pack(side="left", padx=(0, 15), pady=5)
        
        auto_sync_btn = ctk.CTkRadioButton(
            modes_frame,
            text="Auto Sync",
            variable=self.sync_mode,
            value="auto_sync",
            command=self.on_sync_mode_change,
            font=ctk.CTkFont(size=13)
        )
        auto_sync_btn.pack(side="left", padx=(0, 15), pady=5)
        
        bi_directional_btn = ctk.CTkRadioButton(
            modes_frame,
            text="Bi-Directional",
            variable=self.sync_mode,
            value="bi_directional",
            command=self.on_sync_mode_change,
            font=ctk.CTkFont(size=13)
        )
        bi_directional_btn.pack(side="left", pady=5)
        
        # Direction selection
        direction_frame = CTkFrame(controls_frame, fg_color="transparent")
        direction_frame.pack(fill="x", padx=20, pady=(0, 20))
        
        direction_label = CTkLabel(direction_frame, text="Sync Direction:", font=ctk.CTkFont(size=14, weight="bold"))
        direction_label.pack(anchor="w", pady=(0, 10))
        
        self.sync_direction = ctk.StringVar(value="source_to_target")
        
        direction_buttons = CTkFrame(direction_frame, fg_color="transparent")
        direction_buttons.pack(fill="x")
        
        source_to_target_btn = ctk.CTkRadioButton(
            direction_buttons,
            text="→ Source to Target",
            variable=self.sync_direction,
            value="source_to_target",
            font=ctk.CTkFont(size=13)
        )
        source_to_target_btn.pack(side="left", padx=(0, 15), pady=5)
        
        target_to_source_btn = ctk.CTkRadioButton(
            direction_buttons,
            text="← Target to Source",
            variable=self.sync_direction,
            value="target_to_source",
            font=ctk.CTkFont(size=13)
        )
        target_to_source_btn.pack(side="left", pady=5)
        
        # Auto sync interval (only show for auto sync and bi-directional)
        self.interval_frame = CTkFrame(controls_frame, fg_color="transparent")
        self.interval_frame.pack(fill="x", padx=20, pady=(0, 20))
        
        interval_label = CTkLabel(self.interval_frame, text="Auto Sync Interval (seconds):", font=ctk.CTkFont(size=14, weight="bold"))
        interval_label.pack(anchor="w", pady=(0, 8))
        
        self.interval_entry = CTkEntry(
            self.interval_frame, 
            placeholder_text="5", 
            width=150,
            height=35,
            font=ctk.CTkFont(size=13)
        )
        self.interval_entry.pack(anchor="w", pady=(0, 10))
        self.interval_entry.insert(0, str(self.settings.get("auto_sync_interval", 5)))
        
        # Initially hide interval frame since one-time is default
        self.interval_frame.pack_forget()
        

    

    
    def setup_log_section(self, parent):
        """Setup the log display section with modern design and scrolling"""
        log_frame = CTkFrame(parent)
        log_frame.pack(fill="both", expand=True)
        
        # Log header with modern styling (fixed)
        log_header = CTkFrame(log_frame, fg_color="transparent")
        log_header.pack(fill="x", padx=15, pady=(15, 10))
        
        log_title = CTkLabel(
            log_header,
            text="📋 Sync Log",
            font=ctk.CTkFont(size=18, weight="bold")
        )
        log_title.pack(side="left")
        
        clear_log_btn = CTkButton(
            log_header,
            text="🗑️ Clear",
            command=self.clear_log,
            width=80,
            height=30,
            font=ctk.CTkFont(size=12)
        )
        clear_log_btn.pack(side="right")
        
        # Log text area with scrolling
        self.log_text = CTkTextbox(
            log_frame, 
            font=ctk.CTkFont(size=12, family="Consolas"),
            wrap="word"  # Enable word wrapping
        )
        self.log_text.pack(fill="both", expand=True, padx=15, pady=(0, 10))
        
        # Status label under the log
        self.status_label = CTkLabel(
            log_frame,
            text="🟢 Ready",
            font=ctk.CTkFont(size=12, weight="bold")
        )
        self.status_label.pack(anchor="w", padx=15, pady=(0, 15))
    
    def setup_status_bar(self, parent):
        """Setup the status bar with modern design and sync button"""
        status_frame = CTkFrame(parent)
        status_frame.pack(fill="x", pady=(10, 0))
        
        # Sync button frame to hold button and spinner
        sync_frame = CTkFrame(status_frame, fg_color="transparent")
        sync_frame.pack(fill="x", padx=15, pady=8)
        
        # Main sync button (full width)
        self.sync_button = CTkButton(
            sync_frame,
            text="🚀 Start Sync",
            command=self.start_sync,
            height=40,
            font=ctk.CTkFont(size=16, weight="bold")
        )
        self.sync_button.pack(side="left", fill="x", expand=True)
        
        # Spinner label (hidden by default)
        self.spinner_label = CTkLabel(
            sync_frame,
            text="⏳",
            font=ctk.CTkFont(size=20),
            text_color="orange"
        )
        self.spinner_label.pack(side="right", padx=(10, 0))
        self.spinner_label.pack_forget()  # Initially hidden
        

    
    def log_message(self, message: str, level: str = "INFO"):
        """Add a message to the log"""
        timestamp = time.strftime("%H:%M:%S")
        color_map = {
            "INFO": "white",
            "ERROR": "red",
            "SUCCESS": "green",
            "WARNING": "orange"
        }
        
        color = color_map.get(level, "white")
        log_entry = f"[{timestamp}] {level}: {message}\n"
        
        self.log_text.insert("end", log_entry)
        self.log_text.see("end")
        
        # Update status
        self.status_label.configure(text=f"Last: {message[:50]}...")
    
    def clear_log(self):
        """Clear the log display"""
        self.log_text.delete("1.0", "end")
    
    def on_sync_mode_change(self):
        """Handle sync mode change to show/hide interval field"""
        sync_mode = self.sync_mode.get()
        if sync_mode in ["auto_sync", "bi_directional"]:
            self.interval_frame.pack(fill="x", padx=20, pady=(0, 20))
        else:
            self.interval_frame.pack_forget()
    
    def animate_spinner(self):
        """Animate the spinner while sync is running"""
        if self.sync_running or self.auto_sync_running or self.bi_directional_running:
            spinner_chars = ["⏳", "⌛", "⏳", "⌛"]
            current_char = self.spinner_label.cget("text")
            next_char = spinner_chars[(spinner_chars.index(current_char) + 1) % len(spinner_chars)]
            self.spinner_label.configure(text=next_char)
            self.root.after(500, self.animate_spinner)  # Animate every 500ms
    
    def toggle_password_visibility(self, entry, button):
        """Toggle password visibility for database URL entries"""
        if entry.cget("show") == "*":
            entry.configure(show="")
            button.configure(text="🙈")
        else:
            entry.configure(show="*")
            button.configure(text="👁")
    
    def test_connections(self):
        """Test database connections"""
        self.log_message("Testing database connections...")
        
        dev_url = self.dev_url_entry.get()
        prod_url = self.prod_url_entry.get()
        
        if not dev_url or not prod_url:
            self.log_message("Please enter both database URLs", "ERROR")
            return
        
        def test_connection():
            try:
                from sqlalchemy import create_engine
                
                # Test source connection
                dev_engine = create_engine(dev_url, pool_pre_ping=True)
                with dev_engine.connect() as conn:
                    conn.execute("SELECT 1")
                self.log_message("✓ Source database connection successful", "SUCCESS")
                
                # Test target connection
                prod_engine = create_engine(prod_url, pool_pre_ping=True)
                with prod_engine.connect() as conn:
                    conn.execute("SELECT 1")
                self.log_message("✓ Target database connection successful", "SUCCESS")
                
                self.connection_status.configure(text="🟢 Connected", text_color="green")
                self.log_message("All connections tested successfully", "SUCCESS")
                
            except Exception as e:
                self.log_message(f"Connection test failed: {e}", "ERROR")
                self.connection_status.configure(text="🔴 Disconnected", text_color="red")
        
        threading.Thread(target=test_connection, daemon=True).start()
    

    
    def start_sync(self):
        """Start the selected sync operation"""
        if self.sync_running or self.auto_sync_running or self.bi_directional_running:
            self.stop_sync()
            return
        
        dev_url = self.dev_url_entry.get()
        prod_url = self.prod_url_entry.get()
        
        if not dev_url or not prod_url:
            self.log_message("Please enter both database URLs", "ERROR")
            return
        
        sync_mode = self.sync_mode.get()
        
        if sync_mode == "one_time":
            self.start_one_time_sync(dev_url, prod_url)
        elif sync_mode == "auto_sync":
            self.start_auto_sync(dev_url, prod_url)
        elif sync_mode == "bi_directional":
            self.start_bi_directional_sync(dev_url, prod_url)
    
    def start_one_time_sync(self, dev_url: str, prod_url: str):
        """Start a one-time sync - using exact same logic as simple_runner.py"""
        self.sync_running = True
        self.sync_button.configure(text="Stop Sync")
        self.spinner_label.pack(side="right", padx=(10, 0))
        self.animate_spinner()
        
        def sync_worker():
            try:
                # Determine sync direction based on radio button selection
                direction = self.sync_direction.get()
                if direction == "source_to_target":
                    local_url = dev_url
                    prod_url_sync = prod_url
                    self.log_message("Starting one-time synchronization (Source → Target)...")
                else:  # target_to_source
                    local_url = prod_url
                    prod_url_sync = dev_url
                    self.log_message("Starting one-time synchronization (Target → Source)...")
                
                # Use exact same parameters as simple_runner.py
                sync_mysql(
                    local_url=local_url,
                    prod_url=prod_url_sync,
                    allow_mysql_port_5432=True,
                    verbosity=1,
                    change_detector=False, # force sync
                )
                
                self.log_message("One-time sync completed successfully", "SUCCESS")
                
            except Exception as e:
                self.log_message(f"Sync failed: {e}", "ERROR")
            finally:
                self.sync_running = False
                self.sync_button.configure(text="🚀 Start Sync")
                self.spinner_label.pack_forget()
        
        self.sync_thread = threading.Thread(target=sync_worker, daemon=True)
        self.sync_thread.start()
    
    def start_auto_sync(self, dev_url: str, prod_url: str):
        """Start continuous auto sync - using exact same logic as auto_sync.py"""
        self.auto_sync_running = True
        self.sync_button.configure(text="Stop Auto Sync")
        self.spinner_label.pack(side="right", padx=(10, 0))
        self.animate_spinner()
        
        def auto_sync_worker():
            # Use the same interval logic as auto_sync.py
            interval = int(os.getenv("AUTO_THINK_INTERVAL_SECS", str(self.interval_entry.get())))
            
            # Determine sync direction based on radio button selection
            direction = self.sync_direction.get()
            if direction == "source_to_target":
                local_url = dev_url
                prod_url_sync = prod_url
                self.log_message(f"Starting auto sync (Source → Target) with {interval}s interval...")
            else:  # target_to_source
                local_url = prod_url
                prod_url_sync = dev_url
                self.log_message(f"Starting auto sync (Target → Source) with {interval}s interval...")
            
            while self.auto_sync_running:
                try:
                    # Use exact same parameters as auto_sync.py
                    sync_mysql(
                        local_url=local_url,
                        prod_url=prod_url_sync,
                        allow_mysql_port_5432=True,
                        verbosity=1,
                        # change_detector=False,  # Commented out in auto_sync.py
                    )
                    
                    self.log_message("Auto sync completed", "SUCCESS")
                    
                except Exception as e:
                    self.log_message(f"Auto sync error: {e}", "ERROR")
                
                time.sleep(interval)
            
            self.log_message("Auto sync stopped")
        
        self.sync_thread = threading.Thread(target=auto_sync_worker, daemon=True)
        self.sync_thread.start()
    
    def start_bi_directional_sync(self, dev_url: str, prod_url: str):
        """Start bi-directional sync - using exact same logic as full_real_time_sync.py"""
        self.bi_directional_running = True
        self.sync_button.configure(text="Stop Bi-Directional Sync")
        self.spinner_label.pack(side="right", padx=(10, 0))
        self.animate_spinner()
        
        def bi_directional_worker():
            # Use the same interval logic as full_real_time_sync.py
            interval = int(os.getenv("AUTO_THINK_INTERVAL_SECS", str(self.interval_entry.get())))
            self.log_message(f"Starting bi-directional sync with {interval}s interval...")
            
            while self.bi_directional_running:
                try:
                    source_state_file = get_state_file_for_database(dev_url)
                    target_state_file = get_state_file_for_database(prod_url)

                    # Add diagnostic printing like in full_real_time_sync.py
                    current_sig = get_quick_signature(dev_url, signature_type="content")
                    last_sig = load_last_signature(source_state_file)
                    self.log_message(f"Checking... | Current Sig: {current_sig} | Last Sig: {last_sig}")

                    # Use "content" signature for reliable change detection
                    if has_database_changes(dev_url, signature_type="content"):
                        with self.sync_lock:
                            self.log_message("Changes detected in source. Syncing to target...")
                            
                            # Use exact same parameters as full_real_time_sync.py
                            sync_mysql(
                                local_url=dev_url,
                                prod_url=prod_url,
                                allow_mysql_port_5432=True,
                                verbosity=0,
                                change_detector=False, # Use our own logic
                            )
                            
                            # After sync, update both signatures to prevent feedback loops
                            new_sig = get_quick_signature(prod_url, signature_type="content")
                            save_current_signature(new_sig, source_state_file)
                            save_current_signature(new_sig, target_state_file)
                            self.log_message("Sync complete. Updated signatures.")
                    
                    # Now check the other direction (prod to dev)
                    source_state_file = get_state_file_for_database(prod_url)
                    target_state_file = get_state_file_for_database(dev_url)

                    current_sig = get_quick_signature(prod_url, signature_type="content")
                    last_sig = load_last_signature(source_state_file)
                    self.log_message(f"Checking reverse... | Current Sig: {current_sig} | Last Sig: {last_sig}")

                    if has_database_changes(prod_url, signature_type="content"):
                        with self.sync_lock:
                            self.log_message("Changes detected in target. Syncing to source...")
                            
                            sync_mysql(
                                local_url=prod_url,
                                prod_url=dev_url,
                                allow_mysql_port_5432=True,
                                verbosity=0,
                                change_detector=False, # Use our own logic
                            )
                            
                            # After sync, update both signatures to prevent feedback loops
                            new_sig = get_quick_signature(dev_url, signature_type="content")
                            save_current_signature(new_sig, source_state_file)
                            save_current_signature(new_sig, target_state_file)
                            self.log_message("Reverse sync complete. Updated signatures.")
                    
                    time.sleep(interval)
                    
                except Exception as e:
                    self.log_message(f"Bi-directional sync error: {e}", "ERROR")
                    time.sleep(interval)
            
            self.log_message("Bi-directional sync stopped")
        
        self.sync_thread = threading.Thread(target=bi_directional_worker, daemon=True)
        self.sync_thread.start()
    
    def stop_sync(self):
        """Stop all running sync operations"""
        self.sync_running = False
        self.auto_sync_running = False
        self.bi_directional_running = False
        
        self.sync_button.configure(text="🚀 Start Sync")
        self.spinner_label.pack_forget()
        
        self.log_message("Stopping sync operations...")
    
    def on_closing(self):
        """Handle application closing"""
        self.stop_sync()
        self.save_settings()
        self.root.destroy()
    
    def run(self):
        """Start the GUI application"""
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.root.mainloop()


def main():
    """Main entry point"""
    try:
        app = DatabaseSyncGUI()
        app.run()
    except Exception as e:
        print(f"Error starting GUI: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
