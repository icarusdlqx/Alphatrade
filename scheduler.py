#!/usr/bin/env python3
"""
AlphaTrade Scheduler - Runs automated trading checks at configured times
"""
import time
import datetime as dt
import pytz
from settings_store import get_settings, init_settings_table
from trader import main as run_trader, within_time_window_et
from memory import init_db, insert_log

def should_run_now() -> bool:
    """Check if we should run a trading check now based on configured windows"""
    try:
        S = get_settings()
        if not S.get("ENABLED", True):
            return False
            
        now_utc = dt.datetime.now(pytz.UTC)
        return within_time_window_et(now_utc, S["WINDOWS_ET"], int(S.get("WINDOW_TOL_MIN", 30)))
    except Exception as e:
        print(f"Error checking schedule: {e}")
        return False

def main():
    """Main scheduler loop - checks every minute if it's time to trade"""
    print("AlphaTrade Scheduler starting...")
    
    # Initialize database and settings
    try:
        init_db()
        init_settings_table()
    except Exception as e:
        print(f"Failed to initialize: {e}")
        return
    
    last_run_minute = None
    
    while True:
        try:
            now = dt.datetime.now(pytz.UTC)
            eastern = now.astimezone(pytz.timezone("America/New_York"))
            current_minute = eastern.strftime("%H:%M")
            
            # Only check once per minute to avoid duplicate runs
            if current_minute != last_run_minute and should_run_now():
                print(f"[{eastern.strftime('%Y-%m-%d %H:%M:%S ET')}] Triggering scheduled trading run...")
                try:
                    run_trader(trigger="scheduled")
                    print(f"[{eastern.strftime('%Y-%m-%d %H:%M:%S ET')}] Trading run completed")
                except Exception as e:
                    print(f"[{eastern.strftime('%Y-%m-%d %H:%M:%S ET')}] Trading run failed: {e}")
                    insert_log("ERROR", "scheduled_run_failed", {"error": str(e)})
                
                last_run_minute = current_minute
            
            # Sleep for 30 seconds before checking again
            time.sleep(30)
            
        except KeyboardInterrupt:
            print("Scheduler stopped by user")
            break
        except Exception as e:
            print(f"Scheduler error: {e}")
            time.sleep(60)  # Wait longer on errors

if __name__ == "__main__":
    main()