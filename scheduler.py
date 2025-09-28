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


def minute_marker(eastern_dt: dt.datetime):
    """Return a tuple identifying the minute for a specific Eastern timestamp."""
    return (eastern_dt.date(), eastern_dt.strftime("%H:%M"))

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
        print("✓ Database and settings initialized")
    except Exception as e:
        print(f"Failed to initialize: {e}")
        return
    
    last_run_marker = None
    last_status_marker = None
    check_count = 0
    
    while True:
        try:
            now = dt.datetime.now(pytz.UTC)
            eastern = now.astimezone(pytz.timezone("America/New_York"))
            current_minute = eastern.strftime("%H:%M")
            current_marker = minute_marker(eastern)
            check_count += 1
            
            # Get current settings
            S = get_settings()
            enabled = S.get("ENABLED", True)
            windows = S.get("WINDOWS_ET", "11:50,14:35")
            
            # Only check once per minute to avoid duplicate runs
            if last_run_marker and last_run_marker[0] != current_marker[0]:
                print(f"[{eastern.strftime('%Y-%m-%d %H:%M:%S ET')}] 📅 New trading day detected – resetting duplicate-run guard")
                last_run_marker = None
            if last_status_marker and last_status_marker[0] != current_marker[0]:
                last_status_marker = None

            if current_marker != last_run_marker:
                in_window = should_run_now()

                if in_window and enabled:
                    print(f"[{eastern.strftime('%Y-%m-%d %H:%M:%S ET')}] 🚀 TRIGGERING TRADING RUN")
                    print(f"   Windows: {windows} | Enabled: {enabled}")
                    try:
                        run_trader(trigger="scheduled")
                        print(f"[{eastern.strftime('%Y-%m-%d %H:%M:%S ET')}] ✅ Trading run completed successfully")
                    except Exception as e:
                        error_msg = str(e)
                        print(f"[{eastern.strftime('%Y-%m-%d %H:%M:%S ET')}] ❌ Trading run failed: {error_msg}")
                        
                        # Check if it's a pattern day trading error
                        if "pattern day trading" in error_msg.lower() or "40310100" in error_msg:
                            print(f"   ⚠️  Pattern Day Trading Protection - trades blocked by broker")
                            insert_log("WARNING", "pattern_day_trading_blocked", {"error": error_msg, "message": "Trades blocked due to PDT protection"})
                        else:
                            insert_log("ERROR", "scheduled_run_failed", {"error": error_msg})
                        
                        # Continue running regardless of trading errors
                        print(f"   🔄 Scheduler will continue monitoring for next window")
                    
                    last_run_marker = current_marker

                # Show status every 10 minutes or when approaching trading windows
                elif current_marker != last_status_marker and (check_count % 20 == 0 or
                    eastern.hour in [11, 14] or current_minute.endswith('0')):
                    
                    # Calculate time to next window
                    next_window = None
                    for window_str in windows.split(','):
                        try:
                            hour, minute = map(int, window_str.strip().split(':'))
                            window_today = eastern.replace(hour=hour, minute=minute, second=0, microsecond=0)
                            if window_today > eastern:
                                next_window = window_today
                                break
                        except:
                            continue
                    
                    if next_window:
                        time_until = next_window - eastern
                        hours, remainder = divmod(int(time_until.total_seconds()), 3600)
                        minutes, _ = divmod(remainder, 60)
                        print(f"[{eastern.strftime('%Y-%m-%d %H:%M:%S ET')}] ⏰ Next trading window in {hours}h {minutes}m | Windows: {windows}")
                    else:
                        print(f"[{eastern.strftime('%Y-%m-%d %H:%M:%S ET')}] ⏰ Outside trading hours | Windows: {windows}")
                    
                    last_status_marker = current_marker
            
            # Sleep for 30 seconds before checking again
            time.sleep(30)
            
        except KeyboardInterrupt:
            print("Scheduler stopped by user")
            break
        except Exception as e:
            print(f"[{dt.datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S ET')}] ⚠️  Scheduler error: {e}")
            import traceback
            traceback.print_exc()
            
            # Log the scheduler error but keep running
            try:
                insert_log("ERROR", "scheduler_error", {"error": str(e), "message": "Scheduler encountered error but will continue"})
            except:
                pass  # Don't let logging errors crash the scheduler
                
            print("🔄 Scheduler recovering, will continue monitoring...")
            time.sleep(60)  # Wait longer on errors

if __name__ == "__main__":
    main()