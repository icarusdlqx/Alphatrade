# AlphaTrade V3 - Replit Configuration

## Project Overview
AlphaTrade V3 is a Flask-based trading application that uses AI (OpenAI GPT) to make automated trading decisions via the Alpaca trading API. The application includes a web dashboard for monitoring and configuration.

## Project Architecture
- **Backend**: Flask web application (`webapp.py`)
- **Database**: PostgreSQL (configured via Replit's built-in database)
- **APIs**: 
  - Alpaca API for trading operations
  - OpenAI API for AI-powered analysis
- **Frontend**: HTML templates with Pico CSS framework
- **Trading Logic**: Automated trader (`trader.py`) that runs on schedule

## Recent Changes
- **2025-09-20**: Project imported and configured for Replit environment
  - Updated webapp.py to use port 5000 (required for Replit)
  - Updated Procfile to bind to port 5000
  - Installed all Python dependencies
  - Configured PostgreSQL database

## Configuration Requirements

### Required Environment Variables
The following secrets need to be set in Replit:
- `APP_PASSWORD`: Login password for the web interface
- `APP_SECRET_KEY`: Random string for Flask session security
- `OPENAI_API_KEY`: OpenAI API key for AI analysis
- `APCA_API_KEY_ID`: Alpaca API key ID
- `APCA_API_SECRET_KEY`: Alpaca API secret key
- `APCA_BASE_URL`: Set to `https://paper-api.alpaca.markets` for paper trading

### Database
- Uses Replit's built-in PostgreSQL database
- Connection configured via `DATABASE_URL` environment variable
- Database schema automatically initialized on first run

## Development Setup
1. Python 3.11 is installed with all required dependencies
2. Flask application runs on port 5000 (required for Replit)
3. Database tables are automatically created on startup
4. Web interface available at the preview URL

## Deployment Notes
- Uses Gunicorn for production deployment
- Configured for autoscale deployment target
- Procfile configured for proper port binding

## User Preferences
- None specified yet

## Key Files
- `webapp.py`: Main Flask application
- `trader.py`: Automated trading logic
- `memory.py`: Database operations and schema
- `settings_store.py`: Configuration management
- `templates/`: HTML templates for web interface
- `requirements.txt`: Python dependencies
- `Procfile`: Deployment configuration