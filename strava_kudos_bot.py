import os
import json
import time
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Set
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class StravaKudosBot:
    def __init__(self):
        self.setup_logging()
        self.setup_database()
        self.load_config()
        self.setup_requests_session()
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler()  # Only console for Render
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def setup_database(self):
        """Initialize SQLite database for tracking"""
        self.conn = sqlite3.connect('strava_bot.db', check_same_thread=False)
        self.cursor = self.conn.cursor()
        
        # Create tables
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS kudos_given (
                user_id INTEGER,
                activity_id INTEGER,
                timestamp TEXT,
                PRIMARY KEY (user_id, activity_id)
            )
        ''')
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_activities (
                activity_id INTEGER PRIMARY KEY,
                timestamp TEXT
            )
        ''')
        
        self.conn.commit()
        
    def load_config(self):
        """Load configuration from environment variables"""
        self.client_id = os.getenv('STRAVA_CLIENT_ID')
        self.client_secret = os.getenv('STRAVA_CLIENT_SECRET')
        self.access_token = os.getenv('STRAVA_ACCESS_TOKEN')
        self.refresh_token = os.getenv('STRAVA_REFRESH_TOKEN')
        
        if not all([self.client_id, self.client_secret, self.access_token, self.refresh_token]):
            raise ValueError("Missing required Strava API credentials")
            
        self.logger.info("Configuration loaded successfully")
        
    def setup_requests_session(self):
        """Setup requests session with retry strategy"""
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
    def refresh_access_token(self):
        """Refresh the access token using refresh token"""
        url = "https://www.strava.com/oauth/token"
        data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'refresh_token': self.refresh_token,
            'grant_type': 'refresh_token'
        }
        
        try:
            response = self.session.post(url, data=data)
            response.raise_for_status()
            
            tokens = response.json()
            self.access_token = tokens['access_token']
            self.refresh_token = tokens['refresh_token']
            
            # Update environment variables (for current session)
            os.environ['STRAVA_ACCESS_TOKEN'] = self.access_token
            os.environ['STRAVA_REFRESH_TOKEN'] = self.refresh_token
            
            self.logger.info("Access token refreshed successfully")
            return True
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to refresh access token: {e}")
            return False
            
    def make_api_request(self, url: str, method: str = 'GET', data: Dict = None) -> Dict:
        """Make authenticated API request with rate limiting"""
        headers = {'Authorization': f'Bearer {self.access_token}'}
        
        try:
            if method == 'GET':
                response = self.session.get(url, headers=headers)
            elif method == 'POST':
                response = self.session.post(url, headers=headers, json=data)
            else:
                response = self.session.request(method, url, headers=headers, json=data)
                
            # Handle rate limiting
            if response.status_code == 429:
                self.logger.warning("Rate limit exceeded, sleeping...")
                time.sleep(60)  # Wait 1 minute
                return self.make_api_request(url, method, data)
                
            # Handle token expiration
            elif response.status_code == 401:
                self.logger.info("Token expired, refreshing...")
                if self.refresh_access_token():
                    return self.make_api_request(url, method, data)
                else:
                    raise Exception("Failed to refresh token")
                    
            response.raise_for_status()
            return response.json() if response.content else {}
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {e}")
            raise
            
    def get_athlete_activities(self, limit: int = 10) -> List[Dict]:
        """Get recent activities for the authenticated athlete"""
        url = f"https://www.strava.com/api/v3/athlete/activities?per_page={limit}"
        return self.make_api_request(url)
        
    def get_activity_kudos(self, activity_id: int) -> List[Dict]:
        """Get kudos for a specific activity"""
        url = f"https://www.strava.com/api/v3/activities/{activity_id}/kudos"
        return self.make_api_request(url)
        
    def get_athlete_activities_by_id(self, athlete_id: int, limit: int = 2) -> List[Dict]:
        """Get recent activities for a specific athlete"""
        url = f"https://www.strava.com/api/v3/athletes/{athlete_id}/activities?per_page={limit}"
        try:
            return self.make_api_request(url)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                self.logger.warning(f"Cannot access activities for athlete {athlete_id} (private profile)")
                return []
            raise
            
    def give_kudos(self, activity_id: int) -> bool:
        """Give kudos to an activity"""
        url = f"https://www.strava.com/api/v3/activities/{activity_id}/kudos"
        try:
            self.make_api_request(url, method='POST')
            return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 422:
                self.logger.info(f"Kudos already given to activity {activity_id}")
                return False
            self.logger.error(f"Failed to give kudos to activity {activity_id}: {e}")
            return False
            
    def has_given_kudos(self, user_id: int, activity_id: int) -> bool:
        """Check if we've already given kudos to this user's activity"""
        self.cursor.execute(
            "SELECT 1 FROM kudos_given WHERE user_id = ? AND activity_id = ?",
            (user_id, activity_id)
        )
        return self.cursor.fetchone() is not None
        
    def record_kudos_given(self, user_id: int, activity_id: int):
        """Record that we've given kudos to prevent duplicates"""
        timestamp = datetime.now().isoformat()
        self.cursor.execute(
            "INSERT OR REPLACE INTO kudos_given (user_id, activity_id, timestamp) VALUES (?, ?, ?)",
            (user_id, activity_id, timestamp)
        )
        self.conn.commit()
        
    def is_activity_processed(self, activity_id: int) -> bool:
        """Check if we've already processed this activity for kudos"""
        self.cursor.execute(
            "SELECT 1 FROM processed_activities WHERE activity_id = ?",
            (activity_id,)
        )
        return self.cursor.fetchone() is not None
        
    def mark_activity_processed(self, activity_id: int):
        """Mark activity as processed"""
        timestamp = datetime.now().isoformat()
        self.cursor.execute(
            "INSERT OR REPLACE INTO processed_activities (activity_id, timestamp) VALUES (?, ?)",
            (activity_id, timestamp)
        )
        self.conn.commit()
        
    def auto_kudos_cycle(self):
        """Main cycle to check for new kudos and return them"""
        self.logger.info("Starting auto-kudos cycle...")
        
        try:
            # Get recent activities
            activities = self.get_athlete_activities(limit=20)
            self.logger.info(f"Found {len(activities)} recent activities")
            
            new_kudos_count = 0
            returned_kudos_count = 0
            
            for activity in activities:
                activity_id = activity['id']
                activity_name = activity.get('name', 'Unnamed Activity')
                
                # Skip if already processed recently (within last hour)
                if self.is_activity_processed(activity_id):
                    continue
                    
                self.logger.info(f"Checking kudos for activity: {activity_name} ({activity_id})")
                
                # Get kudos for this activity
                kudos = self.get_activity_kudos(activity_id)
                
                for kudo_giver in kudos:
                    user_id = kudo_giver['id']
                    user_name = f"{kudo_giver.get('firstname', '')} {kudo_giver.get('lastname', '')}".strip()
                    
                    self.logger.info(f"Processing kudos from: {user_name} ({user_id})")
                    new_kudos_count += 1
                    
                    # Get their recent activities
                    user_activities = self.get_athlete_activities_by_id(user_id, limit=2)
                    
                    for user_activity in user_activities:
                        user_activity_id = user_activity['id']
                        user_activity_name = user_activity.get('name', 'Unnamed Activity')
                        
                        # Skip if we've already given kudos to this activity
                        if self.has_given_kudos(user_id, user_activity_id):
                            continue
                            
                        # Give kudos
                        if self.give_kudos(user_activity_id):
                            self.record_kudos_given(user_id, user_activity_id)
                            returned_kudos_count += 1
                            self.logger.info(f"✅ Gave kudos to {user_name}'s activity: {user_activity_name}")
                            time.sleep(2)  # Small delay to be respectful
                        else:
                            self.logger.info(f"⚠️ Could not give kudos to {user_name}'s activity: {user_activity_name}")
                            
                # Mark this activity as processed
                self.mark_activity_processed(activity_id)
                
            self.logger.info(f"Cycle complete. New kudos: {new_kudos_count}, Returned kudos: {returned_kudos_count}")
            
        except Exception as e:
            self.logger.error(f"Error in auto-kudos cycle: {e}")
            
    def run_forever(self, interval_minutes: int = 10):
        """Run the bot continuously"""
        self.logger.info(f"🏃‍♂️ Starting Strava Auto-Kudos Bot (checking every {interval_minutes} minutes)")
        
        while True:
            try:
                self.auto_kudos_cycle()
                self.logger.info(f"💤 Sleeping for {interval_minutes} minutes...")
                time.sleep(interval_minutes * 60)
                
            except KeyboardInterrupt:
                self.logger.info("Bot stopped by user")
                break
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                self.logger.info("Continuing after error...")
                time.sleep(60)  # Wait 1 minute before retrying
                
    def cleanup(self):
        """Clean up resources"""
        if hasattr(self, 'conn'):
            self.conn.close()


if __name__ == "__main__":
    bot = StravaKudosBot()
    
    try:
        # Run with 10-minute intervals (adjust as needed)
        bot.run_forever(interval_minutes=10)
    finally:
        bot.cleanup()
