"""Runtime configuration. Copy .env.example to .env before running locally."""
import os
import re
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().with_name('.env'))

def required(name):
    value = os.environ.get(name, '').strip()
    if not value:
        raise RuntimeError(f'Set {name} in your environment or .env file.')
    return value

PROJECT_ID = required('GOOGLE_CLOUD_PROJECT')
DATASET_ID = os.environ.get('BIGQUERY_DATASET', 'event_data')
if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9-]*', PROJECT_ID):
    raise RuntimeError('Invalid GOOGLE_CLOUD_PROJECT identifier.')
if not re.fullmatch(r'[A-Za-z0-9_]+', DATASET_ID):
    raise RuntimeError('Invalid BIGQUERY_DATASET identifier.')
EVENT_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.events'
EVENT_STAGING_ID = f'{PROJECT_ID}.{DATASET_ID}.events_staging'
ATTENDEE_TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.attendee_data'
ATTENDEE_STAGING_ID = f'{PROJECT_ID}.{DATASET_ID}.attendee_staging'
EVENT_TABLE_ID_TEST = f'{PROJECT_ID}.{DATASET_ID}.events_test'
EVENT_STAGING_ID_TEST = f'{PROJECT_ID}.{DATASET_ID}.events_staging_test'
ATTENDEE_TABLE_ID_TEST = f'{PROJECT_ID}.{DATASET_ID}.attendee_data_test'
ATTENDEE_STAGING_ID_TEST = f'{PROJECT_ID}.{DATASET_ID}.attendee_staging_test'
CVENT_CLIENT_ID = required('CVENT_CLIENT_ID')
CVENT_CLIENT_SECRET = required('CVENT_CLIENT_SECRET')
CVENT_BASE_URL = os.environ.get('CVENT_BASE_URL', 'https://api-platform.cvent.com/ea').rstrip('/')
EVENTBRITE_TOKEN = required('EVENTBRITE_TOKEN')
EVENTBRITE_BASE = os.environ.get('EVENTBRITE_BASE', 'https://www.eventbriteapi.com/v3').rstrip('/')
