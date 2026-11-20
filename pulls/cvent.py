"""
    CVENT API Pull

"""

import os
import requests
import pandas as pd
from datetime import datetime, timezone, date
import time
from collections import Counter

from helpers.helper import abbr_state, format_event_time, format_order_date_pacific

# --- Config -----------------------------------------------------------
from config import (
    CVENT_CLIENT_ID,
    CVENT_CLIENT_SECRET,
    CVENT_BASE_URL,
)

# --- Helper Vars ------------------------------------------------------
ACCEPTED_STATUS = "Accepted"

RESULTS_KEY = "data"
CURSOR_KEY = "nextCursor"

CUTOFF_DATE = None
if os.environ.get('CVENT_CREATED_AFTER'):
    CUTOFF_DATE = datetime.fromisoformat(os.environ['CVENT_CREATED_AFTER'].replace('Z', '+00:00'))
    if CUTOFF_DATE.tzinfo is None:
        raise RuntimeError('CVENT_CREATED_AFTER must include a timezone.')
DATE_FIELD = "created"


# ------------------------------------------------------------------
# AUTHENTICATION
# ------------------------------------------------------------------

def cvent_login():
    """Authenticate with bounded retries and credential-safe errors."""
    if not CVENT_CLIENT_ID or not CVENT_CLIENT_SECRET:
        raise RuntimeError('Missing Cvent client ID or client secret in runtime configuration.')
    for attempt in range(3):
        try:
            resp = requests.post(
                f"{CVENT_BASE_URL.rstrip('/')}/oauth2/token",
                auth=(CVENT_CLIENT_ID, CVENT_CLIENT_SECRET),
                data={"grant_type": "client_credentials"},
                headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
                timeout=30,
                allow_redirects=False,
            )
        except requests.RequestException:
            if attempt < 2:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError('Cvent login network/timeout failure after 3 attempts.') from None
        if resp.status_code in (429, 500, 502, 503, 504) and attempt < 2:
            time.sleep(2 ** attempt)
            continue
        # Do not log response bodies, credentials, tokens or arbitrary headers.
        details = (f'HTTP {resp.status_code}; '
                   f'content_type={resp.headers.get("Content-Type", "missing")}; '
                   f'body_bytes={len(resp.content)}')
        if not 200 <= resp.status_code < 300:
            if resp.status_code in (401, 403):
                hint = 'Check the Cvent credentials, app permissions and Cloud Run secret mappings.'
            elif resp.status_code == 429:
                hint = 'Cvent rate limit reached; retry later.'
            elif 300 <= resp.status_code < 400 or resp.status_code == 404:
                hint = 'Check the Cvent API region and authentication endpoint.'
            else:
                hint = 'Check Cvent availability and runtime endpoint configuration.'
            raise RuntimeError(f'Cvent login failed ({details}). {hint}')
        try:
            data = resp.json()
        except ValueError:
            raise RuntimeError(f'Cvent login returned invalid JSON ({details}). '
                               'Check API availability and the authentication endpoint.') from None
        token = data.get('access_token') if isinstance(data, dict) else None
        if not isinstance(token, str) or not token.strip():
            raise RuntimeError(f'Cvent login response is missing access_token ({details}). '
                               'Check credentials, permissions and authentication configuration.')
        return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


# --- 2. Fetch one event (port of fetchCvent) -------------------------------
def fetch_cvent(event_id, headers):

    # --------------------------------------------------------
    # Event information
    # --------------------------------------------------------

    event_resp = requests.get(
        f"{CVENT_BASE_URL}/events/{event_id}",
        headers=headers,
    )

    event_resp.raise_for_status()

    event = event_resp.json()

    # --------------------------------------------------------
    # Count accepted attendees
    # --------------------------------------------------------

    filter_str = (
        f"event.id eq '{event_id}'"
    )

    if ACCEPTED_STATUS:
        filter_str += (
            f" AND status eq "
            f"'{ACCEPTED_STATUS}'"
        )

    att_resp = requests.get(
        f"{CVENT_BASE_URL}/attendees",
        headers=headers,
        params={
            "filter": filter_str,
            "limit": 1,
        },
    )

    att_resp.raise_for_status()

    att = att_resp.json()

    count = (
        (att.get("paging") or {})
        .get("totalCount", 0)
    )

    # --------------------------------------------------------
    # Location
    # --------------------------------------------------------

    venues = event.get("venues") or []

    venue = (
        venues[0]
        if venues
        else {}
    )

    addr = venue.get("address") or {}

    state = abbr_state(
        addr.get("region"),
        "abbr",
    )

    location = ", ".join(
        filter(
            None,
            [
                addr.get("city"),
                state or addr.get("state"),
            ],
        )
    )

    # --------------------------------------------------------
    # Timezone
    # --------------------------------------------------------

    timezone_name = event.get("timezone")

    # --------------------------------------------------------
    # Capacity
    # --------------------------------------------------------

    cap = 0

    if isinstance(event.get("capacity"),(int, float)):
        cap = event["capacity"]

    elif (
        isinstance(
            event.get("capacity"),
            dict,
        )
        and isinstance(
            event["capacity"].get("total"),
            (int, float),
        )
    ):
        cap = event["capacity"]["total"]

    elif (
        isinstance(
            event.get("registration"),
            dict,
        )
        and isinstance(
            event["registration"].get("capacity"),
            (int, float),
        )
    ):
        cap = event["registration"]["capacity"]

    # --------------------------------------------------------
    # Final record
    # --------------------------------------------------------

    return {
        "event_id": event_id,
        "event_name": event.get("title", ""),
        "event_location": location,
        "event_timezone": timezone_name,
        "event_start": format_event_time(event.get("start"), timezone_name),
        "event_end": format_event_time(event.get("end"), timezone_name),
        "event_capacity": cap,
        "event_attendee_count": count,
        "event_source": "CVENT",
        "event_url":("https://web.cvent.com/"f"events/{event_id}/summary"),
    }

# ============================================================
# GET ALL EVENT IDS
# ============================================================

def get_all_event_ids(headers, extra_filter=None,):

    params = {}

    if extra_filter:
        params["filter"] = extra_filter

    ids = []

    while True:

        resp = requests.get(
            f"{CVENT_BASE_URL}/events",
            headers=headers,
            params=params,
        )

        resp.raise_for_status()

        page = resp.json()

        ids.extend(
            event["id"]
            for event
            in page.get(
                RESULTS_KEY,
                [],
            )
        )

        cursor = (
            (page.get("paging") or {})
            .get(CURSOR_KEY)
        )

        if not cursor:
            break

        params["cursor"] = cursor

    return ids

# ============================================================
# RUN COMPLETE CVENT PULL
# ============================================================

def run_cvent_pull():

    headers = cvent_login()

    date_filter = (f"{DATE_FIELD} gt '{CUTOFF_DATE.isoformat()}'"
                   if CUTOFF_DATE else None)

    event_ids = get_all_event_ids(
        headers,
        extra_filter=date_filter,
    )

    # Optional comma-separated event IDs to exclude in your own account.
    skippable_events = {value.strip() for value in
                        os.environ.get('CVENT_EXCLUDED_EVENT_IDS', '').split(',')
                        if value.strip()}

    rows = []

    for event_id in event_ids:

        if event_id in skippable_events:
            continue

        rows.append(
            fetch_cvent(
                event_id,
                headers,
            )
        )

        # Respect Cvent API rate limits
        time.sleep(0.5)

    cvent_df = pd.DataFrame(rows)

    if not cvent_df.empty:

        cvent_df["event_start"] = (
            pd.to_datetime(
                cvent_df["event_start"]
            )
        )

        cvent_df["event_end"] = (
            pd.to_datetime(
                cvent_df["event_end"]
            )
        )

    return cvent_df

# ------------------------------------------------------------------
# ATTENDEES
# ------------------------------------------------------------------

def fetch_attendees(event_id, event_name):
    headers = cvent_login()

    records = []
    token = None
    seen_tokens = set()
    seen_ids = set()

    page_number = 0
    status_counts = Counter()

    while True:

        # -------------------------------------------
        # Request parameters
        # -------------------------------------------

        params = {
            "filter": f"event.id eq '{event_id}'",
            "limit": 100,
        }

        if token:
            params["token"] = token

        # -------------------------------------------
        # Fetch page
        # -------------------------------------------

        response = requests.get(
            f"{CVENT_BASE_URL}/attendees",
            headers=headers,
            params=params,
            timeout=30,
        )

        response.raise_for_status()

        data = response.json()

        attendees = data.get("data", [])
        paging = data.get("paging") or {}

        page_number += 1

        print(
            f"[{event_name}] "
            f"Page {page_number}: "
            f"{len(attendees)} attendees"
        )

        # -------------------------------------------
        # Process attendees
        # -------------------------------------------

        for attendee in attendees:

            attendee_id = attendee.get("id")

            # Detect overlapping/repeated pages
            if attendee_id in seen_ids:
                raise RuntimeError(
                    f"Duplicate attendee ID detected: "
                    f"{attendee_id}"
                )

            if attendee_id:
                seen_ids.add(attendee_id)

            status = (
                attendee.get("status") or ""
            ).strip().lower()

            status_counts[status] += 1

            # Only currently accepted attendees
            if status != "accepted":
                continue

            contact = attendee.get("contact") or {}
            registration_type = attendee.get("admissionItem") or {}

            records.append({
                "event_id": event_id,
                "attendee_id": attendee_id,
                "first_name": contact.get("firstName", ""),
                "last_name": contact.get("lastName", ""),
                "email": contact.get("email", ""),
                "phone": next((value.strip() for value in (
                    contact.get("mobilePhone"), contact.get("workPhone"), contact.get("homePhone")
                ) if isinstance(value, str) and value.strip()), ""),
                "company": contact.get("company") or "",
                "order_id": attendee.get("confirmationNumber", ""),
                "order_date": format_order_date_pacific(attendee.get("registeredAt")),
                "event_name": event_name,
                "ticket_type": registration_type.get("code", ""),
            })

        # -------------------------------------------
        # Pagination
        # -------------------------------------------

        next_token = paging.get("nextToken")

        print(
            f"Total available: {paging.get('totalCount')}"
        )

        if not next_token:
            break

        if next_token in seen_tokens:
            raise RuntimeError(
                f"Cvent returned a repeated pagination token "
                f"for event {event_id}"
            )

        seen_tokens.add(next_token)
        token = next_token

    # -------------------------------------------
    # Final diagnostics
    # -------------------------------------------

    print(f"\nEvent: {event_name}")
    print(f"Pages retrieved: {page_number}")
    print(f"Unique attendee IDs: {len(seen_ids)}")
    print(f"Accepted attendees: {len(records)}")
    print(f"Status breakdown: {dict(status_counts)}\n")

    # -------------------------------------------
    return records


if __name__ == "__main__":
    cvent_df = run_cvent_pull()
    cvent_df.to_csv("cvent.csv", index=False)
    print(cvent_df)
