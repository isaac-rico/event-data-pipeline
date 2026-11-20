"""Eventbrite events and attendee extraction for the Cloud Run pipeline.

Preserves the workflow of the existing eventbrite_pull notebook:
  organization lookup -> event IDs -> individual event details/ticket classes.
"""

import time
from datetime import datetime

import pandas as pd
import requests

from config import EVENTBRITE_TOKEN, EVENTBRITE_BASE
from helpers.helper import format_order_date_pacific


# ============================================================
# CONFIG
# ============================================================

REQUEST_TIMEOUT = 30
REQUEST_DELAY = 0.5


# ============================================================
# AUTHENTICATION
# ============================================================

def get_eventbrite_token():
    """Use the token injected as an environment variable by Cloud Run."""
    if not EVENTBRITE_TOKEN:
        raise RuntimeError("EVENTBRITE_TOKEN is not configured.")
    return EVENTBRITE_TOKEN


def get_headers(token):
    return {"Authorization": f"Bearer {token}"}


# ============================================================
# FETCH ONE EVENT
# ============================================================

def fetch_eventbrite(event_id, token):
    headers = get_headers(token)

    event_resp = requests.get(
        f"{EVENTBRITE_BASE}/events/{event_id}/",
        params={"expand": "venue"},
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    if not event_resp.ok:
        raise RuntimeError(
            f'Eventbrite event request failed for id "{event_id}" '
            f"({event_resp.status_code}): {event_resp.text[:300]}"
        )
    event = event_resp.json()

    # Ticket-class pagination: handles events with more than one page of classes.
    tickets = []
    continuation = None
    while True:
        params = {"continuation": continuation} if continuation else {}
        ticket_resp = requests.get(
            f"{EVENTBRITE_BASE}/events/{event_id}/ticket_classes/",
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        ticket_resp.raise_for_status()
        ticket_page = ticket_resp.json()
        tickets.extend(ticket_page.get("ticket_classes", []))

        paging = ticket_page.get("pagination") or {}
        continuation = paging.get("continuation") if paging.get("has_more_items") else None
        if not continuation:
            break

    cap = sum(t.get("quantity_total") or 0 for t in tickets)
    sold = sum(t.get("quantity_sold") or 0 for t in tickets)

    addr = (event.get("venue") or {}).get("address") or {}
    if addr.get("localized_area_display"):
        location = addr["localized_area_display"]
    elif addr.get("city") and addr.get("region"):
        location = f"{addr['city']}, {addr['region']}"
    else:
        location = addr.get("city", "")

    event_timezone = (event.get("start") or {}).get("timezone")

    def parse_local(field):
        local = (event.get(field) or {}).get("local")
        return datetime.fromisoformat(local) if local else None

    return {
        "event_id": event_id,
        "event_name": (event.get("name") or {}).get("text", ""),
        "event_url": event.get("url", ""),
        "event_location": location,
        "event_timezone": event_timezone,
        "event_start": parse_local("start"),
        "event_end": parse_local("end"),
        "event_capacity": cap,
        "event_attendee_count": sold,
        "event_source": "EVENTBRITE",
    }


# ============================================================
# FETCH ATTENDEES (RAW)
# ============================================================

def get_eventbrite_attendees(event_id, event_name):
    """
    Retrieve Eventbrite attendees and normalize records
    for the BigQuery attendee_data table.
    """

    if not event_id:
        raise ValueError("Eventbrite event ID is required.")

    headers = get_headers(EVENTBRITE_TOKEN)
    records = []
    continuation = None
    seen_continuations = set()

    while True:
        params = {
            "expand": "ticket_class"
        }

        if continuation:
            params["continuation"] = continuation

        resp = requests.get(
            f"{EVENTBRITE_BASE}/events/{event_id}/attendees/",
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )

        if not resp.ok:
            raise RuntimeError(
                f"Eventbrite attendee request failed "
                f"({resp.status_code}): {resp.text[:300]}"
            )

        data = resp.json()

        for att in data.get("attendees", []):

            # Preserve original status-filtering intent
            status = (att.get("status") or "").strip().lower()

            if status != "attending":
                continue

            profile = att.get("profile") or {}
            ticket_class = att.get("ticket_class") or {}

            records.append({
                "event_id": event_id,
                "attendee_id": att.get("id", ""),
                "first_name": profile.get("first_name", ""),
                "last_name": profile.get("last_name", ""),
                "email": profile.get("email", ""),
                "phone": (profile.get("cell_phone") or "").strip(),
                "company": profile.get("company") or "",
                "order_id": att.get("order_id", ""),
                "order_date": format_order_date_pacific(att.get("created")),
                "event_name": event_name,
                "ticket_type": ticket_class.get("name", ""),
            })

        pagination = data.get("pagination") or {}

        if not pagination.get("has_more_items"):
            break

        next_continuation = pagination.get("continuation")

        if not next_continuation:
            raise RuntimeError(
                "Eventbrite indicates more attendee pages "
                "but did not return a continuation token."
            )

        if next_continuation in seen_continuations:
            raise RuntimeError(
                "Repeated Eventbrite pagination token detected."
            )

        seen_continuations.add(next_continuation)
        continuation = next_continuation

    return records


# ============================================================
# GET ORGANIZATION ID
# ============================================================

def get_organization_id(token):
    headers = get_headers(token)
    resp = requests.get(
        f"{EVENTBRITE_BASE}/users/me/organizations/",
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    orgs = resp.json().get("organizations", [])
    if not orgs:
        raise RuntimeError("No Eventbrite organizations found for this token.")
    return orgs[0]["id"]  # Preserve notebook behavior; choose explicitly if multiple.


# ============================================================
# GET ALL EVENT IDS
# ============================================================

def get_all_event_ids(organization_id, token, extra_params=None):
    headers = get_headers(token)
    params = dict(extra_params or {})
    ids = []
    continuation = None

    while True:
        if continuation:
            params["continuation"] = continuation

        resp = requests.get(
            f"{EVENTBRITE_BASE}/organizations/{organization_id}/events/",
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        page = resp.json()
        ids.extend(event["id"] for event in page.get("events", []))

        paging = page.get("pagination") or {}
        continuation = paging.get("continuation") if paging.get("has_more_items") else None
        if not continuation:
            break

    return ids


# ============================================================
# RUN COMPLETE EVENTBRITE PULL
# ============================================================

def fetch_events():
    token = get_eventbrite_token()
    organization_id = get_organization_id(token)
    event_ids = get_all_event_ids(organization_id, token)

    rows = []
    for event_id in event_ids:
        rows.append(fetch_eventbrite(event_id, token))
        time.sleep(REQUEST_DELAY)

    return pd.DataFrame(rows)


if __name__ == "__main__":
    eventbrite_df = fetch_events()
    eventbrite_df.to_csv("eventbrite.csv", index=False)
    print(eventbrite_df)
