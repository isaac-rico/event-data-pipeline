from datetime import datetime, timezone, date
from zoneinfo import ZoneInfo

def abbr_state(region, mode="abbr"):
    if not region:
        return region
    if mode == "abbr" and len(region) > 2:
        return US_STATE_ABBR.get(region.strip().lower(), region)
    return region


US_STATE_ABBR = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY",
}


def format_event_time(iso_string, timezone):
    if not iso_string:
        return None
    dt = datetime.fromisoformat(iso_string.replace("Z", "+00:00"))
    if timezone:
        dt = dt.astimezone(ZoneInfo(timezone))
    return dt.replace(tzinfo=None)  # naive datetime — matches BigQuery DATETIME type

def format_order_date_pacific(value):
    """Convert an ISO registration instant to Pacific time, preserving its offset."""
    if value is None or value == "":
        return ""
    if not isinstance(value, str):
        raise ValueError("order_date must be an ISO timestamp string.")
    try:
        dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        if dt.tzinfo is None or dt.utcoffset() is None:
            raise ValueError()
    except ValueError:
        raise ValueError("order_date is invalid or missing its timezone; attendee upload stopped.") from None
    return dt.astimezone(ZoneInfo("America/Los_Angeles")).isoformat()
