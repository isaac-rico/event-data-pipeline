#!/usr/bin/env python3
"""Extract Cvent admission-item attendees to CSV and optionally synchronize BigQuery."""
import argparse
import csv
import logging
import os
from collections import Counter
from pathlib import Path
from tempfile import NamedTemporaryFile

import requests
from dotenv import load_dotenv

LOG = logging.getLogger('cvent')
EVENT_ID = None
ADMISSION_ITEM_ID = None
FIELDS = ['attendee_id', 'event_id', 'admission_item_id', 'status',
          'contact_id', 'first_name', 'last_name', 'email', 'company', 'title',
          'phone', 'mobile_phone', 'work_phone', 'home_phone', 'registered_at']


def safe_shape(value, depth=0):
    """Log keys and types only, never values, credentials or attendee details."""
    if depth >= 3:
        return type(value).__name__
    if isinstance(value, dict):
        return {str(k): safe_shape(v, depth + 1) for k, v in list(value.items())[:30]}
    if isinstance(value, list):
        return {'type': 'list', 'first_shape': safe_shape(value[0], depth + 1) if value else None}
    return type(value).__name__


def get_json(session, method, url, timeout, **kwargs):
    response = session.request(method, url, timeout=timeout, **kwargs)
    # Do not include URLs (pagination tokens), request headers or response bodies in errors.
    if not response.ok:
        raise RuntimeError(f'Cvent {method} failed with HTTP {response.status_code}; '
                           'check endpoint, credentials, scopes and account access.')
    try:
        data = response.json()
    except ValueError:
        raise RuntimeError('Cvent returned invalid JSON.') from None
    if not isinstance(data, dict):
        raise RuntimeError('Expected a JSON object from Cvent.')
    return data


def login(session, timeout):
    client_id = os.environ.get('CVENT_CLIENT_ID')
    secret = os.environ.get('CVENT_CLIENT_SECRET')
    if not client_id or not secret:
        raise RuntimeError('Set CVENT_CLIENT_ID and CVENT_CLIENT_SECRET in .env.')
    base = os.environ.get('CVENT_BASE_URL', 'https://api-platform.cvent.com/ea').rstrip('/')
    token_url = os.environ.get('CVENT_TOKEN_URL', base + '/oauth2/token')
    if not base.startswith('https://') or not token_url.startswith('https://'):
        raise RuntimeError('Cvent endpoints must use HTTPS.')
    form = {'grant_type': 'client_credentials', 'client_id': client_id}
    scope = os.environ.get('CVENT_SCOPE', '').strip()
    if scope:
        form['scope'] = scope
    data = get_json(session, 'POST', token_url, timeout,
                    auth=(client_id, secret), data=form)
    token = data.get('access_token')
    if not isinstance(token, str) or not token:
        raise RuntimeError('Authentication response is missing access_token.')
    session.headers.update({'Authorization': f'Bearer {token}', 'Accept': 'application/json'})
    return base


def registration_timestamp(value):
    """Return a UTC ISO timestamp, or NULL when the source provides no date."""
    from datetime import datetime, timezone
    if value is None or value == '':
        return None
    if not isinstance(value, str):
        raise RuntimeError('registeredAt must be a timezone-aware timestamp string.')
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError()
        return parsed.astimezone(timezone.utc).isoformat()
    except ValueError:
        raise RuntimeError('Invalid or timezone-missing registeredAt; no export written.') from None


def normalize(attendee, event_id, admission_id):
    contact = attendee.get('contact')
    contact = contact if isinstance(contact, dict) else {}
    # Confirm these nested contact fields against your account's attendee response.
    def phone_value(key):
        value = contact.get(key)
        return value.strip() if isinstance(value, str) else ''
    mobile, work, home = (phone_value(k) for k in ('mobilePhone', 'workPhone', 'homePhone'))
    phone = mobile or work or home
    return dict(zip(FIELDS, [attendee['id'], event_id, admission_id, attendee['status'],
                contact.get('id', ''), contact.get('firstName', ''), contact.get('lastName', ''),
                contact.get('email', ''), contact.get('company', ''), contact.get('title', ''),
                phone, mobile, work, home, registration_timestamp(attendee.get('registeredAt'))]))


def fetch(session, base, event_id, admission_id, timeout=30, max_pages=10000):
    rows, seen_ids, seen_tokens = [], set(), set()
    token = None
    totals = Counter()
    for page in range(1, max_pages + 1):
        params = {'filter': f"event.id eq '{event_id}'", 'limit': 100}
        if token is not None:
            params['token'] = token
        payload = get_json(session, 'GET', base + '/attendees', timeout, params=params)
        records, paging = payload.get('data'), payload.get('paging', {})
        if not isinstance(records, list) or not isinstance(paging, dict):
            raise RuntimeError('Expected data list and paging object; no export written.')
        counts = Counter(received=len(records))
        for attendee in records:
            if not isinstance(attendee, dict) or not isinstance(attendee.get('id'), str) or not attendee['id']:
                raise RuntimeError('Attendee missing valid id; cannot safely deduplicate.')
            if attendee['id'] in seen_ids:
                counts['duplicates'] += 1
                continue
            seen_ids.add(attendee['id'])
            counts['unique'] += 1
            item = attendee.get('admissionItem')
            if item is None:
                counts['missing_admission_item'] += 1
                continue
            if not isinstance(item, dict) or not isinstance(item.get('id'), str):
                counts['unexpected_shape'] += 1
                if totals['unexpected_shape'] + counts['unexpected_shape'] == 1:
                    LOG.warning('Safe attendee sample shape: %s', safe_shape(attendee))
                continue
            if item['id'] != admission_id:
                continue
            counts['item_matches'] += 1
            if attendee.get('status') == 'Accepted':
                rows.append(normalize(attendee, event_id, admission_id))
                counts['accepted'] += 1
        totals.update(counts)
        LOG.info('page=%d received=%d unique=%d duplicates=%d item_matches=%d accepted=%d missing_admission_item=%d unexpected_shape=%d',
                 page, *(counts[k] for k in ['received', 'unique', 'duplicates', 'item_matches', 'accepted', 'missing_admission_item', 'unexpected_shape']))
        next_token = paging.get('nextToken')
        if next_token is None or next_token == '':
            break
        if not isinstance(next_token, str):
            raise RuntimeError('paging.nextToken must be a string.')
        if next_token in seen_tokens:
            raise RuntimeError('Repeated pagination token detected; no export written.')
        seen_tokens.add(next_token)
        token = next_token
    else:
        raise RuntimeError('Maximum page count reached; no export written.')
    LOG.info('Totals: %s', dict(totals))
    if totals['missing_admission_item']:
        LOG.warning('%d attendees had no admissionItem and were excluded. '
                    'Export contains explicit target-ID matches only; verify missing assignments in Cvent if needed.',
                    totals['missing_admission_item'])
    if totals['unexpected_shape']:
        raise RuntimeError('Unexpected admissionItem shape encountered. Review the safe shape and '
                           'adjust fetch() only after confirming the real schema; no export written.')
    return rows


def export_csv(rows, destination):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Only replace the final file after pagination and writing both finish successfully.
    temporary = None
    try:
        with NamedTemporaryFile(mode='w', encoding='utf-8-sig', newline='',
                                dir=destination.parent, delete=False) as handle:
            temporary = Path(handle.name)
            csv_fields = [field for field in FIELDS[FIELDS.index('first_name'):]
                          if field != 'registered_at']
            writer = csv.DictWriter(handle, fieldnames=csv_fields)
            writer.writeheader()
            writer.writerows({field: row.get(field, '') for field in csv_fields}
                             for row in rows)
        temporary.replace(destination)
    finally:
        if temporary and temporary.exists():
            temporary.unlink()


def upload_bigquery(rows, table_id):
    """Stage a completed nonempty snapshot, then atomically synchronize its scope."""
    import re
    from uuid import uuid4
    from datetime import datetime, timedelta, timezone
    try:
        from google.cloud import bigquery
        from google.api_core.exceptions import GoogleAPICallError
        from google.auth.exceptions import GoogleAuthError
    except ImportError:
        raise RuntimeError('Install google-cloud-bigquery to upload; CSV has already been saved.') from None
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9-]*\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+', table_id):
        raise RuntimeError('Expected a fully qualified project.dataset.table identifier.')
    if not rows:
        raise RuntimeError('No matching rows; BigQuery upload skipped without deleting existing rows. CSV is saved.')
    keys = ['event_id', 'admission_item_id', 'attendee_id']
    seen = set()
    scopes = set()
    for row in rows:
        key = tuple(row.get(field) for field in keys)
        if not all(isinstance(value, str) and value for value in key) or key in seen:
            raise RuntimeError('Missing or duplicate BigQuery merge key; destination unchanged.')
        seen.add(key)
        scopes.add(key[:2])
    if len(scopes) != 1:
        raise RuntimeError('Expected one event/admission-item snapshot per upload; destination unchanged.')
    event_id, admission_id = next(iter(scopes))
    staging_id = table_id + '_staging_' + uuid4().hex
    schema = [bigquery.SchemaField(field, 'TIMESTAMP' if field == 'registered_at' else 'STRING')
              for field in FIELDS]
    updates = ',\n        '.join(f'{field} = S.{field}' for field in FIELDS if field not in keys)
    columns = ', '.join(FIELDS)
    values = ', '.join(f'S.{field}' for field in FIELDS)
    # Stale-row removal and MERGE commit together, mirroring the other pipelines
    # without leaving a partial deletion if MERGE fails.
    sql = f"""
    BEGIN TRANSACTION;
    DELETE FROM `{table_id}` T
    WHERE T.event_id = @event_id
      AND T.admission_item_id = @admission_item_id
      AND NOT EXISTS (
        SELECT 1 FROM `{staging_id}` S
        WHERE S.event_id = T.event_id
          AND S.admission_item_id = T.admission_item_id
          AND S.attendee_id = T.attendee_id
      );
    MERGE `{table_id}` T
    USING `{staging_id}` S
    ON T.event_id = S.event_id
      AND T.admission_item_id = S.admission_item_id
      AND T.attendee_id = S.attendee_id
    WHEN MATCHED THEN UPDATE SET
        {updates}
    WHEN NOT MATCHED THEN
      INSERT ({columns}) VALUES ({values});
    COMMIT TRANSACTION;
    """
    try:
        with bigquery.Client(project=table_id.split('.')[0]) as client:
            client.create_table(bigquery.Table(table_id, schema=schema), exists_ok=True)
            # Existing tables also need the newly introduced date column.
            client.query(
                f'ALTER TABLE `{table_id}` ADD COLUMN IF NOT EXISTS registered_at TIMESTAMP',
                timeout=30,
            ).result(timeout=300)
            staging = bigquery.Table(staging_id, schema=schema)
            staging.expires = datetime.now(timezone.utc) + timedelta(days=1)
            client.create_table(staging)
            config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            job = client.load_table_from_json(rows, staging_id, job_config=config, timeout=30)
            LOG.info('BigQuery staging load submitted: job=%s staging=%s', job.job_id, staging_id)
            job.result(timeout=300)
            query_config = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter('event_id', 'STRING', event_id),
                bigquery.ScalarQueryParameter('admission_item_id', 'STRING', admission_id),
            ])
            merge_job = client.query(sql, job_config=query_config, timeout=30)
            LOG.info('BigQuery synchronization submitted: job=%s', merge_job.job_id)
            merge_job.result(timeout=300)
            LOG.info('BigQuery merge complete: table=%s snapshot_rows=%d job=%s',
                     table_id, len(rows), merge_job.job_id)
            try:
                client.delete_table(staging_id, not_found_ok=True, timeout=30)
            except GoogleAPICallError:
                LOG.warning('Merge succeeded; staging cleanup deferred to its one-day expiration.')
    except (GoogleAPICallError, GoogleAuthError, TimeoutError):
        raise RuntimeError('BigQuery staging/merge failed or completion is uncertain; CSV is saved. '
                           'Check credentials, permissions, schema and logged job IDs before retrying. '
                           'Staging expires after one day; destination updates commit atomically.') from None



def main(argv=None):
    load_dotenv(Path(__file__).resolve().with_name('.env'))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--csv-only', action='store_true', help='Skip BigQuery upload')
    parser.add_argument('--bigquery-table', default=os.environ.get('ADMISSION_BIGQUERY_TABLE'))
    parser.add_argument('--event-id', default=os.environ.get('CVENT_EVENT_ID'))
    parser.add_argument('--admission-item-id', default=os.environ.get('CVENT_ADMISSION_ITEM_ID'))
    parser.add_argument('--output', default='exports/admission_attendees.csv')
    parser.add_argument('--timeout', type=float, default=30)
    parser.add_argument('--max-pages', type=int, default=10000)
    args = parser.parse_args(argv)
    if not args.event_id or not args.admission_item_id:
        parser.error('Set CVENT_EVENT_ID and CVENT_ADMISSION_ITEM_ID or supply the ID options.')
    if not args.csv_only and not args.bigquery_table:
        parser.error('Set ADMISSION_BIGQUERY_TABLE, supply --bigquery-table, or use --csv-only.')
    from uuid import UUID
    try:
        UUID(args.event_id)
        UUID(args.admission_item_id)
        if args.timeout <= 0 or args.max_pages <= 0:
            raise ValueError()
    except ValueError:
        parser.error('IDs must be UUIDs; timeout and max-pages must be positive.')
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    try:
        with requests.Session() as session:
            base = login(session, args.timeout)
            rows = fetch(session, base, args.event_id, args.admission_item_id,
                         args.timeout, args.max_pages)
        export_csv(rows, args.output)
        LOG.info('Export complete: %d Accepted attendees; file=%s', len(rows), args.output)
        if not args.csv_only:
            upload_bigquery(rows, args.bigquery_table)
        return 0
    except requests.RequestException:
        LOG.error('Network/timeout failure; no new export written. Check connectivity and retry.')
    except (RuntimeError, OSError) as exc:
        LOG.error('%s', exc)
    return 1




if __name__ == "__main__":
    raise SystemExit(main())
