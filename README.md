# Event Registration Data Pipeline

Cvent and Eventbrite each show what’s happening on their own platform. I built this pipeline to bring both together so an events team can answer everyday questions from one dashboard: **How many people have registered? Is registration picking up? Which events might need more promotion?**

## What the team can see

- **Across events:** Compare registrations, filter events by quarter, and see how much of each event’s capacity is filled.
- **For a single event:** Select an event to see its details, daily registration trends, ticket-type breakdown, and attendee list.
- **For a specific course or ticket:** Track registrations separately for a selected Cvent admission item.

Capacity is most useful alongside the actual count and time remaining. For example, **24 of 30 seats filled** tells a different story from **240 of 500**, especially if one event is next week and the other is two months away.

## What I built

```text
Cvent + Eventbrite → Python pipeline → BigQuery → Looker Studio
                        ↑
                Scheduled Cloud Run job
```

Python pulls event details and registrations from both platforms and maps them to a shared set of fields. BigQuery updates existing records and inserts new ones, so routine reruns can refresh the reporting tables without appending the same registrations each time. The Dockerized job runs in Cloud Run and can be triggered by Cloud Scheduler.

The dashboard reads from BigQuery, so the team can review updated registration numbers without preparing and uploading a separate dashboard file. The trend charts show when **currently qualifying attendees** registered. They don’t trace cancellations over time or measure who actually attended.

## Explore the code

| File | What to look for |
| --- | --- |
| [main.py](main.py) | How the events and attendee steps run together |
| [pulls/](pulls/) | How the pipeline retrieves and normalizes data from each platform |
| [pipelines/](pipelines/) | How staging tables feed the BigQuery updates |
| [getadmissionitem.py](getadmissionitem.py) | An optional Cvent ticket-level extract that can export a CSV and update BigQuery |

## Running it with your own event data

This repository showcases the pipeline code. A live run requires access to Cvent and Eventbrite event data, plus a Google Cloud project with an existing BigQuery dataset. The public copy contains sample settings in [.env.example](.env.example).

With Python 3.11, install the packages in `requirements.txt`, copy `.env.example` to `.env`, and fill in your own settings. Authenticate with `gcloud auth application-default login`, then run `python main.py`. Start with a test dataset because the pipeline writes to and updates the configured tables.
