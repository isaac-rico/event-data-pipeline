"""Run event and attendee synchronization, with an optional admission-item extract."""
import os
from pipelines.events import run_event_pipeline
from pipelines.attendees import run_attendee_pipeline
from getadmissionitem import main as run_admission_extract


def main():
    print('EVENT DATA PIPELINE')
    print('[1/2] Events')
    event_df = run_event_pipeline()
    print('[2/2] Attendees')
    run_attendee_pipeline(event_df)
    if os.environ.get('ENABLE_ADMISSION_EXTRACT', '').lower() in ('1', 'true', 'yes'):
        print('[optional] Admission-item attendees')
        if run_admission_extract([]) != 0:
            raise RuntimeError('Admission-item extraction failed.')
    print('PIPELINE COMPLETED SUCCESSFULLY')


if __name__ == '__main__':
    main()
