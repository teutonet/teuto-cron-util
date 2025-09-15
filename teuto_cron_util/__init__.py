#!/usr/bin/env python3

from os import environ
from datetime import datetime, timedelta, timezone
from json import loads

from cron_converter import Cron


ENV_CRON_UTIL_START_OFFSET_SECONDS = "CRON_UTIL_START_OFFSET_SECONDS"
DEFAULT_CRON_UTIL_START_OFFSET_SECONDS = "120"
CRON_UTIL_START_OFFSET_SECONDS = int(
    environ[ENV_CRON_UTIL_START_OFFSET_SECONDS]
    if ENV_CRON_UTIL_START_OFFSET_SECONDS in environ
    else DEFAULT_CRON_UTIL_START_OFFSET_SECONDS
)


def parse_scheduler_options(scheduler_options_json: str) -> dict[str, dict[str, str]]:
    return loads(scheduler_options_json)


def is_job_due(
    scheduler_options: dict[str, dict[str, str]], 
    reference_utc_now: datetime = None,
    pre_start_offset_seconds:int = CRON_UTIL_START_OFFSET_SECONDS
):
    now = reference_utc_now if reference_utc_now else datetime.now().astimezone(timezone.utc)

    pre_start_offset = timedelta(seconds=pre_start_offset_seconds)

    start_time = now + pre_start_offset

    next_job = None

    for job_id, job_definition in scheduler_options.items():
        job_cron = Cron(job_definition["schedule"])
        job_schedule = job_cron.schedule(timezone_str=job_definition["timezone"])
        next_exec = job_schedule.next()

        if start_time >= next_exec and (next_job is None or next_exec < next_job[3]):
            next_job = (job_id, job_cron, next_exec)

    return next_job is not None


if __name__ == "__main__":
    scheduler_options_json = """{"0d25889e-8db8-4658-bc4b-6c93612eef5e": {"schedule": "30 0 * * *", "timezone": "Europe/Berlin"}, "202c40d8-5fc7-4918-9724-1bdeccc5a20b": {"schedule": "*/10 * * * *", "timezone": "Europe/Berlin"}, "eeb5a2a6-d4c3-4e10-975d-5bda43e7ffa8": {"schedule": "*/3 * * * *", "timezone": "Europe/Berlin"}}"""
    scheduler_options = parse_scheduler_options(scheduler_options_json)
    print(is_job_due(scheduler_options))
