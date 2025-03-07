from dagster import ScheduleDefinition
from ..jobs import warehouse_update_job, lake_update_job, stock_retrieval_job

# Schedule to run stock_retrieval job at midnight on Sundays
stocks_update_schedule = ScheduleDefinition(
    job=stock_retrieval_job,
    cron_schedule="0 0 * * 0", # every Sunday at midnight
)
