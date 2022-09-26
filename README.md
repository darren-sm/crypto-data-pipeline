# Cryptocurrency Data Pipeline

Near real time data pipeline for top 100 cryptocurrency coins data using [Coingecko API](https://www.coingecko.com/en/api/documentation). The data pipeline is made as a function in [Google Cloud Function](https://cloud.google.com/functions) to be executed every 5 minutes through [Google Cloud Scheduler](https://cloud.google.com/scheduler) trigger.

For now, I am running the pipeline locally. I scheduled the script to run for every 5 minutes through cron with:

```
*/5 * * * * <path-to-executable-script>
```

