# Cryptocurrency Data Pipeline

Near real time data pipeline for top 100 cryptocurrency coins data using [Coingecko API](https://www.coingecko.com/en/api/documentation). The data pipeline is made as a function in [Google Cloud Function](https://cloud.google.com/functions) to be executed every 5 minutes through [Google Cloud Scheduler](https://cloud.google.com/scheduler) trigger.

For now, I am running the pipeline through cloud run. However, the destination is a free PostgreSQL cloud database that I use temporarily. Next step would be to apply a Cloud Scheduler before moving the data to BigQuery. The plan is to execute the script for every 5 minutes just like the cron below. However, it seems that the data from CoinGecko is not refreshed for every 5 minutes. I might have to increase the wait time to avoid `Unique` row constraints

```
*/5 * * * * <path-to-executable-script>
```

