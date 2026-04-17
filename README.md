# Turnstile — Serverless Data Lake Pipeline

Turnstile is an AWS CDK project for a serverless Bronze → Silver → Gold ETL pipeline.

## Current Architecture

- **Ingest (Bronze):** Python Docker Lambda (`lambdas/ingest_env/app.py`) fetches USGS + Open-Meteo data and writes parquet files to:
  - `s3://<lake>/bronze/usgs/ingestion_date=YYYY/MM/DD/HH/`
  - `s3://<lake>/bronze/weather/ingestion_date=YYYY/MM/DD/HH/`
  - Run manifests to `s3://<lake>/bronze/_manifests/...`
- **Transform/Aggregate (Silver/Gold):** Step Functions executes Athena DDL/CTAS queries to create/update Bronze external tables and build Silver/Gold tables.
- **State tracking:** DynamoDB table (`RunManifest`) stores window status for duplicate prevention and replay.
- **Orchestration:** Step Functions state machine triggered hourly by EventBridge.
- **Monitoring:** CloudWatch alarms publish to SNS.

## Repository Layout

```text
.
├─ bin/turnstile.ts
├─ lib/turnstile-stack.ts
├─ lambdas/
│  ├─ ingest_env/
│  ├─ compute_checksum/    # dependency package only (no active lambda handler)
│  └─ dq_check/            # dependency package only (no active lambda handler)
├─ test/turnstile.test.ts
└─ README.md
```

## Deploy

```bash
npm install
npm run build
npx cdk deploy
```

## Runtime Inputs

The state machine accepts:

- `params.date` (optional, `YYYY-MM-DD`)
- `params.hour` (optional, `HH`)
- `force_replay` (optional, boolean)

Behavior:

- If `params` is missing, date/hour is derived from execution start time.
- If `force_replay=true`, existing DynamoDB state for the window is deleted before processing.
- If `force_replay=false` and the window already has a success/failure manifest row, processing is skipped.

## DynamoDB State Model

- `pk`: `weather#<date>T<hour>:00Z`
- `sk`: `manifest`
- `status`: `SUCCEEDED` or `FAILED`
- timestamps: `started_at`, `finished_at`

## Alerts

The stack creates:

- Step Functions failure alarm
- Ingest Lambda error alarm
- SNS topic for alarm notifications

Subscribe an email:

```bash
aws sns subscribe \
  --topic-arn "$SNS_TOPIC_ARN" \
  --protocol email \
  --notification-endpoint your@email.com
```

## Notes

- Silver/Gold quality checks are currently implemented through Athena transformation logic and state-machine failure handling; there is no separate transform Lambda in this repository.
- Glue crawlers are not defined in the current CDK stack.
