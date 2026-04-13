# pipewatch

> Lightweight CLI for monitoring and alerting on ETL pipeline health in real time.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git && cd pipewatch && pip install -e .
```

---

## Usage

Start monitoring a pipeline by pointing `pipewatch` at your config file:

```bash
pipewatch monitor --config pipeline.yaml
```

Example `pipeline.yaml`:

```yaml
pipelines:
  - name: daily_sales_etl
    schedule: "0 6 * * *"
    alert_on:
      - failure
      - delay_minutes: 30
    notify:
      slack: "#data-alerts"
```

Run a one-time health check:

```bash
pipewatch check --pipeline daily_sales_etl
```

View live status of all monitored pipelines:

```bash
pipewatch status
```

Silence alerts for a pipeline temporarily:

```bash
pipewatch silence --pipeline daily_sales_etl --duration 2h
```

---

## Features

- Real-time pipeline health monitoring
- Configurable alerting via Slack, email, or webhooks
- Lightweight with minimal dependencies
- Simple YAML-based configuration
- Temporary alert silencing with `silence` command

---

## License

This project is licensed under the [MIT License](LICENSE).
