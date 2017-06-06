# Prometheus to Clickhouse Adapter
Prometheus to Clickhouse Adapter


# Clickhouse Schema

```
CREATE TABLE `metrics` (
  `EventDate`     Date,
  `EventDateTime` DateTime,

  `Metric`        String,
  `LabelNames`    Array(String),
  `LabelValues`   Array(String),

  `Value`         Float64

) ENGINE = MergeTree(EventDate, (Metric, LabelNames), 8192)
```
