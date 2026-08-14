# PathLens

PathLens is a streaming pipeline for **IOT-based traffic-camera processing and alerting**. Raw records are consumed from Kafka, parsed and enriched with device/location data, then evaluated against notification rules — with every stage observable through Prometheus metrics and backed by PostgreSQL.

## Architecture

```
                                │  raw IOT-traffic-data records (RawLogProto)
                                ▼
                           ┌──────────┐
                           │  Kafka   │  raw-log topic
                           └────┬─────┘
                                │ consume (KafkaParallelConsumer)
                                ▼
   ┌─────────────────────────────────────────┐   ┌──────────────────────────────┐
   │               Processor                 │   │        Device REST API       │
   │  parse (CameraLogParser)                │◄──│     devices & locations      │
   │  transform + enrich with device cache   │   │                              │
   │  route valid → destination, bad → trash │   │                              │
   └───────────────────┬─────────────────────┘   └──────────────┬───────────────┘
                       │  enriched logs (CameraLogProto)         ▲ revision-based
                       ▼                                        │ cache sync
                   ┌──────────┐                                 │
                   │  Kafka   │  destination / trash topics     │
                   └────┬─────┘                                 │
                        │ consume                               │
                        ▼                                       │
   ┌─────────────────────────────────────────┐   ┌──────────────┴───────────────┐
   │          Alerting Evaluator             │   │                              │
   │            rule evaluation              │◄──│    Notification-System       │
   │  rules cached from alerting API         │   │                              │
   └───────────────────┬─────────────────────┘   └──────────────────────────────┘
                       │ notifications & tracked records
                       ▼
                ┌──────────────┐
                │  PostgreSQL  │
                └──────────────┘
```

## Getting started

```bash
# Build everything (compile + test + checkstyle)
./gradlew build

# Just check code style
./gradlew checkstyleMain checkstyleTest

# Run the test suite for a module
./gradlew :device:rest:test :processor:test
```

## Code generation

jOOQ sources are generated from the Flyway migrations against a disposable PostgreSQL container:

```bash
./gradlew :device:rest:generateJooq
```
```bash
./gradlew :alerting-system:rest:generateJooq
```