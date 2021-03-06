# Postgres Event Sourcing

## Goal

This is an attempt to reproduce a Kafka-like data behavior for event-sourcing using Postgres to coordinate write/read concurrency over a topic of messages from multiple producers/consumers instances.

## Pitch

Big companies benefit from products like RabbitMQ, Kafka, and Elastic, and have the matching economy to pay for the required resources, mostly clusters of virtual machines.

Most of the companies around this world would benefit from the same patterns, but **don't have enough scale to make it efficient**.

> Running a cluster of 3 servers just to store a few million messages in a RabbitMQ queue or Kafka topic is simply too expensive.

I'm building open-source low-cost tools based on PostgreSQL that provide functionalities similar to the big players at a fraction of the cost.

> [Fetchq](https://fetchq.com) is the first tool that I have released.  
> It offers an efficient queue management system based on Postgres.

## Development

```bash
# Start the development project:
docker-compose up -d

# Run tests:
npm test
npm run tdd
```

## Stress Tests

A stress test runs an entire cycle of producer / consumer out of a combination of settings that is provided as `CSV` file.
You can find the demo CSVs in `stress_xxx/xxx.csv`.

The results of each round will be appended into `.docker-data/stats` for the specific file.

Please refer to the `docker-compose.stress.yml` definition to checkout the stress test setup and see the available configuration options.

```bash
# First Setup
# (installs NPM dependencies into the container)
make stress-setup on=xxx

# Single shot
# (customize the test settings in `.env`)
make stress-run on=xxx

# Out of a CSV
# (customize the test settings in the relative `.csv` file)
./stress partitions
./stress subscriptions
```
