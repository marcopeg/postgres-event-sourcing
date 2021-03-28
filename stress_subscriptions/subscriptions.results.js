const fs = require("fs");
const prettyMilliseconds = require("pretty-ms");
const { Client } = require("pg");

const boot = async () => {
  console.log("Connecting...");
  const db = new Client({ connectionString: process.env.PGSTRING });
  await db.connect();

  const writeConcurrencyFactor =
    process.env.PRODUCER_BATCH_PARALLEL * process.env.PRODUCER_REPLICAS;

  const readConcurrencyFactor =
    process.env.CONSUMER1_REPLICAS *
      Math.min(
        process.env.CONSUMER1_BATCH_PARALLEL,
        process.env.PRODUCER_MAX_PARTITIONS
      ) +
    process.env.CONSUMER2_REPLICAS *
      Math.min(
        process.env.CONSUMER2_BATCH_PARALLEL,
        process.env.PRODUCER_MAX_PARTITIONS
      );

  const input = await db.query(`
    SELECT
      COUNT(*) AS "count",
      MAX("created_at") - MIN("created_at") AS "elapsed" 
    FROM "fq"."events";
  `);

  const results = await db.query(`
    SELECT
      COUNT(*) AS "count",
      MAX("processed_at") - MIN("processed_at") AS "elapsed" 
    FROM "fq"."results";
  `);

  const writeElapsed =
    (input.rows[0].elapsed.hours || 0) * 1000 * 60 * 60 +
    (input.rows[0].elapsed.minutes || 0) * 1000 * 60 +
    (input.rows[0].elapsed.seconds || 0) * 1000 +
    input.rows[0].elapsed.milliseconds;

  const readElapsed =
    (results.rows[0].elapsed.hours || 0) * 1000 * 60 * 60 +
    (results.rows[0].elapsed.minutes || 0) * 1000 * 60 +
    (results.rows[0].elapsed.seconds || 0) * 1000 +
    results.rows[0].elapsed.milliseconds;

  const writeSpeed = writeElapsed / input.rows[0].count;
  const writeThroughput = 1000 / writeSpeed;

  const readSpeed = readElapsed / results.rows[0].count;
  const readThroughput = 1000 / readSpeed;

  console.log("");
  console.log("=============== SETUP ==================");
  console.log();
  console.log(`Write concurrency factor: ${writeConcurrencyFactor}`);
  console.log(
    `> ${process.env.PRODUCER_REPLICAS} containers pushing ${process.env.PRODUCER_BATCH_PARALLEL} in parallel`
  );
  console.log("");
  console.log(`Read concurrency factor: ${readConcurrencyFactor}`);
  console.log(
    `> Consumer n.1: ${Math.min(
      process.env.CONSUMER1_BATCH_PARALLEL,
      process.env.PRODUCER_MAX_PARTITIONS
    )}/${process.env.CONSUMER1_REPLICAS}`
  );
  console.log(
    `> Consumer n.2: ${Math.min(
      process.env.CONSUMER2_BATCH_PARALLEL,
      process.env.PRODUCER_MAX_PARTITIONS
    )}/${process.env.CONSUMER2_REPLICAS}`
  );

  console.log("");
  console.log("============== RESULTS =================");
  console.log("");
  console.log(
    `${input.rows[0].count} events were pushed in ${prettyMilliseconds(
      writeElapsed
    )}`
  );
  console.log(`> ${Math.round(writeThroughput)} events/s`);
  console.log("");
  console.log(
    `${results.rows[0].count} events were processed in ${prettyMilliseconds(
      readElapsed
    )}`
  );
  console.log(`> ${Math.round(readThroughput)} events/s`);
  console.log("");

  if (!fs.existsSync("/stats/subscriptions.csv")) {
    const headers = [
      "producer_replicas",
      "producer_batch_serial",
      "producer_batch_parallel",
      "producer_max_partitions",
      "consumer1_replicas",
      "consumer1_batch_parallel",
      "consumer2_replicas",
      "consumer2_batch_parallel",
      "events_count",
      "events_elapsed",
      "results_count",
      "results_elapsed",
      "write_concurrency_factor",
      "write_speed",
      "write_throughput",
      "read_concurrency_factor",
      "read_speed",
      "read_throughput",
    ].join(",");
    fs.appendFileSync("/stats/subscriptions.csv", `${headers}\n`);
  }

  const csv = [
    process.env.PRODUCER_REPLICAS,
    process.env.PRODUCER_BATCH_SERIAL,
    process.env.PRODUCER_BATCH_PARALLEL,
    process.env.PRODUCER_MAX_PARTITIONS,
    process.env.CONSUMER1_REPLICAS,
    process.env.CONSUMER1_BATCH_PARALLEL,
    process.env.CONSUMER2_REPLICAS,
    process.env.CONSUMER2_BATCH_PARALLEL,
    input.rows[0].count,
    writeElapsed,
    results.rows[0].count,
    readElapsed,
    writeConcurrencyFactor,
    writeSpeed,
    writeThroughput,
    readConcurrencyFactor,
    readSpeed,
    readThroughput,
  ].join(",");
  fs.appendFileSync("/stats/subscriptions.csv", `${csv}\n`);
};

boot()
  .then(() => {
    process.exit(0);
  })
  .catch((err) => {
    console.error("ERROR!");
    console.error(err.message);
    process.exit(-1);
  });
