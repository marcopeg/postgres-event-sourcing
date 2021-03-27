const fs = require("fs");
const { Client } = require("pg");
const schema = require("../src/schema.partitions");

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
    FROM "fq"."messages";
  `);

  const results = await db.query(`
    SELECT
      COUNT(*) AS "count",
      MAX("processed_at") - MIN("processed_at") AS "elapsed" 
    FROM "fq"."results";
  `);

  const writeSpeed = input.rows[0].elapsed.milliseconds / input.rows[0].count;
  const writeThroughput = 1000 / writeSpeed;

  const readSpeed =
    results.rows[0].elapsed.milliseconds / results.rows[0].count;
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
    `${input.rows[0].count} messages were pushed in ${input.rows[0].elapsed.milliseconds}ms`
  );
  console.log(`> ${Math.round(writeThroughput)} messages/s`);
  console.log("");
  console.log(
    `${results.rows[0].count} messages were processed in ${results.rows[0].elapsed.milliseconds}ms`
  );
  console.log(`> ${Math.round(readThroughput)} messages/s`);
  console.log("");

  if (!fs.existsSync("/stats/partitions.csv")) {
    const headers = [
      "producer_replicas",
      "producer_batch_serial",
      "producer_batch_parallel",
      "producer_max_partitions",
      "consumer1_replicas",
      "consumer1_batch_parallel",
      "consumer2_replicas",
      "consumer2_batch_parallel",
      "messages_count",
      "messages_elapsed",
      "results_count",
      "results_elapsed",
      "write_concurrency_factor",
      "write_speed",
      "write_throughput",
      "read_concurrency_factor",
      "read_speed",
      "read_throughput",
    ].join(",");
    fs.appendFileSync("/stats/partitions.csv", `${headers}\n`);
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
    input.rows[0].elapsed.milliseconds,
    results.rows[0].count,
    results.rows[0].elapsed.milliseconds,
    writeConcurrencyFactor,
    writeSpeed,
    writeThroughput,
    readConcurrencyFactor,
    readSpeed,
    readThroughput,
  ].join(",");
  fs.appendFileSync("/stats/partitions.csv", `${csv}\n`);
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
