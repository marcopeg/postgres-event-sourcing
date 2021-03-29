const { Client } = require("pg");
const schema = require("../src/schema.partitions");

const boot = async () => {
  console.log("Connecting...");
  const client = new Client({ connectionString: process.env.PGSTRING });
  await client.connect();

  try {
    await schema.reset(client);
    await schema.create(client);
  } catch (err) {
    console.error(`Errors while upserting the schema: ${err.message}`);
  }

  // Results table
  try {
    await client.query(`
      DROP TABLE IF EXISTS "fq"."results";
    `);
    await client.query(`
      CREATE TABLE IF NOT EXISTS "fq"."results" (
      "client" VARCHAR(32),
      "offset" BIGINT,
      "topic" VARCHAR(50),
      "partition" VARCHAR(50),
      "payload" JSONB DEFAULT '{}',
      "created_at" TIMESTAMP DEFAULT NOW() NOT NULL,
      "processed_at" TIMESTAMP DEFAULT NOW() NOT NULL,
      PRIMARY KEY ("client", "offset", "topic", "partition")
      );
    `);
  } catch (err) {
    console.error("Error while creating the results table", err.message);
  }
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
