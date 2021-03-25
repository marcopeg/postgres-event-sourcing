const { Client } = require("pg");
const schema = require("./schema.partitions");

const batchSerial = process.env.BATCH_SERIAL || 10;
const batchParallel = process.env.BATCH_PARALLEL || 100;

const boot = async () => {
  console.log("Connecting...");
  const client = new Client({ connectionString: process.env.PGSTRING });
  await client.connect();

  try {
    await schema.reset(client);
  } catch (err) {
    console.error(`Errors while upserting the schema: ${err.message}`);
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
