const { Client } = require("pg");
const schema = require("./schema.partitions");

const batchSerial = process.env.BATCH_SERIAL || 10;
const batchParallel = process.env.BATCH_PARALLEL || 100;

const boot = async () => {
  console.log("Connecting...");
  const client = new Client({ connectionString: process.env.PGSTRING });
  await client.connect();

  try {
    await schema.create(client);
  } catch (err) {
    console.error(`Errors while upserting the schema: ${err.message}`);
  }

  for (let i = 0; i < batchSerial; i++) {
    console.log(`Runing batch ${i + 1}/${batchSerial}...`);
    const promises = [];
    for (let j = 0; j < batchParallel; j++) {
      const randomPartition = Math.floor(Math.random() * 9);
      // const randomPartition = 1;
      promises.push(schema.put(client, {}, "*", `p${randomPartition}`));
    }
    try {
      await Promise.all(promises);
    } catch (err) {
      console.error(`[batch ${i + 1}/${batchSerial}] error: ${err.message}`);
    }
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
