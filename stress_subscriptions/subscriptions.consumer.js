const { Client } = require("pg");
const schema = require("../src/schema.subscriptions");

const clientId = process.env.CLIENT_ID || process.env.HOSTNAME || "*";
const batchParallel = process.env.BATCH_PARALLEL || 10;

const boot = async () => {
  console.log("Connecting...");
  const client = new Client({ connectionString: process.env.PGSTRING });
  await client.connect();

  await schema.subscribe(client, clientId, "*", true);
  // console.log(clientId, clientResult);

  let iterations = 0;
  let keepWorking = true;
  while (keepWorking) {
    // Get parallel messages
    const messages = [];
    for (let j = 0; j < batchParallel; j++) {
      messages.push(schema.get(client, clientId));
    }

    // Committing the messages
    const results = await Promise.all(messages);
    for (const result of results) {
      if (result) {
        console.log(
          `[consumer][${clientId}][iteration:${iterations + 1}] ${
            result.partition
          }:${result.offset}`
        );
        try {
          await client.query(`
          INSERT INTO "fq"."results" VALUES (
            '${clientId}',
            ${result.offset},
            '${result.topic}',
            '${result.partition}',
            '${JSON.stringify(result.payload)}',
            '${result.createdAt.toISOString()}'
          )
        `);
        } catch (err) {
          console.log(err.message);
          console.log(
            clientId,
            result.client,
            "--",
            result.topic,
            result.partition,
            result.offset
          );
        }
        await schema.commit(client, result);
        // await new Promise((r) => setTimeout(r, 1000));
      }
    }

    // At least one non-null item to keep working
    keepWorking = results.find((item) => item !== null);
    iterations += 1;
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
