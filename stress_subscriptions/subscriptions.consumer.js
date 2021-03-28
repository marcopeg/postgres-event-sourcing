const { Client } = require("pg");
const schema = require("../src/schema.subscriptions");

const clientId = process.env.CLIENT_ID || process.env.HOSTNAME || "*";
const batchParallel = process.env.BATCH_PARALLEL || 10;

const boot = async () => {
  console.log(`[${clientId}] Connecting...`);
  const client = new Client({ connectionString: process.env.PGSTRING });
  await client.connect();

  console.log(`[${clientId}] Subscribing and awaiting 3s to start...`);
  await schema.subscribe(client, clientId, `*`, true);
  await new Promise((r) => setTimeout(r, 3000));
  // console.log(clientId, clientResult);

  console.log(`[${clientId}] Start working on it!`);
  let iterations = 0;
  let keepWorking = true;

  // Output informations once a second:
  let __lastResult = null;
  let __logInterval = setInterval(() => {
    console.log(
      `[consumer][${clientId}][iteration:${iterations + 1}] ${
        __lastResult.partition
      }:${__lastResult.offset}`
    );
  }, 5000);

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
        __lastResult = result;
        // console.log(
        //   `[consumer][${clientId}][iteration:${iterations + 1}] ${
        //     result.partition
        //   }:${result.offset}`
        // );
        try {
          await client.query(`
          INSERT INTO "fq"."results" (
            "client",
            "offset",
            "topic",
            "partition",
            "payload",
            "created_at"
          ) VALUES (
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

  clearInterval(__logInterval);
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
