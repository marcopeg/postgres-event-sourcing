const parseMessage = (msg) => ({
  offset: Number(msg.offset),
  createdAt: new Date(msg.created_at),
  topic: msg.topic,
  payload: msg.payload,
});

const parseClient = (client) => ({
  id: client.id,
  topic: client.topic,
  offset: Number(client.offset),
});

module.exports = {
  reset: async (client) => {
    await client.query('DROP SCHEMA IF EXISTS "fq" CASCADE;');
    await client.query('CREATE SCHEMA IF NOT EXISTS "fq";');
  },
  create: async (client) => {
    await client.query(`
      CREATE TABLE IF NOT EXISTS "fq"."messages" (
      "offset" BIGSERIAL,
      "topic" VARCHAR(50),
      "payload" JSONB DEFAULT '{}',
      "created_at" TIMESTAMP DEFAULT NOW() NOT NULL,
      PRIMARY KEY ("offset")
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS "fq"."clients" (
        "client_id" VARCHAR(10),
        "topic" VARCHAR(50),
        "offset" BIGINT DEFAULT -1,
        "locked_until" TIMESTAMP DEFAULT NOW() - INTERVAL '1ms' NOT NULL,
        PRIMARY KEY ("client_id", "topic")
      );
    `);
  },
  registerClient: async (client, clientId = "*", topic = "*") => {
    const result = await client.query(`
      INSERT INTO "fq"."clients"
      ("client_id", "topic") VALUES ('${clientId}', '${topic}')
      RETURNING *
    `);
    return parseClient(result.rows[0]);
  },
  put: async (client, topic = "*", payload) => {
    const result = await client.query(`
      INSERT INTO "fq"."messages"
      ("topic", "payload") VALUES
      ('${topic}', '${JSON.stringify(payload)}')
      RETURNING *
    `);
    return parseMessage(result.rows[0]);
  },
  get: async (client, clientId = "*", topic = "*") => {
    const result = await client.query(`
      UPDATE "fq"."clients" AS "t3"
      SET "locked_until" = NOW() + INTERVAL '5m'
      FROM (
        SELECT "t2".*, "t1".* FROM "fq"."messages" AS "t1"
        INNER JOIN (
          SELECT 
          '${clientId}' AS "client_id"
        ) AS "t2"
        ON "t1"."offset" > 0
        WHERE "t1"."topic" = '${topic}'
        AND "t1"."offset" > (
          SELECT "offset" FROM "fq"."clients"
          WHERE "client_id" = '${clientId}'
          AND "topic" = '${topic}'
          AND "locked_until" < NOW()
          LIMIT 1
          FOR UPDATE
        )
        ORDER BY "t1"."offset" ASC
        LIMIT 1
      ) AS "messages"
      WHERE "t3"."client_id" = '${clientId}'
        AND "t3"."topic" = '${topic}'
      RETURNING *
    `);

    if (!result.rowCount) return null;

    const message = parseMessage(result.rows[0]);

    return {
      ...message,
      commit: async () => {
        const result = await client.query(`
          UPDATE "fq"."clients"
          SET "offset" = ${message.offset},
              "locked_until" = NOW() - INTERVAL '1ms'
          WHERE "client_id" = '${clientId}'
            AND "topic" = '${topic}'
          RETURNING *
        `);
        return result.rows.map(parseClient).shift();
      },
    };
  },
};
