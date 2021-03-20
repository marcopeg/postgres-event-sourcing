const parseMessage = (msg) => ({
  offset: Number(msg.offset),
  createdAt: new Date(msg.created_at),
  payload: msg.payload,
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
        "payload" JSONB DEFAULT '{}',
        "created_at" TIMESTAMP DEFAULT NOW() NOT NULL,
        PRIMARY KEY ("offset")
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS "fq"."clients" (
        "id" VARCHAR(10),
        "offset" BIGINT DEFAULT -1,
        PRIMARY KEY ("id")
      );
    `);
  },
  put: async (client, payload) => {
    const result = await client.query(`
      INSERT INTO "fq"."messages"
      ("payload") VALUES
      ('${JSON.stringify(payload)}')
      RETURNING *
    `);
    return parseMessage(result.rows[0]);
  },
  get: async (client, clientId = "*") => {
    const result = await client.query(`
      SELECT * FROM "fq"."messages"
      WHERE "offset" > (
        SELECT
          CASE count(*)
            WHEN 0 THEN -1 
            ELSE MAX("offset") 
          END 
          AS "offset"
        FROM fq.clients 
        WHERE id = '${clientId}'
        LIMIT 1
      )
      ORDER BY "offset" ASC
      LIMIT 1;
    `);

    const message = parseMessage(result.rows[0]);

    return {
      ...message,
      commit: async () => {
        const result = await client.query(`
           INSERT INTO "fq"."clients"
          ("id", "offset")
          VALUES
          ('${clientId}', ${message.offset})
          RETURNING *
        `);
        return result.rows
          .map((item) => ({
            id: item.id,
            offset: Number(item.offset),
          }))
          .shift();
      },
    };
  },
};
