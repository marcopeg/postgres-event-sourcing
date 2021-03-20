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
  get: async (client, offset = -1, limit = 1) => {
    const result = await client.query(`
      SELECT * FROM "fq"."messages"
      WHERE "offset" > ${offset}
      ORDER BY "offset" ASC
      LIMIT ${limit};
    `);
    return parseMessage(result.rows[0]);
  },
};
