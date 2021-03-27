const parseMessage = (msg) => ({
  offset: Number(msg.offset),
  createdAt: new Date(msg.created_at),
  payload: msg.payload,
});

module.exports = {
  reset: async (db) => {
    await db.query('DROP SCHEMA IF EXISTS "fq" CASCADE;');
    await db.query('CREATE SCHEMA IF NOT EXISTS "fq";');
  },
  create: async (db) => {
    await db.query(`
      CREATE TABLE IF NOT EXISTS "fq"."events" (
        "offset" BIGSERIAL,
        "payload" JSONB DEFAULT '{}',
        "created_at" TIMESTAMP DEFAULT NOW() NOT NULL,
        PRIMARY KEY ("offset")
      );
    `);
  },
  put: async (db, payload) => {
    const result = await db.query(`
      INSERT INTO "fq"."events" 
      ("payload") VALUES
      ('${JSON.stringify(payload)}')
      RETURNING *
    `);
    return parseMessage(result.rows[0]);
  },
  get: async (db, offset = -1, limit = 1) => {
    const result = await db.query(`
      SELECT * FROM "fq"."events"
      WHERE "offset" > ${offset}
      ORDER BY "offset" ASC
      LIMIT ${limit};
    `);
    return parseMessage(result.rows[0]);
  },
};
