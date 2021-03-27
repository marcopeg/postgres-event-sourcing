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
        "payload" JSONB DEFAULT '{}'
        PRIMARY KEY ("offset")
      );
    `);

    await db.query(`
      CREATE TABLE IF NOT EXISTS "fq"."clients" (
        "id" VARCHAR(10),
        "offset" BIGINT DEFAULT -1,
        PRIMARY KEY ("id")
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
  get: async (db, client = "*") => {
    const result = await db.query(`
      SELECT * FROM "fq"."events"
      WHERE "offset" > (
        SELECT
          CASE count(*)
            WHEN 0 THEN -1 
            ELSE MAX("offset") 
          END 
          AS "offset"
        FROM fq.clients 
        WHERE id = '${client}'
        LIMIT 1
      )
      ORDER BY "offset" ASC
      LIMIT 1;
    `);

    const message = parseMessage(result.rows[0]);

    return {
      ...message,
      commit: async () => {
        const result = await db.query(`
           INSERT INTO "fq"."clients"
          ("id", "offset")
          VALUES
          ('${client}', ${message.offset})
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
