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
  reset: async (db) => {
    await db.query('DROP SCHEMA IF EXISTS "fq" CASCADE;');
    await db.query('CREATE SCHEMA IF NOT EXISTS "fq";');
  },
  create: async (db) => {
    await db.query(`
        CREATE TABLE IF NOT EXISTS "fq"."messages" (
          "offset" BIGSERIAL,
          "topic" VARCHAR(50),
          "payload" JSONB DEFAULT '{}',
          "created_at" TIMESTAMP DEFAULT NOW() NOT NULL,
          PRIMARY KEY ("offset")
        );
      `);

    await db.query(`
        CREATE TABLE IF NOT EXISTS "fq"."clients" (
          "id" VARCHAR(10),
          "topic" VARCHAR(50),
          "offset" BIGINT DEFAULT -1,
          PRIMARY KEY ("id", "topic")
        );
      `);
  },
  put: async (db, topic = "*", payload) => {
    const result = await db.query(`
        INSERT INTO "fq"."messages"
        ("topic", "payload") VALUES
        ('${topic}', '${JSON.stringify(payload)}')
        RETURNING *
      `);
    return parseMessage(result.rows[0]);
  },
  get: async (db, client = "*", topic = "*") => {
    const result = await db.query(`
        SELECT * FROM "fq"."messages"
        WHERE "topic" = '${topic}' 
          AND "offset" > (
          SELECT
            CASE count(*)
              WHEN 0 THEN -1 
              ELSE MAX("offset") 
            END 
            AS "offset"
          FROM fq.clients 
          WHERE id = '${client}'
            AND topic = '${topic}'
          LIMIT 1
        )
        ORDER BY "offset" ASC
        LIMIT 1;
      `);

    if (!result.rowCount) return null;

    const message = parseMessage(result.rows[0]);

    return {
      ...message,
      commit: async () => {
        const result = await db.query(`
             INSERT INTO "fq"."clients"
            ("id", "topic", "offset")
            VALUES
            ('${client}', '${topic}', ${message.offset})
            RETURNING *
          `);
        return result.rows.map(parseClient).shift();
      },
    };
  },
};
