const parseMessage = (msg) => ({
  offset: Number(msg.offset),
  createdAt: new Date(msg.created_at),
  topic: msg.topic,
  partition: msg.partition,
  payload: msg.payload,
});

const parseClient = (client) => ({
  id: client.id,
  createdAt: new Date(client.created_at),
  updatedAt: new Date(client.updated_at),
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
      "partition" VARCHAR(50),
      "payload" JSONB DEFAULT '{}',
      "created_at" TIMESTAMP DEFAULT NOW() NOT NULL,
      PRIMARY KEY ("offset")
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS "fq"."partitions" (
      "topic" VARCHAR(50),
      "partition" VARCHAR(50),
      "offset" BIGINT,
      "created_at" TIMESTAMP DEFAULT NOW() NOT NULL,
      PRIMARY KEY ("topic", "partition")
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS "fq"."clients" (
        "id" VARCHAR(10),
        "created_at" TIMESTAMP DEFAULT NOW() NOT NULL,
        "updated_at" TIMESTAMP DEFAULT NOW() NOT NULL,
        PRIMARY KEY ("id")
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS "fq"."locks" (
        "client" VARCHAR(10),
        "topic" VARCHAR(50),
        "partition" VARCHAR(50),
        "offset" BIGINT DEFAULT -1,
        "locked_until" TIMESTAMP DEFAULT NOW() - INTERVAL '1ms' NOT NULL,
        PRIMARY KEY ("client", "topic", "partition")
      );
    `);

    // SIDE EFFECT:
    // after appending a new message, the relative topic/partition
    // line is upserted and updated with the latest available offset
    await client.query(`
      CREATE FUNCTION "fq"."on_insert_on_messages"()
      RETURNS trigger 
      AS $on_insert_on_messages$
      BEGIN

        INSERT INTO "fq"."partitions"
        ("topic", "partition", "offset") VALUES
        (NEW."topic", NEW."partition", NEW."offset")
        ON CONFLICT ON CONSTRAINT "partitions_pkey"
        DO UPDATE SET "offset" = NEW."offset";

        RETURN NEW;
      END;
      $on_insert_on_messages$ LANGUAGE plpgsql;

      CREATE TRIGGER "fq_on_insert_on_messages" AFTER INSERT ON "fq"."messages"
      FOR EACH ROW EXECUTE PROCEDURE "fq"."on_insert_on_messages"();
    `);

    await client.query(`
      CREATE FUNCTION "fq"."on_insert_on_partitions"()
      RETURNS trigger
      AS $on_insert_on_partitions$
      BEGIN
        
        INSERT INTO "fq"."locks"
        SELECT 
          "t1"."id" AS "client", 
          NEW."topic" AS "topic",
          NEW."partition" AS "partition",
          -1 AS "offset",
          NOW() AS "lock_until"
        FROM "fq"."clients" AS "t1";

        RETURN NEW;
      END;
      $on_insert_on_partitions$ LANGUAGE plpgsql;

      CREATE TRIGGER "fq_on_insert_on_partitions" AFTER INSERT OR UPDATE ON "fq"."partitions"
      FOR EACH ROW EXECUTE PROCEDURE "fq"."on_insert_on_partitions"();
    `);
  },
  put: async (client, payload, topic = "*", partition = "*") => {
    const result = await client.query(`
      INSERT INTO "fq"."messages"
      ("topic", "partition", "payload") VALUES
      ('${topic}', '${partition}', '${JSON.stringify(payload)}')
      RETURNING *
    `);
    return parseMessage(result.rows[0]);
  },
  registerClient: async (client, clientId = "*", fromStart = false) => {
    const result = await client.query(`
      WITH
      "upsert_client" AS (
        INSERT INTO "fq"."clients"
        ("id") VALUES ('${clientId}')
        ON CONFLICT ON CONSTRAINT "clients_pkey"
        DO UPDATE SET "updated_at" = NOW()
        RETURNING *
      ),
      "upsert_locks" AS (
        INSERT INTO "fq"."locks"
        SELECT 
          "t2"."id" AS "client",
          "t1"."topic" AS "topic",
          "t1"."partition" AS "partition",
          ${fromStart ? '-1 AS "offset"' : '"t1"."offset"'},
          NOW() AS "locked_until"
        FROM "fq"."partitions" AS "t1"
        LEFT JOIN "upsert_client" AS "t2" ON 1 = 1
        ON CONFLICT ON CONSTRAINT "locks_pkey"
        DO NOTHING
      )
      SELECT * FROM "upsert_client"
    `);
    return parseClient(result.rows[0]);
  },
  get: async (client, clientId = "*", topic = "*") => {
    const foo = await client.query(`
      SELECT 
    `);
    /*
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
    */
  },
};
