const parseMessage = (msg) => ({
  client: msg.client,
  topic: msg.topic,
  partition: msg.partition,
  offset: Number(msg.offset),
  payload: msg.payload,
  createdAt: new Date(msg.created_at),
  lockedUntil: new Date(msg.locked_until),
});

const parseClient = (client) => ({
  id: client.id,
  createdAt: new Date(client.created_at),
  updatedAt: new Date(client.updated_at),
});

const parseLock = (lock) => ({
  client: lock.client,
  topic: lock.topic,
  partition: lock.partition,
  offset: Number(lock.offset),
  lockedUntil: new Date(lock.locked_until),
});

module.exports = {
  reset: async (client) => {
    await client.query('DROP SCHEMA IF EXISTS "fq" CASCADE;');
    await client.query('CREATE SCHEMA IF NOT EXISTS "fq";');
  },
  create: async (client) => {
    await client.query('CREATE SCHEMA IF NOT EXISTS "fq";');

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
        "id" VARCHAR(32),
        "created_at" TIMESTAMP DEFAULT NOW() NOT NULL,
        "updated_at" TIMESTAMP DEFAULT NOW() NOT NULL,
        PRIMARY KEY ("id")
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS "fq"."locks" (
        "client" VARCHAR(32),
        "topic" VARCHAR(50),
        "partition" VARCHAR(50),
        "offset" BIGINT DEFAULT -1,
        "last_offset" BIGINT DEFAULT 0,
        "locked_until" TIMESTAMP DEFAULT NOW() - INTERVAL '1ms' NOT NULL,
        "created_at" TIMESTAMP DEFAULT NOW() NOT NULL,
        "updated_at" TIMESTAMP DEFAULT NOW() NOT NULL,
        PRIMARY KEY ("client", "topic", "partition")
      );
    `);

    // SIDE EFFECT:
    // automatically bump "updated_at" when modifying a lock
    await client.query(`
      CREATE OR REPLACE FUNCTION "fq"."before_update_locks_table"()
      RETURNS trigger 
      AS $before_update_locks_table$
      BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
      END;
      $before_update_locks_table$ LANGUAGE plpgsql;

      DROP TRIGGER IF EXISTS "fq_before_update_locks_table" ON "fq"."locks";
      CREATE TRIGGER "fq_before_update_locks_table" BEFORE UPDATE ON "fq"."locks"
      FOR EACH ROW EXECUTE PROCEDURE "fq"."before_update_locks_table"();  
    `);

    // SIDE EFFECT:
    // after appending a new message, the relative topic/partition
    // line is upserted and updated with the latest available offset
    await client.query(`
      CREATE OR REPLACE FUNCTION "fq"."on_insert_on_messages"()
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

      DROP TRIGGER IF EXISTS "fq_on_insert_on_messages" ON "fq"."messages";
      CREATE TRIGGER "fq_on_insert_on_messages" AFTER INSERT ON "fq"."messages"
      FOR EACH ROW EXECUTE PROCEDURE "fq"."on_insert_on_messages"();
    `);

    await client.query(`
      CREATE OR REPLACE FUNCTION "fq"."on_insert_on_partitions"()
      RETURNS trigger
      AS $on_insert_on_partitions$
      BEGIN
        
        INSERT INTO "fq"."locks"
        SELECT 
          "t1"."id" AS "client", 
          NEW."topic" AS "topic",
          NEW."partition" AS "partition",
          -1 AS "offset",
          0 AS "last_offset",
          NOW() AS "lock_until"
        FROM "fq"."clients" AS "t1"
        ON CONFLICT ON CONSTRAINT "locks_pkey"
        DO NOTHING;

        RETURN NEW;
      END;
      $on_insert_on_partitions$ LANGUAGE plpgsql;

      DROP TRIGGER IF EXISTS "fq_on_insert_on_partitions" ON "fq"."partitions";
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
          0 AS "last_offset",
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
    const result = await client.query(`
      WITH
      "apply_lock" AS (
        UPDATE "fq"."locks" AS "t1"
        SET "locked_until" = NOW() + INTERVAL '5m'
        FROM (
          SELECT * FROM "fq"."locks"
          WHERE "client" = '${clientId}'
            AND "topic" = '${topic}'
            AND "locked_until" < NOW()
            AND "offset" < "last_offset"
          LIMIT 1
          FOR UPDATE
        ) AS t2
        WHERE "t1"."client" = "t2"."client"
          AND "t1"."topic" = "t2"."topic"
          AND "t1"."partition" = "t2"."partition"
        RETURNING "t2".*
      )
      SELECT 
        (SELECT "client" FROM "apply_lock") AS "client",
        "t1".*,
        (SELECT "locked_until" FROM "apply_lock") AS "locked_until"
      FROM "fq"."messages" AS "t1"
      WHERE "topic" = (SELECT "topic" FROM "apply_lock")
        AND "partition" = (SELECT "partition" FROM "apply_lock")
        AND "offset" > ANY (SELECT "offset" FROM "apply_lock")
      ORDER BY "offset" ASC
      LIMIT 1;
    `);

    if (!result.rowCount) return null;

    const message = parseMessage(result.rows[0]);

    return {
      ...message,
      commit: async () => {
        const result = await client.query(`
          UPDATE "fq"."locks"
          SET "offset" = ${message.offset},
              "locked_until" = NOW(),
              "last_offset" = (
                SELECT "t2"."offset" FROM "fq"."messages" AS "t2"
                WHERE "t2"."topic" = '${message.topic}'
                  AND "partition" = '${message.partition}'
                ORDER BY "t2"."offset" DESC
                LIMIT 1
              )
          WHERE "client" = '${message.client}'
            AND "topic" = '${message.topic}'
            AND "partition" = '${message.partition}'
          RETURNING *
        `);
        return parseLock(result.rows[0]);
      },
    };
  },
};
