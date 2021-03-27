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
  reset: async (db) => {
    await db.query('DROP SCHEMA IF EXISTS "fq" CASCADE;');
    await db.query('CREATE SCHEMA IF NOT EXISTS "fq";');
  },
  create: async (db) => {
    await db.query('CREATE SCHEMA IF NOT EXISTS "fq";');

    await db.query(`
      CREATE TABLE IF NOT EXISTS "fq"."events" (
      "offset" BIGSERIAL,
      "topic" VARCHAR(50) DEFAULT '*',
      "partition" VARCHAR(50) DEFAULT '*',
      "payload" JSONB DEFAULT '{}',
      "created_at" TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
      PRIMARY KEY ("offset")
      );
    `);

    await db.query(`
      CREATE TABLE IF NOT EXISTS "fq"."partitions" (
      "topic" VARCHAR(50),
      "partition" VARCHAR(50),
      "offset" BIGINT,
      "created_at" TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
      PRIMARY KEY ("topic", "partition")
      );
    `);

    await db.query(`
      CREATE TABLE IF NOT EXISTS "fq"."clients" (
        "id" VARCHAR(32),
        "start_at" TIMESTAMP DEFAULT NOW() NOT NULL,
        "created_at" TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        "updated_at" TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        PRIMARY KEY ("id")
      );
    `);

    await db.query(`
      CREATE TABLE IF NOT EXISTS "fq"."locks" (
        "client" VARCHAR(32),
        "topic" VARCHAR(50),
        "partition" VARCHAR(50),
        "offset" BIGINT DEFAULT -1,
        "last_offset" BIGINT DEFAULT 0,
        "locked_until" TIMESTAMP WITH TIME ZONE DEFAULT NOW() - INTERVAL '1ms' NOT NULL,
        "created_at" TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        "updated_at" TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        PRIMARY KEY ("client", "topic", "partition")
      );
    `);

    // SIDE EFFECT:
    // automatically bump "updated_at" when modifying a lock
    await db.query(`
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
    await db.query(`
      CREATE OR REPLACE FUNCTION "fq"."on_insert_on_events"()
      RETURNS trigger 
      AS $on_insert_on_events$
      BEGIN

        INSERT INTO "fq"."partitions"
        ("topic", "partition", "offset") VALUES
        (NEW."topic", NEW."partition", NEW."offset")
        ON CONFLICT ON CONSTRAINT "partitions_pkey"
        DO UPDATE SET "offset" = NEW."offset";

        RETURN NEW;
      END;
      $on_insert_on_events$ LANGUAGE plpgsql;

      DROP TRIGGER IF EXISTS "fq_on_insert_on_events" ON "fq"."events";
      CREATE TRIGGER "fq_on_insert_on_events" AFTER INSERT ON "fq"."events"
      FOR EACH ROW EXECUTE PROCEDURE "fq"."on_insert_on_events"();
    `);

    // SIDE EFFECT:
    // after upserting a client, all the locks should be re-upserted so to
    // keep the correct matrix of client/topic/partition locks
    await db.query(`
      CREATE OR REPLACE FUNCTION "fq"."on_insert_or_update_on_clients"()
      RETURNS trigger
      AS $on_insert_or_update_on_clients$
      BEGIN

        INSERT INTO "fq"."locks"
        SELECT
          NEW."id" AS "client", 
          "t1"."topic" AS "topic",
          "t1"."partition" AS "partition",
          COALESCE(
            (
              SELECT "t2"."offset" - 1 AS "offset" FROM "fq"."events" AS "t2"
              WHERE "t2"."topic" = "t1"."topic"
                AND "t2"."partition" = "t1"."partition"
                AND "t2"."created_at" >= NEW."start_at"
              ORDER BY "t2"."offset" ASC
              LIMIT 1
            ),
          (
              SELECT "t2"."offset" AS "offset" FROM "fq"."events" AS "t2"
              WHERE "t2"."topic" = "t1"."topic"
                AND "t2"."partition" = "t1"."partition"
              ORDER BY "t2"."offset" DESC
              LIMIT 1
            )	
          ) AS "offset",
          "t1"."offset" AS "last_offset",
          NOW() AS "locked_until"
        FROM "fq"."partitions" AS "t1"
        ON CONFLICT ON CONSTRAINT "locks_pkey"
        DO UPDATE
        SET "offset" = EXCLUDED."offset",
            "last_offset" = EXCLUDED."last_offset"
        ;

        RETURN NEW;
      END;
      $on_insert_or_update_on_clients$ LANGUAGE plpgsql;

      DROP TRIGGER IF EXISTS "fq_on_insert_or_update_on_clients" ON "fq"."clients";
      CREATE TRIGGER "fq_on_insert_or_update_on_clients" AFTER INSERT ON "fq"."clients"
      FOR EACH ROW EXECUTE PROCEDURE "fq"."on_insert_or_update_on_clients"();
    `);

    await db.query(`
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
        DO UPDATE SET "last_offset" = NEW."offset";

        RETURN NEW;
      END;
      $on_insert_on_partitions$ LANGUAGE plpgsql;

      DROP TRIGGER IF EXISTS "fq_on_insert_on_partitions" ON "fq"."partitions";
      CREATE TRIGGER "fq_on_insert_on_partitions" AFTER INSERT OR UPDATE ON "fq"."partitions"
      FOR EACH ROW EXECUTE PROCEDURE "fq"."on_insert_on_partitions"();
    `);
  },
  put: async (db, payload, topic = "*", partition = "*") => {
    const result = await db.query(`
      INSERT INTO "fq"."events"
      ("topic", "partition", "payload") VALUES
      ('${topic}', '${partition}', '${JSON.stringify(payload)}')
      RETURNING *
    `);
    return parseMessage(result.rows[0]);
  },
  registerClient: async (db, client = "*", fromStart = false) => {
    let startAt = "";
    switch (fromStart) {
      case true:
        startAt = `'0001-01-01'`;
        break;
      case false:
        startAt = "NOW()";
        break;
      default:
        startAt = `'${fromStart.toISOString()}'`;
    }
    const registerSql = `
      INSERT INTO "fq"."clients"
      ("id", "start_at") VALUES ('${client}', ${startAt})
      ON CONFLICT ON CONSTRAINT "clients_pkey"
      DO UPDATE SET "updated_at" = NOW(), "start_at" = EXCLUDED."start_at"
      RETURNING *
    `;
    const result = await db.query(registerSql);
    return parseClient(result.rows[0]);
  },
  get: async (db, client = "*", topic = "*") => {
    const result = await db.query(`
      WITH
      "apply_lock" AS (
        UPDATE "fq"."locks" AS "t1"
        SET "locked_until" = NOW() + INTERVAL '5m'
        FROM (
          SELECT * FROM "fq"."locks"
          WHERE "client" = '${client}'
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
      FROM "fq"."events" AS "t1"
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
        const result = await db.query(`
          UPDATE "fq"."locks"
          SET "offset" = ${message.offset},
              "locked_until" = NOW(),
              "last_offset" = (
                SELECT "t2"."offset" FROM "fq"."events" AS "t2"
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
