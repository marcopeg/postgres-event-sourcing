const parseMessage = (msg) => ({
  client: msg.client,
  topic: msg.topic,
  partition: msg.partition,
  offset: Number(msg.offset),
  payload: msg.payload,
  createdAt: new Date(msg.created_at),
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

    // SCHEMA: events
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

    // SCHEMA: partitions
    await db.query(`
      CREATE TABLE IF NOT EXISTS "fq"."partitions" (
      "topic" VARCHAR(50),
      "partition" VARCHAR(50),
      "offset" BIGINT,
      "created_at" TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
      PRIMARY KEY ("topic", "partition")
      );
    `);

    // SCHEMA: subscriptions
    await db.query(`
      CREATE TABLE IF NOT EXISTS "fq"."subscriptions" (
        "client" VARCHAR(32),
        "topic" VARCHAR(50),
        "start_at" TIMESTAMP DEFAULT NOW() NOT NULL,
        "created_at" TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        "updated_at" TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        PRIMARY KEY ("client", "topic")
      );
    `);

    // SCHEMA: locks
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
    // after upserting a subscription, all the locks should be re-upserted so to
    // keep the correct matrix of client/topic/partition locks
    await db.query(`
      CREATE OR REPLACE FUNCTION "fq"."on_insert_or_update_on_subscriptions"()
      RETURNS trigger
      AS $on_insert_or_update_on_subscriptions$
      BEGIN
        INSERT INTO "fq"."locks"
        SELECT
          NEW."client" AS "client",
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
        WHERE "t1"."topic" = NEW."topic"
        ON CONFLICT ON CONSTRAINT "locks_pkey"
        DO UPDATE
        SET "offset" = EXCLUDED."offset",
            "last_offset" = EXCLUDED."last_offset"
        ;

        RETURN NEW;
      END;
      $on_insert_or_update_on_subscriptions$ LANGUAGE plpgsql;

      DROP TRIGGER IF EXISTS "fq_on_insert_or_update_on_subscriptions" ON "fq"."subscriptions";
      CREATE TRIGGER "fq_on_insert_or_update_on_subscriptions" AFTER INSERT OR UPDATE ON "fq"."subscriptions"
      FOR EACH ROW EXECUTE PROCEDURE "fq"."on_insert_or_update_on_subscriptions"();
    `);

    // SIDE EFFECT:
    // While new events are posted, and partitions upserted, this side effect keeps in sync
    // the locks for all the active subscriptions.
    await db.query(`
      CREATE OR REPLACE FUNCTION "fq"."on_insert_on_partitions"()
      RETURNS trigger
      AS $on_insert_on_partitions$
      BEGIN

        INSERT INTO "fq"."locks"
        SELECT
          "t1"."client" AS "client",
          NEW."topic" AS "topic",
          NEW."partition" AS "partition",
          COALESCE(
            (
              SELECT "t2"."offset" - 1 AS "offset" FROM "fq"."events" AS "t2"
              WHERE "t2"."topic" = NEW."topic"
                AND "t2"."partition" = NEW."partition"
                AND "t2"."created_at" >= "t1"."start_at"
              ORDER BY "t2"."offset" ASC
              LIMIT 1
            ),
          (
              SELECT "t2"."offset" AS "offset" FROM "fq"."events" AS "t2"
              WHERE "t2"."topic" = NEW."topic"
                AND "t2"."partition" = NEW."partition"
              ORDER BY "t2"."offset" DESC
              LIMIT 1
            )
          ) AS "offset",
          0 AS "last_offset",
          NOW() AS "lock_until"
        FROM "fq"."subscriptions" AS "t1"
        WHERE "t1"."topic" = NEW."topic"
        ON CONFLICT ON CONSTRAINT "locks_pkey"
        DO UPDATE SET "last_offset" = NEW."offset";

        RETURN NEW;
      END;
      $on_insert_on_partitions$ LANGUAGE plpgsql;

      DROP TRIGGER IF EXISTS "fq_on_insert_on_partitions" ON "fq"."partitions";
      CREATE TRIGGER "fq_on_insert_on_partitions" AFTER INSERT OR UPDATE ON "fq"."partitions"
      FOR EACH ROW EXECUTE PROCEDURE "fq"."on_insert_on_partitions"();
    `);

    // FUNCTION: fq.get(client, topic)
    // Retrieve a new message for processing
    await db.query(`
      CREATE OR REPLACE FUNCTION "fq"."get"(
        PAR_client VARCHAR,
	      PAR_topic VARCHAR
      )
      RETURNS TABLE (
        "client" VARCHAR(32),
        "offset" BIGINT,
        "topic" VARCHAR(50),
        "partition" VARCHAR(50),
        "payload" JSONB,
        "created_at" TIMESTAMP WITH TIME ZONE,
        "locked_until" TIMESTAMP WITH TIME ZONE
      )
      AS $fn$
      BEGIN

        RETURN QUERY
        WITH
        "apply_lock" AS (
          UPDATE "fq"."locks" AS "t1"
          SET "locked_until" = NOW() + INTERVAL '5m'
          FROM (
            SELECT * FROM "fq"."locks" AS "t2"
            WHERE "t2"."client" = PAR_client
              AND "t2"."topic" = PAR_topic
              AND "t2"."locked_until" < NOW()
              AND "t2"."offset" < "last_offset"
            LIMIT 1
            FOR UPDATE
          ) AS t2
          WHERE "t1"."client" = "t2"."client"
            AND "t1"."topic" = "t2"."topic"
            AND "t1"."partition" = "t2"."partition"
          RETURNING "t2".*
        )
        SELECT 
          (SELECT "t3"."client" FROM "apply_lock" AS "t3") AS "client",
          "t1".*,
          (SELECT "t3"."locked_until" FROM "apply_lock" AS "t3") AS "locked_until"
        FROM "fq"."events" AS "t1"
        WHERE "t1"."topic" = (SELECT "t3"."topic" FROM "apply_lock" AS "t3")
          AND "t1"."partition" = (SELECT "t3"."partition" FROM "apply_lock" AS "t3")
          AND "t1"."offset" > ANY (SELECT "t3"."offset" FROM "apply_lock" AS "t3")
        ORDER BY "offset" ASC
        LIMIT 1;

      END;
      $fn$
      LANGUAGE plpgsql;
    `);

    // FUNCTION: fq.commit(client, topic, partition, offset)
    // Commit a message after it's been processed
    await db.query(`
      CREATE OR REPLACE FUNCTION "fq"."commit"(
        PAR_client VARCHAR,
        PAR_topic VARCHAR,
        PAR_partition VARCHAR,
        PAR_offset BIGINT
      )
      RETURNS SETOF "fq"."locks"
      AS $fn$
      BEGIN

        RETURN QUERY
        UPDATE "fq"."locks"
          SET "offset" = PAR_offset,
              "locked_until" = NOW(),
              "last_offset" = (
                SELECT "t2"."offset" FROM "fq"."events" AS "t2"
                WHERE "t2"."topic" = PAR_topic
                  AND "partition" = PAR_partition
                ORDER BY "t2"."offset" DESC
                LIMIT 1
              )
          WHERE "client" = PAR_client
            AND "topic" = PAR_topic
            AND "partition" = PAR_partition
          RETURNING *;

      END;
      $fn$
      LANGUAGE plpgsql;
    `);

    // FUNCTION: fq.subscribe(client, topic, fromStart)
    // subscribe to a topic from the two ends
    // true -> from the beginning
    // false -> from current time (default option)
    await db.query(`
      CREATE OR REPLACE FUNCTION "fq"."subscribe"(
        PAR_client VARCHAR,
        PAR_topic VARCHAR,
        PAR_startAt BOOLEAN = false
      )
      RETURNS SETOF "fq"."subscriptions"
      AS $fn$
      DECLARE
        VAR_r RECORD;
      BEGIN

        IF PAR_startAt = true THEN
          SELECT "created_at" as "start_at" INTO VAR_r
          FROM "fq"."events"
          WHERE "topic" = PAR_topic
          ORDER BY "created_at" ASC
          LIMIT 1;
        ELSE
          SELECT NOW()::timestamp with time zone AS start_at INTO VAR_r;
        END IF;

        RETURN QUERY
        INSERT INTO "fq"."subscriptions"
        ("client", "topic", "start_at") VALUES
        (PAR_client, PAR_topic, VAR_r.start_at)
        ON CONFLICT ON CONSTRAINT "subscriptions_pkey"
        DO UPDATE SET "updated_at" = NOW(), "start_at" = EXCLUDED."start_at"
        RETURNING *;

      END;
      $fn$
      LANGUAGE plpgsql;
    `);

    // FUNCTION: fq.subscribe(client, topic, startAt)
    // subscribe to a topic from a specific point in time
    await db.query(`
      CREATE OR REPLACE FUNCTION "fq"."subscribeAt"(
        PAR_client VARCHAR,
        PAR_topic VARCHAR,
        PAR_startAt TIMESTAMP WITH TIME ZONE
      )
      RETURNS SETOF "fq"."subscriptions"
      AS $fn$
      BEGIN

        RETURN QUERY
        INSERT INTO "fq"."subscriptions"
        ("client", "topic", "start_at") VALUES 
        (PAR_client, PAR_topic, PAR_startAt)
        ON CONFLICT ON CONSTRAINT "subscriptions_pkey"
        DO UPDATE SET "updated_at" = NOW(), "start_at" = EXCLUDED."start_at"
        RETURNING *;

      END;
      $fn$
      LANGUAGE plpgsql;
    `);

    // FUNCTION: fq.put(payload, topic, partition)
    // subscribe to a topic from a specific point in time
    await db.query(`
      CREATE OR REPLACE FUNCTION "fq"."put"(
        PAR_payload JSONB,
        PAR_topic VARCHAR = '*',
        PAR_partition VARCHAR = '*'
      )
      RETURNS SETOF "fq"."events"
      AS $fn$
      BEGIN

        RETURN QUERY
        INSERT INTO "fq"."events"
        ("topic", "partition", "payload") VALUES
        (PAR_topic, PAR_partition, PAR_payload)
        RETURNING *;

      END;
      $fn$
      LANGUAGE plpgsql;
    `);
  },
  put: async (db, payload, topic = "*", partition = "*") => {
    const payloadStr = JSON.stringify(payload);
    const sql = `SELECT * FROM "fq"."put"('${payloadStr}', '${topic}', '${partition}')`;
    const result = await db.query(sql);
    return parseMessage(result.rows[0]);
  },
  subscribe: async (db, client = "*", topic = "*", fromStart = false) => {
    let sql = "";
    switch (fromStart) {
      case true:
        sql = `SELECT * FROM "fq"."subscribe"('${client}', '${topic}', true)`;
        break;
      case false:
        sql = `SELECT * FROM "fq"."subscribe"('${client}', '${topic}')`;
        break;
      default:
        sql = `SELECT * FROM "fq"."subscribeAt"('${client}', '${topic}', '${fromStart.toISOString()}')`;
    }
    const result = await db.query(sql);
    return parseClient(result.rows[0]);
  },
  get: async (db, client = "*", topic = "*") => {
    const getSql = `SELECT * FROM "fq"."get"('${client}', '${topic}')`;

    const result = await db.query(getSql);
    if (!result.rowCount) return null;

    return parseMessage(result.rows[0]);
  },
  commit: async (db, message) => {
    const { client, topic, partition, offset } = message;
    const commitSql = `SELECT * FROM "fq"."commit"('${client}', '${topic}', '${partition}', ${offset})`;
    const result = await db.query(commitSql);
    return parseLock(result.rows[0]);
  },
};
