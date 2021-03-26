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
      "topic" VARCHAR(50) DEFAULT '*',
      "partition" VARCHAR(50) DEFAULT '*',
      "payload" JSONB DEFAULT '{}',
      "created_at" TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
      PRIMARY KEY ("offset")
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS "fq"."partitions" (
      "topic" VARCHAR(50),
      "partition" VARCHAR(50),
      "offset" BIGINT,
      "created_at" TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
      PRIMARY KEY ("topic", "partition")
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS "fq"."clients" (
        "id" VARCHAR(32),
        "created_at" TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        "updated_at" TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        PRIMARY KEY ("id")
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS "fq"."subscriptions" (
        "client" VARCHAR(32),
        "topic" VARCHAR(50),
        "start_at" TIMESTAMP DEFAULT NOW() NOT NULL,
        "created_at" TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        "updated_at" TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        PRIMARY KEY ("client", "topic")
      );
    `);

    await client.query(`
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

    // SIDE EFFECT:
    // after upserting a subscription, all the locks should be re-upserted so to
    // keep the correct matrix of client/topic/partition locks
    await client.query(`
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
              SELECT "t2"."offset" - 1 AS "offset" FROM "fq"."messages" AS "t2"
              WHERE "t2"."topic" = "t1"."topic"
                AND "t2"."partition" = "t1"."partition"
                AND "t2"."created_at" >= NEW."start_at"
              ORDER BY "t2"."offset" ASC
              LIMIT 1
            ),
          (
              SELECT "t2"."offset" AS "offset" FROM "fq"."messages" AS "t2"
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
      CREATE TRIGGER "fq_on_insert_or_update_on_subscriptions" AFTER INSERT ON "fq"."subscriptions"
      FOR EACH ROW EXECUTE PROCEDURE "fq"."on_insert_or_update_on_subscriptions"();
    `);

    // SIDE EFFECT:
    // While new messages are posted, and partitions upserted, this side effect keeps in sync
    // the locks for all the active subscriptions.
    await client.query(`
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
              SELECT "t2"."offset" - 1 AS "offset" FROM "fq"."messages" AS "t2"
              WHERE "t2"."topic" = NEW."topic"
                AND "t2"."partition" = NEW."partition"
                AND "t2"."created_at" >= "t1"."start_at"
              ORDER BY "t2"."offset" ASC
              LIMIT 1
            ),
          (
              SELECT "t2"."offset" AS "offset" FROM "fq"."messages" AS "t2"
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
  registerClient: async (client, clientId = "*") => {
    const registerSql = `
      INSERT INTO "fq"."clients" VALUES ('${clientId}')
      ON CONFLICT ON CONSTRAINT "clients_pkey"
      DO UPDATE SET "updated_at" = NOW()
      RETURNING *
    `;
    const result = await client.query(registerSql);
    return parseClient(result.rows[0]);
  },
  registerSubscription: async (
    client,
    clientId = "*",
    topic = "*",
    fromStart = false
  ) => {
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
      INSERT INTO "fq"."subscriptions"
      ("client", "topic", "start_at") VALUES 
      ('${clientId}', '${topic}', ${startAt})
      ON CONFLICT ON CONSTRAINT "subscriptions_pkey"
      DO UPDATE SET "updated_at" = NOW(), "start_at" = EXCLUDED."start_at"
      RETURNING *
    `;
    const result = await client.query(registerSql);
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
