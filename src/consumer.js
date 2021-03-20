const parseClient = (client) => ({
  name: client.client_id,
  topic: client.topic,
  offset: Number(client.offset),
});

const parseMessage = (msg) => ({
  offset: Number(msg.offset),
  createdAt: new Date(msg.created_at),
  topic: msg.topic,
  payload: msg.payload,
});

class Consumer {
  constructor(pool, config) {
    this.pool = pool;
    this.name = config.name;
    this.topic = config.topic || "*";
    this.dataDelay = config.dataDelay || 100;
    this.dataSleep = config.dataSleep || 100;

    this.isActive = false;
    this.isStopping = false;
    this.timer = null;
  }

  async start(handler) {
    this.isActive = true;
    clearTimeout(this.timer);
    await this.registerClient();
    return this.loop(handler);
  }

  async stop() {
    this.isActive = false;
    clearTimeout(this.timer);
    // Should make very sure the process has stopped
    return new Promise((resolve) => setTimeout(resolve, 30));
  }

  async registerClient() {
    const result = await this.pool.query(`
      INSERT INTO "fq"."clients"
      ("client_id", "topic") VALUES ('${this.name}', '${this.topic}')
      ON CONFLICT ON CONSTRAINT "clients_pkey"
      DO UPDATE
      SET "client_id" = EXCLUDED."client_id"
      RETURNING *
    `);

    return parseClient(result.rows[0]);
  }

  async pollData() {
    try {
      const result = await this.pool.query(`
        UPDATE "fq"."clients" AS "t3"
        SET "locked_until" = NOW() + INTERVAL '5m'
        FROM (
          SELECT "t2".*, "t1".* FROM "fq"."messages" AS "t1"
          INNER JOIN (
            SELECT 
            '${this.name}' AS "client_id"
          ) AS "t2"
          ON "t1"."offset" > 0
          WHERE "t1"."topic" = '${this.topic}'
          AND "t1"."offset" > (
            SELECT "offset" FROM "fq"."clients"
            WHERE "client_id" = '${this.name}'
            AND "topic" = '${this.topic}'
            AND "locked_until" < NOW()
            LIMIT 1
            FOR UPDATE
          )
          ORDER BY "t1"."offset" ASC
          LIMIT 1
        ) AS "messages"
        WHERE "t3"."client_id" = '${this.name}'
          AND "t3"."topic" = '${this.topic}'
        RETURNING *
      `);

      return result.rowCount ? result.rows.map(parseMessage) : [];
    } catch (err) {
      console.log("***", err);
      return [];
    }
  }

  async commitData(message) {
    const result = await this.pool.query(`
      UPDATE "fq"."clients"
      SET "offset" = ${message.offset},
          "locked_until" = NOW() - INTERVAL '1ms'
      WHERE "client_id" = '${this.name}'
        AND "topic" = '${this.topic}'
      RETURNING *
    `);
    return result.rows.map(parseClient).shift();
  }

  async loop(handler) {
    if (!this.isActive) {
      return;
    }

    const results = await this.pollData();

    if (results.length) {
      const doc = {
        ...results[0],
        commit: () => this.commitData(results[0]),
      };
      try {
        await handler(doc);
      } catch (err) {
        console.log("@Error:", err.message);
      }
    }

    const delay = results.length ? this.dataDelay : this.dataSleep;
    this.timer = setTimeout(() => this.loop(handler), delay);
  }
}

module.exports = Consumer;
