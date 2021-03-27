const parseMessage = (msg) => ({
  offset: Number(msg.offset),
  createdAt: new Date(msg.created_at),
  topic: msg.topic,
  payload: msg.payload,
});

class Producer {
  constructor(client) {
    this.client = client;
  }

  async write(payload = null, topic = "*") {
    if (!payload) {
      throw new Error("Payload needed");
    }

    const result = await this.client.query(`
      INSERT INTO "fq"."messages"
      ("topic", "payload") VALUES
      ('${topic}', '${JSON.stringify(payload)}')
      RETURNING *
    `);

    return parseMessage(result.rows[0]);
  }
}

module.exports = Producer;
