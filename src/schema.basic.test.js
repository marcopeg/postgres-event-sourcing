require("dotenv").config();
const { Client } = require("pg");
const schemaBasic = require("./schema.basic");

describe("Schema", () => {
  // Connect to PG
  const client = new Client({ connectionString: process.env.PGSTRING });
  beforeAll(() => client.connect());
  afterAll(() => client.end());

  describe("BASIC", () => {
    beforeEach(async () => {
      await schemaBasic.reset(client);
      await schemaBasic.create(client);
    });

    test("Add messages and consume them", async () => {
      // Pushing messages on the queue:
      const r1 = await schemaBasic.put(client, { foo: "bar" });
      const r2 = await schemaBasic.put(client, { foo: "bar" });

      // Consume messages from the queue:
      // (the consumer must remember the offset)
      const m1 = await schemaBasic.get(client);
      const m2 = await schemaBasic.get(client, m1.offset);

      // The reading order should respect write order:
      expect(m2.offset).toBeGreaterThan(m1.offset);
      expect(m2.offset - m1.offset).toBe(1);
    });
  });
});
