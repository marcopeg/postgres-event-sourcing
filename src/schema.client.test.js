require("dotenv").config();
const { Client } = require("pg");
const schemaClient = require("./schema.client");

const sleep = (t) => new Promise((r) => setTimeout(r, t));

describe("Schema", () => {
  // Connect to PG
  const client = new Client({ connectionString: process.env.PGSTRING });
  beforeAll(() => client.connect());
  afterAll(() => client.end());

  describe("CLIENT", () => {
    beforeEach(async () => {
      await schemaClient.reset(client);
      await schemaClient.create(client);
    });

    test("A client should store the last read message in the cloud", async () => {
      // Pushing messages on the queue:
      await schemaClient.put(client, { foo: "bar" });
      await schemaClient.put(client, { foo: "bar" });
      await schemaClient.put(client, { foo: "bar" });

      // Consume messages from the queue:
      // (the system remember the offset)
      const m1 = await schemaClient.get(client, "c1");
      await m1.commit();
      const m2 = await schemaClient.get(client, "c1");

      // NOTE: M3 is requested BEFORE m2.commit()
      // it should get back "m2" again
      const m3 = await schemaClient.get(client, "c1");

      // NOTE: M4 is requested by a new client.
      // it should receive the first message
      const m4 = await schemaClient.get(client, "c2");

      // The reading order should respect write order:
      expect(m2.offset).toBeGreaterThan(m1.offset);
      expect(m2.offset - m1.offset).toBe(1);
      expect(m2.offset).toEqual(m3.offset);
      expect(m4.offset).toBe(1);
    });
  });
});
