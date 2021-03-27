require("dotenv").config();
const { Client } = require("pg");
const schemaClients = require("./schema.clients");

const connectionString =
  process.env.PGSTRING ||
  "postgres://postgres:postgres@localhost:5432/postgres";

describe("Schema Clients", () => {
  // Connect to PG
  const client = new Client({ connectionString });
  beforeAll(() => client.connect());
  afterAll(() => client.end());

  beforeEach(async () => {
    await schemaClients.reset(client);
    await schemaClients.create(client);
  });

  test("A client should store the last read message in the cloud", async () => {
    // Pushing messages on the queue:
    await schemaClients.put(client, { foo: "bar" });
    await schemaClients.put(client, { foo: "bar" });
    await schemaClients.put(client, { foo: "bar" });

    // Consume messages from the queue:
    // (the system remember the offset)
    const m1 = await schemaClients.get(client, "c1");
    await m1.commit();
    const m2 = await schemaClients.get(client, "c1");

    // NOTE: M3 is requested BEFORE m2.commit()
    // it should get back "m2" again
    const m3 = await schemaClients.get(client, "c1");

    // NOTE: M4 is requested by a new client.
    // it should receive the first message
    const m4 = await schemaClients.get(client, "c2");

    // The reading order should respect write order:
    expect(m2.offset).toBeGreaterThan(m1.offset);
    expect(m2.offset - m1.offset).toBe(1);
    expect(m2.offset).toEqual(m3.offset);
    expect(m4.offset).toBe(1);
  });
});
