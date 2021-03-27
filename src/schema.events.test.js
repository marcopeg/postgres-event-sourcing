require("dotenv").config();
const { Client } = require("pg");
const schemaEvents = require("./schema.events");

const connectionString =
  process.env.PGSTRING ||
  "postgres://postgres:postgres@localhost:5432/postgres";

describe("Schema Events", () => {
  // Connect to PG
  const db = new Client({ connectionString });
  beforeAll(() => db.connect());
  afterAll(() => db.end());

  beforeEach(async () => {
    await schemaEvents.reset(db);
    await schemaEvents.create(db);
  });

  test("Add messages and consume them", async () => {
    // Pushing messages on the queue:
    const r1 = await schemaEvents.put(db, { foo: "bar" });
    const r2 = await schemaEvents.put(db, { foo: "bar" });

    // Consume messages from the queue:
    // (the consumer must remember the offset)
    const m1 = await schemaEvents.get(db);
    const m2 = await schemaEvents.get(db, m1.offset);

    // The reading order should respect write order:
    expect(m2.offset).toBeGreaterThan(m1.offset);
    expect(m2.offset - m1.offset).toBe(1);
  });
});
