require("dotenv").config();
const { Client } = require("pg");
const schemaLocks = require("./schema.locks");

const connectionString =
  process.env.PGSTRING ||
  "postgres://postgres:postgres@localhost:5432/postgres";

describe("Schema", () => {
  // Connect to PG
  const db = new Client({ connectionString });
  beforeAll(() => db.connect());
  afterAll(() => db.end());

  describe("LOCKS", () => {
    beforeEach(async () => {
      await schemaLocks.reset(db);
      await schemaLocks.create(db);
    });

    test("It should NOT return messages without registering a client", async () => {
      await schemaLocks.put(db, "t1", { c: 1 });
      await schemaLocks.put(db, "t1", { c: 2 });
      await schemaLocks.put(db, "t1", { c: 3 });

      // The first read should fail as there is no client registered
      const m1 = await schemaLocks.get(db, "c1", "t1");
      expect(m1).toBe(null);

      // The second read should work as the client is set up
      await schemaLocks.registerClient(db, "c1", "t1");
      const m2 = await schemaLocks.get(db, "c1", "t1");
      expect(m2.payload.c).toBe(1);
    });

    test("It should return a message for the same topic", async () => {
      await schemaLocks.registerClient(db, "c1", "t1");

      await schemaLocks.put(db, "t1", { c: 1 });
      await schemaLocks.put(db, "t1", { c: 2 });
      await schemaLocks.put(db, "t1", { c: 3 });
      const m1 = await schemaLocks.get(db, "c1", "t1");
      const m2 = await schemaLocks.get(db, "c1", "t1");
      expect(m1.payload.c).toBe(1);
      expect(m2).toBe(null);
      await m1.commit();

      const m3 = await schemaLocks.get(db, "c1", "t1");
      expect(m3.payload.c).toBe(2);
      expect(m3.offset).toBeGreaterThan(m1.offset);

      // There should be an entry with the correct offset
      // for the last processed message
      const results = await db.query(`
        SELECT * FROM "fq"."clients"
        WHERE "client_id" = 'c1'
          AND "topic" = 't1'
          AND "offset" = ${m1.offset}
      `);
      expect(results.rowCount).toBe(1);
    });

    test("It should handle multiple clients on the same topic", async () => {
      await schemaLocks.put(db, "t1", { c: 1 });

      await schemaLocks.registerClient(db, "c1", "t1");
      await schemaLocks.registerClient(db, "c2", "t1");

      const m1 = await schemaLocks.get(db, "c1", "t1");
      const m2 = await schemaLocks.get(db, "c2", "t1");

      expect(m1.offset).toBe(m2.offset);
    });
  });
});
