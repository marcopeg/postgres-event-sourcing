require("dotenv").config();
const { Client } = require("pg");
const schemaTopic = require("./schema.topic");

const connectionString =
  process.env.PGSTRING ||
  "postgres://postgres:postgres@localhost:5432/postgres";

describe("Schema", () => {
  // Connect to PG
  const db = new Client({ connectionString });
  beforeAll(() => db.connect());
  afterAll(() => db.end());

  describe("TOPIC", () => {
    beforeEach(async () => {
      await schemaTopic.reset(db);
      await schemaTopic.create(db);
    });

    test("It should return a message for the same topic", async () => {
      await schemaTopic.put(db, "t1", { foo: "bar" });
      const m1 = await schemaTopic.get(db, "c1", "t1");
      await m1.commit();

      // There should be an entry with the correct offset
      // for the last processed message
      const results = await db.query(`
        SELECT * FROM "fq"."clients"
        WHERE "id" = 'c1'
          AND "topic" = 't1'
          AND "offset" = ${m1.offset}
      `);
      expect(results.rowCount).toBe(1);
    });

    test("It should receive a NULL value in case of missing next message", async () => {
      await schemaTopic.put(db, "t1", { foo: "bar" });
      const m1 = await schemaTopic.get(db, "c1", "non-exitent-topic");
      expect(m1).toBe(null);
    });

    test("A client should store the last read message in the cloud per topic", async () => {
      // Pushing messages on the queue:
      await schemaTopic.put(db, "t1", { name: "t1-001" });
      await schemaTopic.put(db, "t2", { name: "t2-001" });
      await schemaTopic.put(db, "t3", { name: "t3-001" });
      await schemaTopic.put(db, "t1", { name: "t1-002" });

      // Consume messages from the queue:
      const m1 = await schemaTopic.get(db, "c1", "t1");
      expect(m1.payload.name).toBe("t1-001");
      await m1.commit();

      // M2 should fetch the second message for topic "t1"
      // as we have commited M1
      const m2 = await schemaTopic.get(db, "c1", "t1");
      expect(m2.payload.name).toBe("t1-002");

      // M3 is requested BEFORE m2.commit()
      // it should get back M2 again
      const m3 = await schemaTopic.get(db, "c1", "t1");
      expect(m3.payload.name).toBe("t1-002");

      // M4 is requested by a new client.
      // it should receive the first message
      const m4 = await schemaTopic.get(db, "c2", "t1");
      expect(m4.payload.name).toBe("t1-001");
      await m4.commit();

      // M5 is requested by C1 on a new topic
      const m5 = await schemaTopic.get(db, "c1", "t2");
      expect(m5.payload.name).toBe("t2-001");
      await m5.commit();

      // Check out the correct offset holders for the
      // clients that we used
      const results1 = await db.query(`
        SELECT * FROM "fq"."clients"
        WHERE "id" = 'c1'
          AND "topic" = 't1'
          AND "offset" = ${m1.offset}
      `);
      expect(results1.rowCount).toBe(1);

      const results2 = await db.query(`
        SELECT * FROM "fq"."clients"
        WHERE "id" = 'c1'
          AND "topic" = 't2'
          AND "offset" = ${m5.offset}
      `);
      expect(results2.rowCount).toBe(1);

      const results3 = await db.query(`
        SELECT * FROM "fq"."clients"
        WHERE "id" = 'c2'
          AND "topic" = 't1'
          AND "offset" = ${m4.offset}
      `);
      expect(results3.rowCount).toBe(1);
    });
  });
});
