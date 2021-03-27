require("dotenv").config();
const { Client } = require("pg");
const schemaSubscriptions = require("./schema.subscriptions");

const connectionString =
  process.env.PGSTRING ||
  "postgres://postgres:postgres@localhost:5432/postgres";

describe("Schema", () => {
  // Connect to PG
  const db = new Client({ connectionString });
  beforeAll(() => db.connect());
  afterAll(() => db.end());

  describe("SUBSCRIPTIONS", () => {
    beforeEach(async () => {
      await schemaSubscriptions.reset(db);
      await schemaSubscriptions.create(db);
    });

    it("should create locks after a subscription is activated", async () => {
      // Should create some partitions definition
      await schemaSubscriptions.put(db, { c: 1 }, "t1", "p1");
      await schemaSubscriptions.put(db, { c: 2 }, "t1", "p2");
      await schemaSubscriptions.put(db, { c: 1 }, "t2", "p1");

      // Should upsert the locks for "c1/t1"
      await schemaSubscriptions.subscribe(db, "c1", "t1");

      const r1 = await db.query(`
        SELECT * FROM "fq"."locks"
        WHERE "topic" = 't1' AND "client" = 'c1'
      `);
      expect(r1.rowCount).toBe(2);

      // Should upsert the locks for "c1/t2"
      await schemaSubscriptions.subscribe(db, "c1", "t2");

      const r2 = await db.query(`
        SELECT * FROM "fq"."locks"
        WHERE "topic" = 't2' AND "client" = 'c1'
      `);
      expect(r2.rowCount).toBe(1);
    });

    it("should be idempotent in registering clients and subscriptions", async () => {
      await Promise.all([
        schemaSubscriptions.subscribe(db, "c1", "t1"),
        schemaSubscriptions.subscribe(db, "c1", "t1"),
      ]);
    });

    it("should add the locks for active subscriptions after adding new messages", async () => {
      await schemaSubscriptions.subscribe(db, "c1", "t1");
      await schemaSubscriptions.put(db, { c: 1 }, "t1", "p1");
      await schemaSubscriptions.put(db, { c: 1 }, "t1", "p2");

      const r1 = await db.query(`
        SELECT * FROM "fq"."locks"
        WHERE "topic" = 't1' AND "client" = 'c1'
      `);
      expect(r1.rowCount).toBe(2);
    });

    it("should subscribe to an existing topic since the beginning of time", async () => {
      const m1 = await schemaSubscriptions.put(db, { c: 1 }, "t1", "p1");

      await schemaSubscriptions.subscribe(db, "c1", "t1", true);

      const r1 = await schemaSubscriptions.get(db, "c1", "t1");
      expect(m1.topic).toEqual(r1.topic);
      expect(m1.partition).toEqual(r1.partition);
      expect(m1.offset).toEqual(r1.offset);
    });

    it("should subscribe to an existing topic since the end of time", async () => {
      await schemaSubscriptions.put(db, { c: 1 }, "t1", "p1");

      await schemaSubscriptions.subscribe(db, "c1", "t1");

      const r1 = await schemaSubscriptions.get(db, "c1", "t1");
      expect(r1).toBe(null);

      const m2 = await schemaSubscriptions.put(db, { c: 2 }, "t1", "p1");
      const r2 = await schemaSubscriptions.get(db, "c1", "t1");
      expect(m2.topic).toEqual(r2.topic);
      expect(m2.partition).toEqual(r2.partition);
      expect(m2.offset).toEqual(r2.offset);
    });

    it("should subscribe to an existing topic since a specific point in time", async () => {
      const m1 = await schemaSubscriptions.put(db, { c: 1 }, "t1", "p1");
      const m2 = await schemaSubscriptions.put(db, { c: 2 }, "t1", "p1");
      const m3 = await schemaSubscriptions.put(db, { c: 3 }, "t1", "p1");

      await schemaSubscriptions.subscribe(db, "c1", "t1", m2.createdAt);

      const r1 = await schemaSubscriptions.get(db, "c1", "t1");
      expect(r1.topic).toEqual(m2.topic);
      expect(r1.partition).toEqual(m2.partition);
      expect(r1.offset).toEqual(m2.offset);
      expect(r1.payload.c).toEqual(m2.payload.c);
    });

    it("should rewind a subscription", async () => {
      const m1 = await schemaSubscriptions.put(db, { c: 1 }, "t1", "p1");
      const m2 = await schemaSubscriptions.put(db, { c: 2 }, "t1", "p1");
      const m3 = await schemaSubscriptions.put(db, { c: 3 }, "t1", "p1");

      await schemaSubscriptions.subscribe(db, "c1", "t1");

      const r1 = await schemaSubscriptions.get(db, "c1", "t1");
      expect(r1).toBe(null);

      // Rewind the subscription to the beginning
      await schemaSubscriptions.subscribe(db, "c1", "t1", true);
      const r2 = await schemaSubscriptions.get(db, "c1", "t1");
      expect(r2.topic).toEqual(m1.topic);
      expect(r2.partition).toEqual(m1.partition);
      expect(r2.offset).toEqual(m1.offset);
      expect(r2.payload.c).toEqual(m1.payload.c);

      // Just check that the cursor can move forward
      await schemaSubscriptions.commit(db, r2);
      const r3 = await schemaSubscriptions.get(db, "c1", "t1");
      expect(r3.topic).toEqual(m2.topic);
      expect(r3.partition).toEqual(m2.partition);
      expect(r3.offset).toEqual(m2.offset);
      expect(r3.payload.c).toEqual(m2.payload.c);

      // Just check that the cursor can move forward
      await schemaSubscriptions.commit(db, r3);
      const r4 = await schemaSubscriptions.get(db, "c1", "t1");
      expect(r4.topic).toEqual(m3.topic);
      expect(r4.partition).toEqual(m3.partition);
      expect(r4.offset).toEqual(m3.offset);
      expect(r4.payload.c).toEqual(m3.payload.c);

      // Just check that the cursor can move forward
      await schemaSubscriptions.commit(db, r4);
      const r5 = await schemaSubscriptions.get(db, "c1", "t1");
      expect(r5).toBe(null);

      // Now rewind to a specific point in time
      await schemaSubscriptions.subscribe(db, "c1", "t1", m2.createdAt);
      const r6 = await schemaSubscriptions.get(db, "c1", "t1");
      expect(r6.topic).toEqual(m2.topic);
      expect(r6.partition).toEqual(m2.partition);
      expect(r6.offset).toEqual(m2.offset);
      expect(r6.payload.c).toEqual(m2.payload.c);
    });

    it("should not be able to get messages without a subscription", async () => {
      await schemaSubscriptions.put(db, { c: 1 }, "t1", "p1");
      const r1 = await schemaSubscriptions.get(db, "c1", "t1");
      expect(r1).toBe(null);
    });
  });
});
