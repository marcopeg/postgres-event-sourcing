require("dotenv").config();
const { Client } = require("pg");
const schemaPartitions = require("./schema.partitions");

const connectionString =
  process.env.PGSTRING ||
  "postgres://postgres:postgres@localhost:5432/postgres";

describe("Schema", () => {
  // Connect to PG
  const db = new Client({ connectionString });
  beforeAll(() => db.connect());
  afterAll(() => db.end());

  describe("PARTITIONS", () => {
    beforeEach(async () => {
      await schemaPartitions.reset(db);
      await schemaPartitions.create(db);
    });

    it("should allocate partitions after appending a new message", async () => {
      await schemaPartitions.put(db, { c: 1 }, "t1", "p1");
      await schemaPartitions.put(db, { c: 2 }, "t1", "p1");
      await schemaPartitions.put(db, { c: 2 }, "t1", "p2");

      const r1 = await db.query(`
        SELECT * FROM "fq"."partitions"
        WHERE "topic" = 't1'
      `);

      expect(r1.rowCount).toBe(2);
    });

    it("should register a client since the beginning of time", async () => {
      const m1 = await schemaPartitions.put(db, { c: 0 });
      await schemaPartitions.registerClient(db, "c1", true);
      const m1g = await schemaPartitions.get(db, "c1");
      expect(m1.topic).toEqual(m1g.topic);
      expect(m1.partition).toEqual(m1g.partition);
      expect(m1.offset).toEqual(m1g.offset);
    });

    it("should register a client since the current point in time", async () => {
      await schemaPartitions.put(db, { c: 0 });
      await schemaPartitions.registerClient(db, "c1");
      const m1 = await schemaPartitions.get(db, "c1");
      expect(m1).toBe(null);

      await schemaPartitions.put(db, { c: 2 });
      const m2 = await schemaPartitions.get(db, "c1");
      expect(m2.offset).toBe(2);
    });

    it("should register a client since a specific point in time", async () => {
      await schemaPartitions.put(db, { c: 0 });
      const p2 = await schemaPartitions.put(db, { c: 1 });
      const p3 = await schemaPartitions.put(db, { c: 2 });

      // Register the client since the POT when the second message
      // entered the queue
      await schemaPartitions.registerClient(db, "c1", p2.createdAt);
      const m1 = await schemaPartitions.get(db, "c1");
      expect(m1.topic).toEqual(p2.topic);
      expect(m1.partition).toEqual(p2.partition);
      expect(m1.offset).toEqual(p2.offset);
      await m1.commit();

      // Get the following message:
      const m2 = await schemaPartitions.get(db, "c1");
      expect(m2.topic).toEqual(p3.topic);
      expect(m2.partition).toEqual(p3.partition);
      expect(m2.offset).toEqual(p3.offset);

      // Add a new message into the topic while the last message
      // is still open, just simulating some shitty condition:
      const p4 = await schemaPartitions.put(db, { c: 3 });
      await m2.commit();

      // It should get such a message:
      const m3 = await schemaPartitions.get(db, "c1");
      expect(m3.topic).toEqual(p4.topic);
      expect(m3.partition).toEqual(p4.partition);
      expect(m3.offset).toEqual(p4.offset);
    });

    // TODO
    it("should roll back the subscription to a specific point in time when re-registering a client", () => {});

    it("should upsert new partitions locks on existing clients after posting a new message", async () => {
      await schemaPartitions.registerClient(db, "c1");
      await schemaPartitions.registerClient(db, "c2");
      await schemaPartitions.put(db, { c: 0 }, "t1");
      await schemaPartitions.put(db, { c: 1 }, "t2");

      // It should upsert the combination of locks for the matrix of (dbs)*(topics)
      const r1 = await db.query(`SELECT COUNT(*) FROM "fq"."locks"`);
      expect(Number(r1.rows[0].count)).toBe(4);

      // Adding a new partition should now bump the locks to 6
      await schemaPartitions.put(db, { c: 0 }, "t3");
      const r2 = await db.query(`SELECT COUNT(*) FROM "fq"."locks"`);
      expect(Number(r2.rows[0].count)).toBe(6);

      // Adding a new client now should bring the locks to 9
      // because it just adds the locks on the 3 current topics with a single partition
      await schemaPartitions.registerClient(db, "c3");
      const r3 = await db.query(`SELECT COUNT(*) FROM "fq"."locks"`);
      expect(Number(r3.rows[0].count)).toBe(9);

      // Adding a new partition to an existing topic should bump the
      // total to 12, as one more lock is added for each lient
      await schemaPartitions.put(db, { c: 0 }, "t1", "p1");
      const r4 = await db.query(`SELECT COUNT(*) FROM "fq"."locks"`);
      expect(Number(r4.rows[0].count)).toBe(12);
    });

    test("it should get the first document of a single partition", async () => {
      await schemaPartitions.registerClient(db, "c1");
      await schemaPartitions.put(db, { c: "1" }, "t1", "p1");
      await schemaPartitions.put(db, { c: "2" }, "t1", "p1");
      await schemaPartitions.put(db, { c: "1b" }, "t1", "p1b");
      await schemaPartitions.put(db, { c: "2b" }, "t1", "p1b");

      // should read from p1-c'1'
      const m1 = await schemaPartitions.get(db, "c1", "t1");
      expect(m1.payload.c).toBe("1");
      expect(m1.offset).toBe(1);
      expect(m1.client).toBe("c1");
      expect(m1.topic).toBe("t1");
      expect(m1.partition).toBe("p1");

      // should read from p1b-c'1b'
      const m2 = await schemaPartitions.get(db, "c1", "t1");
      expect(m2.payload.c).toBe("1b");
      expect(m2.offset).toBe(3);
      expect(m2.client).toBe("c1");
      expect(m2.topic).toBe("t1");
      expect(m2.partition).toBe("p1b");

      // no messages should be available until a commit happens
      const m3 = await schemaPartitions.get(db, "c1", "t1");
      expect(m3).toBe(null);

      // commit the first message, should confirm the message offset as
      // last known offset in the lock table:
      const m1c = await m1.commit();
      expect(m1c.offset).toEqual(m1.offset);

      // commiting the first message should unlock the next offset within
      // the same partition "p1"
      const m4 = await schemaPartitions.get(db, "c1", "t1");
      expect(m4.payload.c).toBe("2");
      expect(m4.offset).toBe(2);
      expect(m4.client).toBe("c1");
      expect(m4.topic).toBe("t1");
      expect(m4.partition).toBe("p1");

      // no messages should be available now, as "p1" is blocked
      // by "m4" that still hasn't commit, and "p1b" is blocked
      // by "m2" still
      const m5 = await schemaPartitions.get(db, "c1", "t1");
      expect(m5).toBe(null);

      // commit the message in the othe partition:
      await Promise.all([m2.commit(), m4.commit()]);

      // At this point, "p1" should be consumed to the end by "c1":
      // (there should be only one row that connects "locks" and "partitions"
      // where the "offset" is matching)
      const r1 = await db.query(`
        SELECT * FROM "fq"."locks" AS "t1"
        WHERE "t1"."client" = 'c1'
          AND "t1"."topic" = 't1'
          AND "t1"."partition" = 'p1'
          AND "t1"."offset" = (
            SELECT "offset" FROM "fq"."partitions" AS "t2"
            WHERE "t2"."topic" = 't1'
              AND "t2"."partition" = 'p1'
          )
      `);
      expect(r1.rowCount).toBe(1);

      // Consumes the last message:
      const m6 = await schemaPartitions.get(db, "c1", "t1");
      await m6.commit();

      const r2 = await db.query(`
        SELECT * FROM "fq"."locks" AS "t1"
        JOIN "fq"."partitions" AS "t2"
          ON "t1"."topic" = "t2"."topic"
         AND "t1"."partition" = "t2"."partition"
         AND "t1"."offset" = "t2"."offset"
      `);
      expect(r2.rowCount).toBe(2);

      // A new client should be able to start over
      await schemaPartitions.registerClient(db, "c2", true);
      const m7 = await schemaPartitions.get(db, "c2", "t1");
      expect(m7.partition).toEqual(m1.partition);
      expect(m7.offset).toEqual(m1.offset);
      expect(m7.payload.c).toEqual(m1.payload.c);
    });

    test("It should NOT return messages without registering a client", async () => {
      await schemaPartitions.put(db, { c: "1" }, "t1", "p1");
      await schemaPartitions.put(db, { c: "2" }, "t1", "p1");
      await schemaPartitions.put(db, { c: "3" }, "t1", "p1");

      // The first read should fail as there is no client registered
      const m1 = await schemaPartitions.get(db, "c1", "t1");
      expect(m1).toBe(null);

      // The second read should work as the client is set up
      await schemaPartitions.registerClient(db, "c1", true);
      const m2 = await schemaPartitions.get(db, "c1", "t1");
      expect(m2.payload.c).toBe("1");
    });

    test("it should work with an uneven amount of messages in different partitions", async () => {
      await schemaPartitions.put(db, { c: "1" }, "t1", "p1");
      await schemaPartitions.put(db, { c: "2" }, "t1", "p1");
      await schemaPartitions.put(db, { c: "3" }, "t1", "p1");
      await schemaPartitions.put(db, { c: "1b" }, "t1", "p1b");

      await schemaPartitions.registerClient(db, "c1", true);

      const m1 = await schemaPartitions.get(db, "c1", "t1");
      const m2 = await schemaPartitions.get(db, "c1", "t1");
      await Promise.all([m1.commit(), m2.commit()]);

      const m3 = await schemaPartitions.get(db, "c1", "t1");
      await m3.commit();

      const m4 = await schemaPartitions.get(db, "c1", "t1");
      await m4.commit();

      const r2 = await db.query(`
        SELECT * FROM "fq"."locks" AS "t1"
        JOIN "fq"."partitions" AS "t2"
          ON "t1"."topic" = "t2"."topic"
         AND "t1"."partition" = "t2"."partition"
         AND "t1"."offset" = "t2"."offset"
      `);
      expect(r2.rowCount).toBe(2);
    });

    test("It should entirely consume a topic even if there are messages from different topics in the log", async () => {
      await schemaPartitions.put(db, { c: "t1-1" }, "t1");
      await schemaPartitions.put(db, { c: "t2-1" }, "t2");
      await schemaPartitions.put(db, { c: "t1-2" }, "t1");
      await schemaPartitions.put(db, { c: "t2-2" }, "t2");
      await schemaPartitions.put(db, { c: "t1-3" }, "t1");

      await schemaPartitions.registerClient(db, "c1", true);

      // Start consuming topic "t1"

      const m1 = await schemaPartitions.get(db, "c1", "t1");
      await m1.commit();
      expect(m1.payload.c).toBe("t1-1");

      const m2 = await schemaPartitions.get(db, "c1", "t1");
      await m2.commit();
      expect(m2.payload.c).toBe("t1-2");

      const m3 = await schemaPartitions.get(db, "c1", "t1");
      await m3.commit();
      expect(m3.payload.c).toBe("t1-3");

      const m4 = await schemaPartitions.get(db, "c1", "t1");
      expect(m4).toBe(null);

      // Start consuming topic "t2"

      const m5 = await schemaPartitions.get(db, "c1", "t2");
      await m5.commit();
      expect(m5.payload.c).toBe("t2-1");

      const m6 = await schemaPartitions.get(db, "c1", "t2");
      await m6.commit();
      expect(m6.payload.c).toBe("t2-2");

      const m7 = await schemaPartitions.get(db, "c1", "t2");
      expect(m7).toBe(null);
    });
  });
});
