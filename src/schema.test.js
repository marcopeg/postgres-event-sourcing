require("dotenv").config();
const { Client } = require("pg");
const schemaBasic = require("./schema.basic");
const schemaClient = require("./schema.client");
const schemaTopic = require("./schema.topic");
const schemaLocks = require("./schema.locks");
const schemaPartitions = require("./schema.partitions");
const schemaSubscriptions = require("./schema.subscriptions");

const sleep = (t) => new Promise((r) => setTimeout(r, t));

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

  describe("TOPIC", () => {
    beforeEach(async () => {
      await schemaTopic.reset(client);
      await schemaTopic.create(client);
    });

    test("It should return a message for the same topic", async () => {
      await schemaTopic.put(client, "t1", { foo: "bar" });
      const m1 = await schemaTopic.get(client, "c1", "t1");
      await m1.commit();

      // There should be an entry with the correct offset
      // for the last processed message
      const results = await client.query(`
        SELECT * FROM "fq"."clients"
        WHERE "id" = 'c1'
          AND "topic" = 't1'
          AND "offset" = ${m1.offset}
      `);
      expect(results.rowCount).toBe(1);
    });

    test("It should receive a NULL value in case of missing next message", async () => {
      await schemaTopic.put(client, "t1", { foo: "bar" });
      const m1 = await schemaTopic.get(client, "c1", "non-exitent-topic");
      expect(m1).toBe(null);
    });

    test("A client should store the last read message in the cloud per topic", async () => {
      // Pushing messages on the queue:
      await schemaTopic.put(client, "t1", { name: "t1-001" });
      await schemaTopic.put(client, "t2", { name: "t2-001" });
      await schemaTopic.put(client, "t3", { name: "t3-001" });
      await schemaTopic.put(client, "t1", { name: "t1-002" });

      // Consume messages from the queue:
      const m1 = await schemaTopic.get(client, "c1", "t1");
      expect(m1.payload.name).toBe("t1-001");
      await m1.commit();

      // M2 should fetch the second message for topic "t1"
      // as we have commited M1
      const m2 = await schemaTopic.get(client, "c1", "t1");
      expect(m2.payload.name).toBe("t1-002");

      // M3 is requested BEFORE m2.commit()
      // it should get back M2 again
      const m3 = await schemaTopic.get(client, "c1", "t1");
      expect(m3.payload.name).toBe("t1-002");

      // M4 is requested by a new client.
      // it should receive the first message
      const m4 = await schemaTopic.get(client, "c2", "t1");
      expect(m4.payload.name).toBe("t1-001");
      await m4.commit();

      // M5 is requested by C1 on a new topic
      const m5 = await schemaTopic.get(client, "c1", "t2");
      expect(m5.payload.name).toBe("t2-001");
      await m5.commit();

      // Check out the correct offset holders for the
      // clients that we used
      const results1 = await client.query(`
        SELECT * FROM "fq"."clients"
        WHERE "id" = 'c1'
          AND "topic" = 't1'
          AND "offset" = ${m1.offset}
      `);
      expect(results1.rowCount).toBe(1);

      const results2 = await client.query(`
        SELECT * FROM "fq"."clients"
        WHERE "id" = 'c1'
          AND "topic" = 't2'
          AND "offset" = ${m5.offset}
      `);
      expect(results2.rowCount).toBe(1);

      const results3 = await client.query(`
        SELECT * FROM "fq"."clients"
        WHERE "id" = 'c2'
          AND "topic" = 't1'
          AND "offset" = ${m4.offset}
      `);
      expect(results3.rowCount).toBe(1);
    });
  });

  describe("LOCKS", () => {
    beforeEach(async () => {
      await schemaLocks.reset(client);
      await schemaLocks.create(client);
    });

    test("It should NOT return messages without registering a client", async () => {
      await schemaLocks.put(client, "t1", { c: 1 });
      await schemaLocks.put(client, "t1", { c: 2 });
      await schemaLocks.put(client, "t1", { c: 3 });

      // The first read should fail as there is no client registered
      const m1 = await schemaLocks.get(client, "c1", "t1");
      expect(m1).toBe(null);

      // The second read should work as the client is set up
      await schemaLocks.registerClient(client, "c1", "t1");
      const m2 = await schemaLocks.get(client, "c1", "t1");
      expect(m2.payload.c).toBe(1);
    });

    test("It should return a message for the same topic", async () => {
      await schemaLocks.registerClient(client, "c1", "t1");

      await schemaLocks.put(client, "t1", { c: 1 });
      await schemaLocks.put(client, "t1", { c: 2 });
      await schemaLocks.put(client, "t1", { c: 3 });
      const m1 = await schemaLocks.get(client, "c1", "t1");
      const m2 = await schemaLocks.get(client, "c1", "t1");
      expect(m1.payload.c).toBe(1);
      expect(m2).toBe(null);
      await m1.commit();

      const m3 = await schemaLocks.get(client, "c1", "t1");
      expect(m3.payload.c).toBe(2);
      expect(m3.offset).toBeGreaterThan(m1.offset);

      // There should be an entry with the correct offset
      // for the last processed message
      const results = await client.query(`
        SELECT * FROM "fq"."clients"
        WHERE "client_id" = 'c1'
          AND "topic" = 't1'
          AND "offset" = ${m1.offset}
      `);
      expect(results.rowCount).toBe(1);
    });

    test("It should handle multiple clients on the same topic", async () => {
      await schemaLocks.put(client, "t1", { c: 1 });

      await schemaLocks.registerClient(client, "c1", "t1");
      await schemaLocks.registerClient(client, "c2", "t1");

      const m1 = await schemaLocks.get(client, "c1", "t1");
      const m2 = await schemaLocks.get(client, "c2", "t1");

      expect(m1.offset).toBe(m2.offset);
    });
  });

  describe("PARTITIONS", () => {
    beforeEach(async () => {
      await schemaPartitions.reset(client);
      await schemaPartitions.create(client);
    });

    it("should allocate partitions after appending a new message", async () => {
      await schemaPartitions.put(client, { c: 1 }, "t1", "p1");
      await schemaPartitions.put(client, { c: 2 }, "t1", "p1");
      await schemaPartitions.put(client, { c: 2 }, "t1", "p2");

      const r1 = await client.query(`
        SELECT * FROM "fq"."partitions"
        WHERE "topic" = 't1'
      `);

      expect(r1.rowCount).toBe(2);
    });

    it("should register a client since the beginning of time", async () => {
      const m1 = await schemaPartitions.put(client, { c: 0 });
      await schemaPartitions.registerClient(client, "c1", true);
      const m1g = await schemaPartitions.get(client, "c1");
      expect(m1.topic).toEqual(m1g.topic);
      expect(m1.partition).toEqual(m1g.partition);
      expect(m1.offset).toEqual(m1g.offset);
    });

    it("should register a client since the current point in time", async () => {
      await schemaPartitions.put(client, { c: 0 });
      await schemaPartitions.registerClient(client, "c1");
      const m1 = await schemaPartitions.get(client, "c1");
      expect(m1).toBe(null);

      await schemaPartitions.put(client, { c: 2 });
      const m2 = await schemaPartitions.get(client, "c1");
      expect(m2.offset).toBe(2);
    });

    it("should register a client since a specific point in time", async () => {
      await schemaPartitions.put(client, { c: 0 });
      const p2 = await schemaPartitions.put(client, { c: 1 });
      const p3 = await schemaPartitions.put(client, { c: 2 });

      // Register the client since the POT when the second message
      // entered the queue
      await schemaPartitions.registerClient(client, "c1", p2.createdAt);
      const m1 = await schemaPartitions.get(client, "c1");
      expect(m1.topic).toEqual(p2.topic);
      expect(m1.partition).toEqual(p2.partition);
      expect(m1.offset).toEqual(p2.offset);
      await m1.commit();

      // Get the following message:
      const m2 = await schemaPartitions.get(client, "c1");
      expect(m2.topic).toEqual(p3.topic);
      expect(m2.partition).toEqual(p3.partition);
      expect(m2.offset).toEqual(p3.offset);

      // Add a new message into the topic while the last message
      // is still open, just simulating some shitty condition:
      const p4 = await schemaPartitions.put(client, { c: 3 });
      await m2.commit();

      // It should get such a message:
      const m3 = await schemaPartitions.get(client, "c1");
      expect(m3.topic).toEqual(p4.topic);
      expect(m3.partition).toEqual(p4.partition);
      expect(m3.offset).toEqual(p4.offset);
    });

    // TODO
    it("should roll back the subscription to a specific point in time when re-registering a client", () => {});

    it("should upsert new partitions locks on existing clients after posting a new message", async () => {
      await schemaPartitions.registerClient(client, "c1");
      await schemaPartitions.registerClient(client, "c2");
      await schemaPartitions.put(client, { c: 0 }, "t1");
      await schemaPartitions.put(client, { c: 1 }, "t2");

      // It should upsert the combination of locks for the matrix of (clients)*(topics)
      const r1 = await client.query(`SELECT COUNT(*) FROM "fq"."locks"`);
      expect(Number(r1.rows[0].count)).toBe(4);

      // Adding a new partition should now bump the locks to 6
      await schemaPartitions.put(client, { c: 0 }, "t3");
      const r2 = await client.query(`SELECT COUNT(*) FROM "fq"."locks"`);
      expect(Number(r2.rows[0].count)).toBe(6);

      // Adding a new client now should bring the locks to 9
      // because it just adds the locks on the 3 current topics with a single partition
      await schemaPartitions.registerClient(client, "c3");
      const r3 = await client.query(`SELECT COUNT(*) FROM "fq"."locks"`);
      expect(Number(r3.rows[0].count)).toBe(9);

      // Adding a new partition to an existing topic should bump the
      // total to 12, as one more lock is added for each lient
      await schemaPartitions.put(client, { c: 0 }, "t1", "p1");
      const r4 = await client.query(`SELECT COUNT(*) FROM "fq"."locks"`);
      expect(Number(r4.rows[0].count)).toBe(12);
    });

    test("it should get the first document of a single partition", async () => {
      await schemaPartitions.registerClient(client, "c1");
      await schemaPartitions.put(client, { c: "1" }, "t1", "p1");
      await schemaPartitions.put(client, { c: "2" }, "t1", "p1");
      await schemaPartitions.put(client, { c: "1b" }, "t1", "p1b");
      await schemaPartitions.put(client, { c: "2b" }, "t1", "p1b");

      // should read from p1-c'1'
      const m1 = await schemaPartitions.get(client, "c1", "t1");
      expect(m1.payload.c).toBe("1");
      expect(m1.offset).toBe(1);
      expect(m1.client).toBe("c1");
      expect(m1.topic).toBe("t1");
      expect(m1.partition).toBe("p1");

      // should read from p1b-c'1b'
      const m2 = await schemaPartitions.get(client, "c1", "t1");
      expect(m2.payload.c).toBe("1b");
      expect(m2.offset).toBe(3);
      expect(m2.client).toBe("c1");
      expect(m2.topic).toBe("t1");
      expect(m2.partition).toBe("p1b");

      // no messages should be available until a commit happens
      const m3 = await schemaPartitions.get(client, "c1", "t1");
      expect(m3).toBe(null);

      // commit the first message, should confirm the message offset as
      // last known offset in the lock table:
      const m1c = await m1.commit();
      expect(m1c.offset).toEqual(m1.offset);

      // commiting the first message should unlock the next offset within
      // the same partition "p1"
      const m4 = await schemaPartitions.get(client, "c1", "t1");
      expect(m4.payload.c).toBe("2");
      expect(m4.offset).toBe(2);
      expect(m4.client).toBe("c1");
      expect(m4.topic).toBe("t1");
      expect(m4.partition).toBe("p1");

      // no messages should be available now, as "p1" is blocked
      // by "m4" that still hasn't commit, and "p1b" is blocked
      // by "m2" still
      const m5 = await schemaPartitions.get(client, "c1", "t1");
      expect(m5).toBe(null);

      // commit the message in the othe partition:
      await Promise.all([m2.commit(), m4.commit()]);

      // At this point, "p1" should be consumed to the end by "c1":
      // (there should be only one row that connects "locks" and "partitions"
      // where the "offset" is matching)
      const r1 = await client.query(`
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
      const m6 = await schemaPartitions.get(client, "c1", "t1");
      await m6.commit();

      const r2 = await client.query(`
        SELECT * FROM "fq"."locks" AS "t1"
        JOIN "fq"."partitions" AS "t2"
          ON "t1"."topic" = "t2"."topic"
         AND "t1"."partition" = "t2"."partition"
         AND "t1"."offset" = "t2"."offset"
      `);
      expect(r2.rowCount).toBe(2);

      // A new client should be able to start over
      await schemaPartitions.registerClient(client, "c2", true);
      const m7 = await schemaPartitions.get(client, "c2", "t1");
      expect(m7.partition).toEqual(m1.partition);
      expect(m7.offset).toEqual(m1.offset);
      expect(m7.payload.c).toEqual(m1.payload.c);
    });

    test("It should NOT return messages without registering a client", async () => {
      await schemaPartitions.put(client, { c: "1" }, "t1", "p1");
      await schemaPartitions.put(client, { c: "2" }, "t1", "p1");
      await schemaPartitions.put(client, { c: "3" }, "t1", "p1");

      // The first read should fail as there is no client registered
      const m1 = await schemaPartitions.get(client, "c1", "t1");
      expect(m1).toBe(null);

      // The second read should work as the client is set up
      await schemaPartitions.registerClient(client, "c1", true);
      const m2 = await schemaPartitions.get(client, "c1", "t1");
      expect(m2.payload.c).toBe("1");
    });

    test("it should work with an uneven amount of messages in different partitions", async () => {
      await schemaPartitions.put(client, { c: "1" }, "t1", "p1");
      await schemaPartitions.put(client, { c: "2" }, "t1", "p1");
      await schemaPartitions.put(client, { c: "3" }, "t1", "p1");
      await schemaPartitions.put(client, { c: "1b" }, "t1", "p1b");

      await schemaPartitions.registerClient(client, "c1", true);

      const m1 = await schemaPartitions.get(client, "c1", "t1");
      const m2 = await schemaPartitions.get(client, "c1", "t1");
      await Promise.all([m1.commit(), m2.commit()]);

      const m3 = await schemaPartitions.get(client, "c1", "t1");
      await m3.commit();

      const m4 = await schemaPartitions.get(client, "c1", "t1");
      await m4.commit();

      const r2 = await client.query(`
        SELECT * FROM "fq"."locks" AS "t1"
        JOIN "fq"."partitions" AS "t2"
          ON "t1"."topic" = "t2"."topic"
         AND "t1"."partition" = "t2"."partition"
         AND "t1"."offset" = "t2"."offset"
      `);
      expect(r2.rowCount).toBe(2);
    });

    test("It should entirely consume a topic even if there are messages from different topics in the log", async () => {
      await schemaPartitions.put(client, { c: "t1-1" }, "t1");
      await schemaPartitions.put(client, { c: "t2-1" }, "t2");
      await schemaPartitions.put(client, { c: "t1-2" }, "t1");
      await schemaPartitions.put(client, { c: "t2-2" }, "t2");
      await schemaPartitions.put(client, { c: "t1-3" }, "t1");

      await schemaPartitions.registerClient(client, "c1", true);

      // Start consuming topic "t1"

      const m1 = await schemaPartitions.get(client, "c1", "t1");
      await m1.commit();
      expect(m1.payload.c).toBe("t1-1");

      const m2 = await schemaPartitions.get(client, "c1", "t1");
      await m2.commit();
      expect(m2.payload.c).toBe("t1-2");

      const m3 = await schemaPartitions.get(client, "c1", "t1");
      await m3.commit();
      expect(m3.payload.c).toBe("t1-3");

      const m4 = await schemaPartitions.get(client, "c1", "t1");
      expect(m4).toBe(null);

      // Start consuming topic "t2"

      const m5 = await schemaPartitions.get(client, "c1", "t2");
      await m5.commit();
      expect(m5.payload.c).toBe("t2-1");

      const m6 = await schemaPartitions.get(client, "c1", "t2");
      await m6.commit();
      expect(m6.payload.c).toBe("t2-2");

      const m7 = await schemaPartitions.get(client, "c1", "t2");
      expect(m7).toBe(null);
    });
  });

  describe("SUBSCRIPTIONS", () => {
    beforeEach(async () => {
      await schemaSubscriptions.reset(client);
      await schemaSubscriptions.create(client);
    });

    it("should create locks after a subscription is activated", async () => {
      // Should create some partitions definition
      await schemaSubscriptions.put(client, { c: 1 }, "t1", "p1");
      await schemaSubscriptions.put(client, { c: 2 }, "t1", "p2");
      await schemaSubscriptions.put(client, { c: 1 }, "t2", "p1");

      // Should upsert the locks for "c1/t1"
      await schemaSubscriptions.subscribe(client, "c1", "t1");

      const r1 = await client.query(`
        SELECT * FROM "fq"."locks"
        WHERE "topic" = 't1' AND "client" = 'c1'
      `);
      expect(r1.rowCount).toBe(2);

      // Should upsert the locks for "c1/t2"
      await schemaSubscriptions.subscribe(client, "c1", "t2");

      const r2 = await client.query(`
        SELECT * FROM "fq"."locks"
        WHERE "topic" = 't2' AND "client" = 'c1'
      `);
      expect(r2.rowCount).toBe(1);
    });

    it("should be idempotent in registering clients and subscriptions", async () => {
      await Promise.all([
        schemaSubscriptions.subscribe(client, "c1", "t1"),
        schemaSubscriptions.subscribe(client, "c1", "t1"),
      ]);
    });

    it("should add the locks for active subscriptions after adding new messages", async () => {
      await schemaSubscriptions.subscribe(client, "c1", "t1");
      await schemaSubscriptions.put(client, { c: 1 }, "t1", "p1");
      await schemaSubscriptions.put(client, { c: 1 }, "t1", "p2");

      const r1 = await client.query(`
        SELECT * FROM "fq"."locks"
        WHERE "topic" = 't1' AND "client" = 'c1'
      `);
      expect(r1.rowCount).toBe(2);
    });

    it("should subscribe to an existing topic since the beginning of time", async () => {
      const m1 = await schemaSubscriptions.put(client, { c: 1 }, "t1", "p1");

      await schemaSubscriptions.subscribe(client, "c1", "t1", true);

      const r1 = await schemaSubscriptions.get(client, "c1", "t1");
      expect(m1.topic).toEqual(r1.topic);
      expect(m1.partition).toEqual(r1.partition);
      expect(m1.offset).toEqual(r1.offset);
    });

    it("should subscribe to an existing topic since the end of time", async () => {
      await schemaSubscriptions.put(client, { c: 1 }, "t1", "p1");

      await schemaSubscriptions.subscribe(client, "c1", "t1");

      const r1 = await schemaSubscriptions.get(client, "c1", "t1");
      expect(r1).toBe(null);

      const m2 = await schemaSubscriptions.put(client, { c: 2 }, "t1", "p1");
      const r2 = await schemaSubscriptions.get(client, "c1", "t1");
      expect(m2.topic).toEqual(r2.topic);
      expect(m2.partition).toEqual(r2.partition);
      expect(m2.offset).toEqual(r2.offset);
    });

    it("should subscribe to an existing topic since a specific point in time", async () => {
      const m1 = await schemaSubscriptions.put(client, { c: 1 }, "t1", "p1");
      const m2 = await schemaSubscriptions.put(client, { c: 2 }, "t1", "p1");
      const m3 = await schemaSubscriptions.put(client, { c: 3 }, "t1", "p1");

      await schemaSubscriptions.subscribe(client, "c1", "t1", m2.createdAt);

      const r1 = await schemaSubscriptions.get(client, "c1", "t1");
      expect(r1.topic).toEqual(m2.topic);
      expect(r1.partition).toEqual(m2.partition);
      expect(r1.offset).toEqual(m2.offset);
      expect(r1.payload.c).toEqual(m2.payload.c);
    });

    it("should rewind a subscription", async () => {
      const m1 = await schemaSubscriptions.put(client, { c: 1 }, "t1", "p1");
      const m2 = await schemaSubscriptions.put(client, { c: 2 }, "t1", "p1");
      const m3 = await schemaSubscriptions.put(client, { c: 3 }, "t1", "p1");

      await schemaSubscriptions.subscribe(client, "c1", "t1");

      const r1 = await schemaSubscriptions.get(client, "c1", "t1");
      expect(r1).toBe(null);

      // Rewind the subscription to the beginning
      await schemaSubscriptions.subscribe(client, "c1", "t1", true);
      const r2 = await schemaSubscriptions.get(client, "c1", "t1");
      expect(r2.topic).toEqual(m1.topic);
      expect(r2.partition).toEqual(m1.partition);
      expect(r2.offset).toEqual(m1.offset);
      expect(r2.payload.c).toEqual(m1.payload.c);

      // Just check that the cursor can move forward
      await schemaSubscriptions.commit(client, r2);
      const r3 = await schemaSubscriptions.get(client, "c1", "t1");
      expect(r3.topic).toEqual(m2.topic);
      expect(r3.partition).toEqual(m2.partition);
      expect(r3.offset).toEqual(m2.offset);
      expect(r3.payload.c).toEqual(m2.payload.c);

      // Just check that the cursor can move forward
      await schemaSubscriptions.commit(client, r3);
      const r4 = await schemaSubscriptions.get(client, "c1", "t1");
      expect(r4.topic).toEqual(m3.topic);
      expect(r4.partition).toEqual(m3.partition);
      expect(r4.offset).toEqual(m3.offset);
      expect(r4.payload.c).toEqual(m3.payload.c);

      // Just check that the cursor can move forward
      await schemaSubscriptions.commit(client, r4);
      const r5 = await schemaSubscriptions.get(client, "c1", "t1");
      expect(r5).toBe(null);

      // Now rewind to a specific point in time
      await schemaSubscriptions.subscribe(client, "c1", "t1", m2.createdAt);
      const r6 = await schemaSubscriptions.get(client, "c1", "t1");
      expect(r6.topic).toEqual(m2.topic);
      expect(r6.partition).toEqual(m2.partition);
      expect(r6.offset).toEqual(m2.offset);
      expect(r6.payload.c).toEqual(m2.payload.c);
    });

    it("should not be able to get messages without a subscription", async () => {
      await schemaSubscriptions.put(client, { c: 1 }, "t1", "p1");
      const r1 = await schemaSubscriptions.get(client, "c1", "t1");
      expect(r1).toBe(null);
    });
  });
});
