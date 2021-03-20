require("dotenv").config();
const { Client } = require("pg");
const schemaBasic = require("./schema.basic");
const schemaClient = require("./schema.client");
const schemaTopic = require("./schema.topic");
const schemaLocks = require("./schema.locks");
const schemaPartitions = require("./schema.partitions");

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

    it("should register a client and generate the related locks from existing partitions using the latest available offset", async () => {
      await schemaPartitions.put(client, { c: 0 }, "t1");
      await schemaPartitions.put(client, { c: 1 }, "t1", "p1");
      await schemaPartitions.put(client, { c: 2 }, "t1", "p2");
      await schemaPartitions.put(client, { c: 3 }, "t1", "p3");

      const c1 = await schemaPartitions.registerClient(client, "c1");
      expect(c1.id).toBe("c1");
      expect(c1.createdAt).toEqual(c1.updatedAt);

      // Should be idempotent, upserting the same client multiple time should
      // have no effects on the stored data, just modify the updatedAt info
      const c1b = await schemaPartitions.registerClient(client, "c1");
      expect(c1.id).toEqual(c1b.id);
      expect(c1b.updatedAt.getTime()).toBeGreaterThan(c1b.createdAt.getTime());

      // Should generate 4 locks, one per partition:
      const r1 = await client.query(`SELECT COUNT(*) FROM "fq"."locks"`);
      expect(Number(r1.rows[0].count)).toBe(4);

      const r2 = await client.query(`
        SELECT * FROM "fq"."locks"
        WHERE "client" = 'c1'
          AND "topic" = 't1'
          AND "partition" = 'p3'
      `);
      expect(Number(r2.rows[0].offset)).toBe(4);
    });

    it("should register a client and generate the related locks from existing partitions starting from the beginning of the available history", async () => {
      await schemaPartitions.put(client, { c: 0 });
      await schemaPartitions.registerClient(client, "c1", true);
      const r1 = await client.query(`SELECT * FROM "fq"."locks"`);
      expect(Number(r1.rows[0].offset)).toBe(-1);
    });

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

    // test("It should NOT return messages without registering a client", async () => {
    //   await schemaPartitions.put(client, "t1", "p1", { c: 1 });
    //   await schemaPartitions.put(client, "t1", "p1", { c: 2 });
    //   await schemaPartitions.put(client, "t1", "p1", { c: 3 });

    //   // The first read should fail as there is no client registered
    //   const m1 = await schemaPartitions.get(client, "c1", "t1");
    //   expect(m1).toBe(null);

    //   // The second read should work as the client is set up
    //   // await schemaLocks.registerClient(client, "c1", "t1");
    //   // const m2 = await schemaLocks.get(client, "c1", "t1");
    //   // expect(m2.payload.c).toBe(1);
    // });
  });
});
