require("dotenv").config();
const { Pool } = require("pg");
const schemaLocks = require("./schema.locks");
const Producer = require("./producer");
const Consumer = require("./consumer");

describe("Clients", () => {
  // Connect to PG
  const client = new Pool({ connectionString: process.env.PGSTRING });
  afterAll(() => client.end());

  describe("Producer", () => {
    beforeEach(async () => {
      await schemaLocks.reset(client);
      await schemaLocks.create(client);
    });

    test("A Producer should write to the queue", async () => {
      const p1 = new Producer(client);
      const w1 = await p1.write({ a: 1 });
      expect(w1.payload.a).toBe(1);

      await schemaLocks.registerClient(client);
      const m1 = await schemaLocks.get(client);
      expect(m1.payload.a).toBe(1);
      expect(m1.offset).toEqual(w1.offset);

      await schemaLocks.registerClient(client, "*", "t1");
      const w2 = await p1.write({ a: 1 }, "t1");
      const m2 = await schemaLocks.get(client, "*", "t1");
      expect(m2.offset).toEqual(w2.offset);
    });

    test("A Consumer should receive a message", () =>
      new Promise(async (resolve) => {
        const makeHanlder = (clientId, delay = 100) =>
          jest.fn(async (doc) => {
            console.log(`[${clientId}] `, doc.payload);
            await new Promise((resolve) => setTimeout(resolve, delay));
            return doc.commit();
          });

        const c1 = new Consumer(client, {
          name: "c1",
          dataDelay: 10,
          dataSleep: 10,
        });
        const c1Handler = makeHanlder("c1");
        await c1.start(c1Handler);

        const c2 = new Consumer(client, {
          name: "c2",
          dataDelay: 10,
          dataSleep: 10,
        });
        const c2Handler = makeHanlder("c2");
        await c2.start(c2Handler);

        // Duplicates client "C1"
        // here we use a small delay just to force the execution
        // order of the queries and make sure we can run predictable
        // expectations on it
        await new Promise((resolve) => setTimeout(resolve, 0));
        const c1b = new Consumer(client, {
          name: "c1",
          dataDelay: 10,
          dataSleep: 10,
        });
        const c1bHandler = makeHanlder("c1b");
        await c1b.start(c1bHandler);

        // Produce one single message:
        const p1 = new Producer(client);
        await p1.write({ a: 1 });

        setTimeout(async () => {
          await Promise.all([c1.stop(), c1b.stop(), c2.stop()]);

          expect(c1Handler.mock.calls.length).toBe(1);
          expect(c1Handler.mock.calls[0][0].payload.a).toBe(1);

          expect(c2Handler.mock.calls.length).toBe(1);
          expect(c2Handler.mock.calls[0][0].payload.a).toBe(1);

          expect(c1bHandler.mock.calls.length).toBe(0);

          resolve();
        }, 150);
      }));
  });
});
