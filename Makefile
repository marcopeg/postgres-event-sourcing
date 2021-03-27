partitions-db:
	TARGET=partitions docker-compose -f docker-compose.stress.yml up -d postgres

partitions-setup:
	TARGET=partitions docker-compose -f docker-compose.stress.yml up setup
	TARGET=partitions docker-compose -f docker-compose.stress.yml rm -f setup

partitions-producer:
	TARGET=partitions docker-compose -f docker-compose.stress.yml up reset
	TARGET=partitions docker-compose -f docker-compose.stress.yml up producer
	TARGET=partitions docker-compose -f docker-compose.stress.yml rm -f producer reset

partitions-consumer:
	TARGET=partitions docker-compose -f docker-compose.stress.yml up consumer1 consumer2
	TARGET=partitions docker-compose -f docker-compose.stress.yml rm -f consumer1 consumer2

partitions-results:
	TARGET=partitions docker-compose -f docker-compose.stress.yml up results
	TARGET=partitions docker-compose -f docker-compose.stress.yml rm -f results

partitions-run:
	TARGET=partitions docker-compose -f docker-compose.stress.yml up reset
	TARGET=partitions docker-compose -f docker-compose.stress.yml up producer
	TARGET=partitions docker-compose -f docker-compose.stress.yml up consumer1 consumer2
	TARGET=partitions docker-compose -f docker-compose.stress.yml rm -f reset producer consumer1 consumer2
	TARGET=partitions docker-compose -f docker-compose.stress.yml up results
	TARGET=partitions docker-compose -f docker-compose.stress.yml rm -f results



subscriptions-db:
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml up -d postgres

subscriptions-setup:
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml up setup
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml rm -f setup

subscriptions-producer:
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml up reset
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml up producer
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml rm -f producer reset

subscriptions-consumer:
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml up consumer1 consumer2
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml rm -f consumer1 consumer2

subscriptions-results:
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml up results
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml rm -f results

subscriptions-run:
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml up reset
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml up producer
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml up consumer1 consumer2
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml rm -f reset producer consumer1 consumer2
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml up results
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml rm -f results

