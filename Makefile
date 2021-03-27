stress-partitions-db:
	TARGET=partitions docker-compose -f docker-compose.stress.yml up -d postgres

stress-partitions-setup:
	TARGET=partitions docker-compose -f docker-compose.stress.yml up setup
	TARGET=partitions docker-compose -f docker-compose.stress.yml rm -f setup

stress-partitions-producer:
	TARGET=partitions docker-compose -f docker-compose.stress.yml up reset
	TARGET=partitions docker-compose -f docker-compose.stress.yml up producer
	TARGET=partitions docker-compose -f docker-compose.stress.yml rm -f producer reset

stress-partitions-consumer:
	TARGET=partitions docker-compose -f docker-compose.stress.yml up consumer1 consumer2
	TARGET=partitions docker-compose -f docker-compose.stress.yml rm -f consumer1 consumer2

stress-partitions-results:
	TARGET=partitions docker-compose -f docker-compose.stress.yml up results
	TARGET=partitions docker-compose -f docker-compose.stress.yml rm -f results

stress-partitions:
	TARGET=partitions docker-compose -f docker-compose.stress.yml up reset
	TARGET=partitions docker-compose -f docker-compose.stress.yml up producer
	TARGET=partitions docker-compose -f docker-compose.stress.yml up consumer1 consumer2
	TARGET=partitions docker-compose -f docker-compose.stress.yml rm -f reset producer consumer1 consumer2
	TARGET=partitions docker-compose -f docker-compose.stress.yml up results
	TARGET=partitions docker-compose -f docker-compose.stress.yml rm -f results



stress-subscriptions-db:
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml up -d postgres

stress-subscriptions-setup:
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml up setup
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml rm -f setup

stress-subscriptions-producer:
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml up reset
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml up producer
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml rm -f producer reset

stress-subscriptions-consumer:
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml up consumer1 consumer2
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml rm -f consumer1 consumer2

stress-subscriptions-results:
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml up results
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml rm -f results

stress-subscriptions:
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml up reset
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml up producer
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml up consumer1 consumer2
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml rm -f reset producer consumer1 consumer2
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml up results
	TARGET=subscriptions docker-compose -f docker-compose.stress.yml rm -f results

