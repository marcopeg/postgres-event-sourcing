partitions-db:
	docker-compose -f docker-compose.partitions.yml up -d postgres

partitions-setup:
	docker-compose -f docker-compose.partitions.yml up setup
	docker-compose -f docker-compose.partitions.yml rm -f setup

partitions-producer:
	docker-compose -f docker-compose.partitions.yml up reset
	docker-compose -f docker-compose.partitions.yml up producer
	docker-compose -f docker-compose.partitions.yml rm -f producer reset

partitions-consumer:
	docker-compose -f docker-compose.partitions.yml up consumer1 consumer2
	docker-compose -f docker-compose.partitions.yml rm -f consumer1 consumer2