partitions-db:
	docker-compose -f docker-compose.partitions.yml up -d postgres

partitions-setup:
	docker-compose -f docker-compose.partitions.yml up setup
	docker-compose -f docker-compose.partitions.yml rm -f setup

partitions-producer:
	docker-compose -f docker-compose.partitions.yml up producer
	docker-compose -f docker-compose.partitions.yml rm -f producer

partitions-consumer:
	docker-compose -f docker-compose.partitions.yml up consumer
	docker-compose -f docker-compose.partitions.yml rm -f consumer