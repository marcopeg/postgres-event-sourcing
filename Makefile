on?="partitions"

stress-cleanup:
	# TARGET=${on} docker-compose -f docker-compose.stress.yml stop postgres
	# TARGET=${on} docker-compose -f docker-compose.stress.yml rm -f postgres

stress-setup:
	TARGET=${on} docker-compose -f docker-compose.stress.yml up setup
	TARGET=${on} docker-compose -f docker-compose.stress.yml rm -f setup

stress-reset:
	TARGET=${on} docker-compose -f docker-compose.stress.yml up reset
	TARGET=${on} docker-compose -f docker-compose.stress.yml rm -f reset

stress-run-producer:
	TARGET=${on} docker-compose -f docker-compose.stress.yml up producer
	TARGET=${on} docker-compose -f docker-compose.stress.yml rm -f producer

stress-run-consumer:
	TARGET=${on} docker-compose -f docker-compose.stress.yml up consumer1 consumer2
	TARGET=${on} docker-compose -f docker-compose.stress.yml rm -f consumer1 consumer2

stress-run-results:
	TARGET=${on} docker-compose -f docker-compose.stress.yml up results
	TARGET=${on} docker-compose -f docker-compose.stress.yml rm -f results

stress-run: stress-reset stress-run-producer stress-run-consumer stress-run-results stress-cleanup
