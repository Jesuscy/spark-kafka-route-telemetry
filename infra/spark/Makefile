build:
	docker compose build

build-nc:
	docker compose build --no-cache

build-progress:
	docker compose build --no-cache --progress=plain

down:
	docker compose down --volumes

run:
	make down && docker compose up

run-scaled:
	make down && docker compose up --scale spark-worker=3

run-d:
	make down && docker compose up -d

stop:
	docker compose stop

submit-landing:
	docker exec da-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark/jobs/streaming_job.py

submit-staging:
	docker exec da-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark/jobs/landing_to_staging.py

submit-staging-batch:
	docker exec da-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark/jobs/landing_to_staging_batch.py

submit-prepare-cities:
	docker exec da-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark/jobs/prepare_localities.py

