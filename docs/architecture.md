# Project Architecture: Speedway Data Pipeline

This project implements an automated pipeline to scrape speedway match data from `ekstraliga.pl`, transform it, and load it into a PostgreSQL database. The pipeline is containerized using Docker Compose and scheduled using cron.

## Components

The project architecture consists of the following key components:

1.  **Speedway Scraper (`ekstraliga_scraper` service):**
    *   A Docker container built from the `scraper_container` directory.
    *   Contains a Scrapy spider (`ekstraliga_match.py`) responsible for extracting raw match data from the source website.
    *   Saves the scraped data as JSON files to the `./output/ekstraliga_scraper` directory on the host machine via a mounted volume (`/app/output` in the container).
    *   Also contains the `run_pipeline.sh` and `data_transformer.py` scripts within the container via mounted volumes from `./scraper_container/pipeline`.

2.  **PostgreSQL Database (`db` service):**
    *   A Docker container running a PostgreSQL database instance.
    *   Stores the transformed speedway data.
    *   Data is persisted to the `./db_data` directory on the host machine via a mounted volume (`/var/lib/postgresql/data` in the container).
    *   The database port (5432) is exposed to the host machine, allowing external connections.
    *   The database schema is defined in the `database/schema.sql` file.

3.  **Data Transformer (`data_transformer.py` script):**
    *   A Python script located in the `scraper_container/pipeline` directory.
    *   Executed within the `ekstraliga_scraper` container.
    *   Reads the raw JSON data from the mounted `./output/ekstraliga_scraper` directory (`/app/output` in the container).
    *   Transforms the data according to the defined PostgreSQL schema.
    *   Loads the transformed data into the PostgreSQL database (`db` service).

4.  **Pipeline Orchestration (`run_pipeline.sh` script):**
    *   A bash script located in the `scraper_container/pipeline` directory.
    *   Executed within the `ekstraliga_scraper` container.
    *   Sequentially runs the Scrapy spider (`scrapy crawl ekstraliga_match`) and the `data_transformer.py` script.
    *   Ensures the scraper completes successfully before the transformer is executed.

5.  **Scheduler (`scheduler` service):**
    *   A Docker container responsible for scheduling the data pipeline execution.
    *   Uses cron to run commands at specified intervals.
    *   Mounts the cronjob file from `./scheduler_container/speedway_cron` on the host to `/etc/cron.d/speedway_cron` in the container.
    *   The cronjob is configured to execute the `run_pipeline.sh` script within the `ekstraliga_scraper` container using `docker-compose run --rm ekstraliga_scraper /app/run_pipeline.sh` every 3 days.

6.  **Docker Compose (`docker-compose.yml`):**
    *   Defines the multi-container application, including the `ekstraliga_scraper`, `db`, and `scheduler` services.
    *   Configures the build process, dependencies, environment variables, port mappings, and volume mounts for each service.
    *   Located in the root directory of the project.

## Data Flow

1.  The `scheduler` service triggers the execution of the `run_pipeline.sh` script within a new instance of the `ekstraliga_scraper` container based on the cron schedule.
2.  The `run_pipeline.sh` script first runs the Scrapy spider.
3.  The Scrapy spider scrapes data from `ekstraliga.pl` and saves it as JSON files in the mounted `./output/ekstraliga_scraper` directory on the host.
4.  Upon successful completion of the scraper, the `run_pipeline.sh` script executes the `data_transformer.py` script.
5.  The `data_transformer.py` script reads the JSON files from the mounted `./output/ekstraliga_scraper` directory (accessible within the `ekstraliga_scraper` container).
6.  The transformer connects to the `db` service (PostgreSQL database) using the internal Docker network and environment variables.
7.  The transformer processes the JSON data and inserts/updates records in the PostgreSQL database according to the `database/schema.sql`.
8.  After the `run_pipeline.sh` script finishes, the `ekstraliga_scraper` container exits (`--rm` flag in `docker-compose run`).
9.  The PostgreSQL database (`db` service) continues to run, persisting the data to the `./db_data` directory on the host.

This architecture provides a robust, automated, and containerized solution for collecting and storing speedway match data.
