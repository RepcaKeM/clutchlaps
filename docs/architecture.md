# Project Architecture: Speedway Data Pipeline

This project implements an automated pipeline to scrape speedway match data from `ekstraliga.pl`, transform it, and load it into a PostgreSQL database. The pipeline is containerized using Docker Compose.

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
    *   The database schema is defined in `db_data/schema.sql`.

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

5.  **Docker Compose (`docker-compose.yml`):**
    *   Defines the multi-container application, including the `ekstraliga_scraper` and `db` services (and `pgadmin` for database management).
    *   Configures the build process, dependencies, environment variables, port mappings, and volume mounts for each service.
    *   Located in the root directory of the project.

## Data Flow

1.  When the `ekstraliga_scraper` service is started (e.g., via `docker-compose up`), its defined command executes the `run_pipeline.sh` script.
2.  The `run_pipeline.sh` script first runs the Scrapy spider.
3.  The Scrapy spider scrapes data from `ekstraliga.pl` and saves it as JSON files in the mounted `./output/ekstraliga_scraper` directory on the host.
4.  Upon successful completion of the scraper, the `run_pipeline.sh` script executes the `data_transformer.py` script.
5.  The `data_transformer.py` script reads the JSON files from the mounted `./output/ekstraliga_scraper` directory (accessible within the `ekstraliga_scraper` container).
6.  The transformer connects to the `db` service (PostgreSQL database) using the internal Docker network and environment variables (sourced from `.env`).
7.  The transformer processes the JSON data and inserts/updates records in the PostgreSQL database according to the `db_data/schema.sql` file.
8.  The `ekstraliga_scraper` service executes the `run_pipeline.sh` script upon starting. Once the script completes, the container will stop as it's not designed as a long-running service but rather a task runner.
9.  The PostgreSQL database (`db` service) and `pgadmin` service continue to run, persisting data as configured.

This architecture provides a robust, automated, and containerized solution for collecting and storing speedway match data.
