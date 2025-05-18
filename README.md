# Speedway Data Scraper and Processor

This project scrapes speedway league data, processes it, and stores it in a PostgreSQL database. It uses Scrapy for scraping and Docker Compose to manage the services.

## Project Structure

- `scraper_container/`: Contains the Scrapy spider and data processing scripts.
- `db_data/`: Stores persistent PostgreSQL data (ignored by Git).
- `pgadmin_data/`: Stores persistent pgAdmin data (ignored by Git).
- `logs/`: Contains logs from the scraper and data transformer (ignored by Git).
- `output/`: Contains output files from the scraper and data transformer (ignored by Git).
- `docker-compose.yml`: Defines the services, networks, and volumes for the Docker application.
- `.env`: Contains environment-specific variables (e.g., database credentials). This file is not committed to Git.
- `.gitignore`: Specifies intentionally untracked files that Git should ignore.

## Prerequisites

- Docker
- Docker Compose

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd <repository-name>
    ```

2.  **Create a `.env` file:**
    Copy the `.env.example` file to `.env` and update the variables with your specific settings.
    ```bash
    cp .env.example .env
    ```
    Example content for `.env.example`:
    ```env
    POSTGRES_USER=your_db_user
    POSTGRES_PASSWORD=your_db_password
    POSTGRES_DB=your_db_name
    PGADMIN_DEFAULT_EMAIL=your_pgadmin_email@example.com
    PGADMIN_DEFAULT_PASSWORD=your_pgadmin_password
    ```

## Running the Project

1.  **Build and start the services:**
    ```bash
    docker-compose up -d --build
    ```
    This command will build the images if they don't exist and start all services in detached mode.

2.  **Accessing Services:**
    -   **pgAdmin (Database Management):** Open your browser and go to `http://localhost:5050`. Log in with the `PGADMIN_DEFAULT_EMAIL` and `PGADMIN_DEFAULT_PASSWORD` defined in your `.env` file. You will need to add a new server connection to the `db` service (hostname: `db`, port: `5432`, username/password from `.env`).
    -   **PostgreSQL Database:** Accessible on port `5432` from the host machine or other services within the Docker network.

3.  **Viewing Logs:**
    Logs for the scraper and data transformer can be found in the `./logs` directory on your host machine. You can also view live logs using:
    ```bash
    docker-compose logs -f ekstraliga_scraper
    docker-compose logs -f db
    docker-compose logs -f pgadmin
    ```

4.  **Stopping the services:**
    ```bash
    docker-compose down
    ```

## Contributing

Please refer to the project's issue tracker for areas to contribute.
