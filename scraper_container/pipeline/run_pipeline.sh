#!/bin/bash

# Make the script executable
chmod +x /app/run_pipeline.sh

# Navigate to the app directory where scripts are mounted
cd /app

# Run the scraper script
echo "Running scraper..."
cd /app/ekstraligapl # Navigate to the Scrapy project directory
scrapy crawl ekstraliga_match
# Check if scraper ran successfully
if [ $? -eq 0 ]; then
  echo "Scraper finished successfully. Running transformer..."
  # Run the transformer script
  cd ..
  python data_transformer.py
  if [ $? -eq 0 ]; then
    echo "Transformer finished successfully."
  else
    echo "Transformer failed."
  fi
else
  echo "Scraper failed. Skipping transformer."
fi
