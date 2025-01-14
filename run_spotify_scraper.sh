#!/bin/bash

# Change to the script directory
cd /home/tenders/Documents/code/spotify_scraper

# Load environment variables if needed (adjust path as needed)
source ~/.zshrc
source .venv/bin/activate

# Run the scraper
python scrape_plays.py 