#!/bin/bash

# Run this script if you use Python 2

# Install necessary libraries
echo "Installing necessary libraries"
sleep 2
pip install scrapy
pip install beautifulsoup4

# Reset terminal to use new libraries
reset
echo "Running crawler"
echo "Press CTRL+C to stop at any time(will take ~20 seconds)"
sleep 5

# Run the crawler
cd ../wiki
scrapy crawl Wiki -o wiki.jl
# Press CTRL+C to stop crawling. Will take ~20 seconds