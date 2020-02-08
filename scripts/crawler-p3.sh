#!/bin/bash

# Install necessary libraries
echo "Installing necessary libraries"
pip3 install scrapy
pip3 install beautifulsoup4

# Reset terminal to use new libraries
reset
echo "Running crawler"

# Run the crawler
cd ../wiki
scrapy crawl Wiki -o wiki.jl
# Press CTRL+C to stop crawling. Will take ~20 seconds