#!/bin/bash

# Start Xvfb on display :99
Xvfb :99 -ac -screen 0 1280x1024x16 &

# Run Python script
python jofemar_dispense_scrapper.py
