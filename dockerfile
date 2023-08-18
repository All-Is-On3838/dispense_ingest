# Use an official Python runtime as a parent image
FROM python:3.9

# Allow statements and log messages to immediately appear in the Cloud Run logs
ENV PYTHONUNBUFFERED True

# Install dependencies
RUN apt-get update && apt-get install -y wget gnupg2 unzip
RUN apt-get update && apt-get install -y wget default-jre

# Install Google Chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y google-chrome-stable

# Download and install the ChromeDriver
RUN CHROME_DRIVER_VERSION=$(curl -sS https://chromedriver.storage.googleapis.com/LATEST_RELEASE) \
    && wget -q https://chromedriver.storage.googleapis.com/$CHROME_DRIVER_VERSION/chromedriver_linux64.zip \
    && unzip chromedriver_linux64.zip \
    && mv chromedriver /usr/local/bin/ \
    && rm chromedriver_linux64.zip

# Set chromedriver to PATH
ENV PATH="/usr/local/bin:${PATH}"

# Install Xvfb
RUN apt-get update && apt-get install -y xvfb

# Set up a virtual display for Xvfb
ENV DISPLAY=:99

# Copy requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Set the working directory to /app
WORKDIR /usr/app

# Copy the application files to the container
COPY .env jofemar_dispense_scrapper.py jofemar_webscrapper_service_account.json run.sh main.py /usr/app/

# make run.sh executable
RUN chmod +x /usr/app/run.sh

# make referenced folders
RUN mkdir /usr/app/logs
RUN mkdir /usr/app/dispense_csv

# Run the web service on container startup.
# Use gunicorn webserver with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app