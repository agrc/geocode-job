FROM python:3.7.0-alpine3.7 as base
COPY geocode-gcs-csv.py /tmp/geocode-gcs-csv.py
COPY geocode-key.json /tmp/geocode-key.json
RUN pip install --upgrade google-cloud-storage
