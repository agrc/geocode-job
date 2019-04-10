FROM python:3.7.0-alpine3.7 as base
COPY geocode_gcs_csv.py /tmp/geocode_gcs_csv.py
RUN pip install --upgrade google-cloud-storage
