# Geocode-job

Run a containerized python geocoding script as a kubernetes job.

# add sub commands
### Steps to run
1. Prepare data
    1. Run [prep_addresses.py](vista/prep_addresses.py)
        - Pulls data from VISTA
        - Partions the data into multiple CSVs
1. Create k8s job yaml specifications
    1. Run [vista_job_template.py](vista/vista_job_template.py)
        - Uploads data to Cloud Storage with [service account credenitals](.secrets/gcs-gecode-writer.json.template)
        - Creates k8s job template files
1. Apply seceret for service worker with cloud storage permissions to k8s cluster
    1. authorize kubectl with geocoding api cluster
    1. run `kubectl apply -f .secrets/gcs-secret.yml`
        - Service account key must first be base64 encoded into [gcs-secret.yml](.secrets/gcs-secret.yml.template)
1. Apply job yamls to cluster 
    1. run `kubectl apply -f job.yaml`
1. Download geocoded CSVs from cloud storage

### Steps to build
1.   Build container from docker file
    1. docker build . -t {container name}
    1. docker tag {container name}:latest gcr.io/{project id}/webapi/{container name}:latest
1. Push to registery
    1. docker push gcr.io/{project id}/webapi/{container name}:latest
    1. User needs project permissions to allow push to gcr
