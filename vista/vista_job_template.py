"""Create Kubernetes job templates for and upload files for geocoding."""
import jinja2
from os import listdir, mkdir
from os.path import isfile, join, exists, basename
import sys
import base64
from google.cloud import storage

GCS_UPLOAD_KEY = '../.secrets/gcs-geocode-writer.json'


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client.from_service_account_json(GCS_UPLOAD_KEY)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

UPLOAD_BUCKET = 'geocoder-csv-storage-95728'

def get_template_args(csv_directory, id_field, address_field, zone_field, upload_bucket, results_bucket, upload=True):
    """Get job template args from csv files in upload directory and optionally upload csvs to Cloud Storage."""
    job_csvs = [f for f in listdir(csv_directory) if isfile(join(csv_directory, f))]
    job_template_args = []
    for job_num, job_csv in enumerate(job_csvs):
        if upload:
            upload_blob(UPLOAD_BUCKET, join(csv_directory, job_csv), job_csv)
            print(job_csv, 'uploaded')
        job_template_args.append({
            'job_number': job_num,
            'csv_name': job_csv,
            'id_field': id_field,
            'address_field': address_field,
            'zone_field': zone_field,
            'upload_bucket': upload_bucket,
            'results_bucket': results_bucket
        })
    return job_template_args

def create_job_ymls(job_template_args, job_template_dir, job_template_name, output_dir, upload=True):
    """Create k8s job specs that can deployed to cluster to start geocoding."""
    for i, template_args in enumerate(job_template_args):
        template_loader = jinja2.FileSystemLoader(searchpath=job_template_dir)
        template_env = jinja2.Environment(loader=template_loader)
        template_file = job_template_name
        template = template_env.get_template(template_file)
        output_text = template.render(template_args)
        job_yml = 'vista-job-{}.yml'.format(i)
        job_yml_path = join(output_dir, job_yml)
        
        if not exists(output_dir):
            mkdir(output_dir)
        with open(job_yml_path, 'w') as output_template:
            output_template.write(output_text)
        if upload:
            upload_blob(UPLOAD_BUCKET, job_yml_path, job_yml)
            print(job_yml, 'uploaded')

def create_secret_yml(secret_template_dir, secret_template_name, gcs_key_name, upload=True):
    base64_key = None
    with open(join(secret_template_dir, gcs_key_name), 'r') as f:
        key = f.read()
        base64_key = base64.b64encode(key.encode('utf-8'))
    
    template_args = {'base64_key': base64_key.decode('utf-8')}
    
    template_loader = jinja2.FileSystemLoader(searchpath=secret_template_dir)
    template_env = jinja2.Environment(loader=template_loader)
    template_file = secret_template_name
    template = template_env.get_template(template_file)
    output_text = template.render(template_args)
    secret_yml = 'gcs-secret.yml'
    secret_yml_path = join(secret_template_dir, secret_yml)
    
    with open(secret_yml_path, 'w') as output_template:
        output_template.write(output_text)
    if upload:
        upload_blob(UPLOAD_BUCKET, secret_yml_path, secret_yml)
        print(secret_yml, 'uploaded')

if __name__ == '__main__':
    csv_directory = 'data/job_uploads'
    id_field = 'RESIDENCE_ID'
    address_field = 'VISTA_ADDRESS'
    zone_field = 'VISTA_CITY'
    upload_bucket = 'geocoder-csv-storage-95728'
    results_bucket = 'geocoder-csv-results-98576'
    # Create arguments for job template
    job_template_args = get_template_args(
        csv_directory,
        id_field,
        address_field,
        zone_field,
        upload_bucket,
        results_bucket)
    # Use arguments to create and upload template
    job_template_dir = '../.kube'
    job_template_name = 'geocoder-template.yml.jinja2'
    output_dir = 'jobs'
    create_job_ymls(
        job_template_args,
        job_template_dir,
        job_template_name,
        output_dir)
    # Create and upload secret
    secret_dir = '../.secrets'
    secret_template_name = 'gcs-secret-template.yml.jinja2'
    gcs_key_name = basename(GCS_UPLOAD_KEY)
    create_secret_yml(
        secret_dir,
        secret_template_name,
        gcs_key_name)
