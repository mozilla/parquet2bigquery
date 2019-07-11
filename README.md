# parquet2bigquery

## Development

Since this project makes heavy use of the bigquery parquet load mechanism, it's not feasible to test without actually running in BigQuery. To set up a project for development:

### Create a service account

Create a new service account for testing in a sandbox project. Give it BigQuery Data Owner, Storage Admin, and BigQuery Job User roles. Download the json file with credentials and place it in ~/.config/

### Create datasets in BigQuery

Create two datasets in BigQuery: the target datasets for final tables, and a temp dataset named `tmp`.

### Create a storage bucket with parquet files

Create a GCS bucket and populate it with some test parquet files.

### Build and run

Make your local changes, then build the docker image

```docker build -t parquet2bigquery .```

Run the project:
```docker run -v ~/.config:/root/.config -e GOOGLE_CLOUD_PROJECT=<sandbox project>  -e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/<service account key file> -it parquet2bigquery -b <gcs bucket> -d <target dataset> -p <parquet file location prefix>  -c30```
