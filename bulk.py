from parquet2bigquery.logs import configure_logging
from parquet2bigquery.lib import bulk
import argparse

log = configure_logging()


def bulk_load_parquet(bucket_name, prefix, concurrency=10):
    bulk(bucket_name, prefix, concurrency)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("-b", "--bucket",
                        help="GCS Bucket",
                        action="store", required=True)

    parser.add_argument("-p", "--prefix",
                        help="Object Prefix",
                        action="store", required=True)

    parser.add_argument("-c", "--concurrency",
                        help="Process concurrency",
                        default=10,
                        type=int,
                        action="store")

    args = parser.parse_args()

    bulk_load_parquet(args.bucket, args.prefix, args.concurrency)


main()
