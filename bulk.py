from parquet2bigquery.lib import bulk
import argparse


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
    parser.add_argument("-d", "--date-format",
                        dest='dateformat',
                        help="Date Format",
                        default='%Y%m%d',
                        action="store")

    glob_group = parser.add_mutually_exclusive_group()

    glob_group.add_argument("-g", "--glob-load",
                            dest='globload',
                            action="store_true")
    glob_group.add_argument("-ng", "--no-glob-load",
                            dest='globload',
                            action="store_false")

    parser.set_defaults(globload=True)

    resume_group = parser.add_mutually_exclusive_group()

    resume_group.add_argument("-r", "--resume",
                              dest='resumeload',
                              action="store_true")
    resume_group.add_argument("-nr", "--no-resume",
                              dest='resumeload',
                              action="store_false")

    parser.set_defaults(resumeload=False)

    args = parser.parse_args()

    bulk(args.bucket, args.prefix, args.concurrency, args.globload,
         args.resumeload, args.dateformat)


main()
