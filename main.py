from parquet2bigquery.logs import configure_logging
from parquet2bigquery.lib import run

log = configure_logging()


def load_parquet(data, context):

    log.info('Bucket: {}'.format(data['bucket']))
    log.info('File: {}'.format(data['name']))

    if context.event_type == 'google.storage.object.finalize':
        run(data['bucket'], data['name'])
