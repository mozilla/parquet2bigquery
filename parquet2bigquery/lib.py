import google.api_core.exceptions
import logging
import re
import secrets

from google.cloud import storage
from google.cloud import bigquery
from google.cloud.bigquery.table import TimePartitioning
from google.cloud.bigquery.table import TimePartitioningType

from dateutil.parser import parse
from datetime import datetime

from multiprocessing import Process, JoinableQueue, Lock


# sample message 2019-02-07 12:34:55,439 root WARNING yay
logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s %(message)s')

# defaults
DEFAULT_DATASET = 'telemetry'
DEFAULT_TMP_DATASET = 'tmp'

IGNORE_PATTERNS = [
    r'.*/$',  # dirs
    r'.*/_[^=/]*/',  # temp dirs
    r'.*/_[^/]*$',  # temp files
    r'.*/[^/]*\$folder\$/?',  # metadata dirs and files
    r'.*/\.spark-staging.*$',  # spark staging dirs
]


class P2BWarning(Exception):
    pass


def get_bq_client(table_id, dataset):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table_id)

    return client, table_ref


def get_date_format(date):
    """
    Attempt to determine the date format.
    """
    date_formats = [
        '%Y%m%d',
        '%Y-%m-%d'
     ]

    for date_format in date_formats:
        try:
            datetime.strptime(date, date_format)
            logging.info("date format {} detected.".format(date_format))
            return date_format
        except ValueError:
            continue

    logging.error('Date format not detected, exiting.')
    exit


def gen_rand_string(size=5):
    """
    Generate a random string.
    """
    return secrets.token_hex(size)


def ignore_key(key, exclude_regex=[]):
    """
    Ignore a string based on IGNORE_PATTERNS.
    """
    return any([re.match(pat, key) for pat in IGNORE_PATTERNS + exclude_regex])


def normalize_table_id(table_name):
    """
    Normalize table name for use with BigQuery.
    """
    return table_name.replace("-", "_").lower()


def _get_object_key_metadata(object_key):
    """
    Parse object key and return useful metadata.

    sample object_key:
    'table_name/vtable_version/date_partition=x/first_partition=y/...'

    Args:
        object_key - contains the gcs object key (str)
    Returns:
        A dict which contains:
        partitions: non date partitions (list)
        table_id: derived table_id (str)
        date_partition: (dict)
            format: date time format (str)
            value: date value (str)
            field: date partition field name (str)
    """

    meta = {
        'partitions': [],
        'date_partition': {}
    }

    split_key = object_key.split('/')

    first_part_idx = next([index for index, elem in enumerate(split_key)
                          if '=' in elem])

    table_version = split_key[first_part_idx - 1]
    table_name = split_key[first_part_idx - 2]

    meta['first_part_idx'] = first_part_idx

    meta['table_id'] = normalize_table_id('_'.join([table_name,
                                                    table_version]))

    date_field, date_value = split_key[first_part_idx].split('=')
    meta['date_partition']['field'] = date_field
    meta['date_partition']['format'] = get_date_format(date_value)
    meta['date_partition']['value'] = parse(date_value).strftime('%Y-%m-%d')

    # try to get additional partition information
    extra_partitions = [elem.split('=')
                        for elem in split_key[first_part_idx+1]
                        if '=' in elem]
    meta['partitions'] += extra_partitions

    return meta


def create_bq_table(table_id, dataset, schema=None, partition_field=None):
    """
    Create a BigQuery table.
    """

    client, table_ref = get_bq_client(table_id, dataset)
    table = bigquery.Table(table_ref, schema=schema)

    if partition_field:
        _tp = TimePartitioning(type_=TimePartitioningType.DAY,
                               field=partition_field)
        table.time_partitioning = _tp

    table = client.create_table(table)

    assert table.table_id == table_id
    logging.info('%s: table created.' % table_id)


def get_bq_table_schema(table_id, dataset):
    """
    Get a BigQuery table schema.
    """

    client, table_ref = get_bq_client(table_id, dataset)

    table = client.get_table(table_ref)

    return table.schema


def update_bq_table_schema(table_id, schema_additions, dataset):
    """
    Update a BigQuery table schema.
    """

    client, table_ref = get_bq_client(table_id, dataset)

    table = client.get_table(table_ref)
    new_schema = table.schema[:]

    for field in schema_additions:
        new_schema.append(field)

    table.schema = new_schema
    table = client.update_table(table, ['schema'])
    logging.info('%s: BigQuery table schema updated.' % table_id)


def generate_bq_schema(table_id, dataset, date_partition_field=None,
                       partitions=None):
    """
    Generate a BigQuery schema based on the current BigQuery schema
    and appending object key metadata like date partition and other
    partition information.
    """
    p_fields = []

    schema = get_bq_table_schema(table_id, dataset)

    if date_partition_field:
        p_fields.append(bigquery.SchemaField(date_partition_field, 'DATE',
                                             mode='REQUIRED'))
    # we want to add the partitions to the schema
    for part, _ in partitions:
        p_fields.append(bigquery.SchemaField(part, 'STRING',
                                             mode='REQUIRED'))

    return p_fields + schema


def load_parquet_to_bq(bucket, object, table_id, dataset, schema=None,
                       partition=None):
    """
    Load parquet data into BigQuery.
    """

    client, table_ref = get_bq_client(table_id, dataset)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.PARQUET
    if schema:
        job_config.schema = schema
    job_config.schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
    ]

    uri = 'gs://%s/%s' % (bucket, object)

    if partition:
        table_id = '%s$%s' % (table_id, partition)

    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config)
    assert load_job.job_type == 'load'
    load_job.result()
    logging.info('%s: Parquet file %s loaded into BigQuery.' % (table_id, object))


def _compare_columns(col1, col2):
    """
    Compare two columns to see if they are changing.
    """

    a = []
    if isinstance(col1, tuple):
        for i in range(len(col1)):
            a.append(_compare_columns(col1[i], col2[i]))

    if isinstance(col1, google.cloud.bigquery.schema.SchemaField):
        if col1.fields and col2.fields:
            a.append(_compare_columns(col1.fields, col2.fields))

        # if mode is changing from required to nullable ignore
        if(col1.mode == 'REQUIRED' and col2.mode == 'NULLABLE'):
            return None


def get_schema_diff(schema1, schema2):
    """
    Compare two BigQuery table schemas and get the additional columns.
    schema2 should contain the latest schema and schema1 should be
    the current schema. We only append additional columns.
    """
    s = []
    diff = set(schema2) - set(schema1)

    for d in diff:
        found = False
        for o in schema1:
            if d.name == o.name:
                found = True
                val = (_compare_columns(d, o))
                if val:
                    s.append(d)
                    break
        if not found:
            s.append(d)
    return(s)


def construct_select_query(table_id, date_partition, partitions=None,
                           dataset=DEFAULT_TMP_DATASET):

    """
    Construct a query to select all data from a temp table, append
    the relevant partitions and output into the primary table
    """

    s_items = ['SELECT *']

    s_items.append("CAST('{0}' AS DATE) as {1}".format(date_partition[1],
                                                       date_partition[0]))

    part_as = "'{1}' as {0}"
    for partition in partitions:
        s_items.append(part_as.format(*partition))

    _tmp_s = ','.join(s_items)

    query = """
    {0}
    FROM {1}.{2}
    """.format(_tmp_s, dataset, table_id)

    return query


def check_bq_partition_exists(table_id, date_partition, dataset,
                              partitions=None):
    """
    Based on available partition information for object key
    construct a query to determine what partitions are currently in a
    BigQuery table.
    """

    client, table_ref = get_bq_client(table_id, dataset)

    job_config = bigquery.QueryJobConfig()
    job_config.use_query_cache = False

    count = 0
    s_items = []
    part_as = "{0} = '{1}'"

    s_items.append(part_as.format(date_partition[0], date_partition[1]))

    for partition in partitions:
        s_items.append(part_as.format(*partition))

    _tmp_s = ' AND '.join(s_items)

    query = """
    SELECT 1
    FROM {0}.{1}
    WHERE {2}
    LIMIT 10
    """.format(dataset, table_id, _tmp_s)

    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    for row in results:
        count += 1

    if count == 0:
        return False
    else:
        return True


def load_bq_query_to_table(query, table_id, dataset):
    """
    Execute constructed query to load data into BigQuery.
    """

    job_config = bigquery.QueryJobConfig()
    client, table_ref = get_bq_client(table_id, dataset)

    job_config.destination = table_ref
    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND

    query_job = client.query(query, job_config=job_config)
    query_job.result()
    logging.info('{}: query results loaded.'.format(table_id))


def check_bq_table_exists(table_id, dataset):
    """
    Check to see if a BigQuery table exists.
    """

    client, table_ref = get_bq_client(table_id, dataset)

    try:
        table = client.get_table(table_ref)
        if table:
            logging.info('{}: table exists.'.format(table_id))
            return True
    except google.api_core.exceptions.NotFound:
        logging.info('{}: table does not exist.'.format(table_id))
        return False


def delete_bq_table(table_id, dataset=DEFAULT_TMP_DATASET):
    """
    Delete a BigQuery table.
    """
    client, table_ref = get_bq_client(table_id, dataset)

    try:
        client.delete_table(table_ref)
        logging.info('{}: table deleted.'.format(table_id))
    except google.api_core.exceptions.NotFound:
        pass


def list_blobs_with_prefix(bucket_name, prefix, delimiter=None):
    """
    Return a list of all objects in a bucket prefix.
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix, delimiter=delimiter)

    objects = []

    for blob in blobs:
        if not ignore_key(blob.name):
            objects.append(blob.name)

    return objects


def get_latest_object(bucket_name, prefix, delimiter=None):
    """
    Get the latest object in a bucket prefix.
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix, delimiter=delimiter)

    latest_objects = {}
    latest_objects_tmp = {}

    for blob in blobs:
        if not ignore_key(blob.name):
            dir = '/'.join(blob.name.split('/')[0:-1])
            if latest_objects_tmp.get(dir):
                if latest_objects_tmp[dir] < blob.updated:
                    latest_objects_tmp[dir] = blob.updated
                    latest_objects[dir] = blob.name
            else:
                latest_objects_tmp[dir] = blob.updated
                latest_objects[dir] = blob.name

    return latest_objects


def create_primary_bq_table(table_id, new_schema, date_partition_field,
                            dataset):
    """
    Crate the primary BigQuery table for a imported dataset
    """
    if not check_bq_table_exists(table_id, dataset):
        try:
            create_bq_table(table_id, dataset, new_schema,
                            date_partition_field)
        except google.api_core.exceptions.Conflict:
            pass


def run(bucket_name, object, dest_dataset, dir=None, lock=None, alias=None):
    """
    Take an object(s) load it into BigQuery
    """

    # We don't care about these objects
    if ignore_key(object):
        logging.warning('Ignoring {}.'.format(object))
        return

    meta = _get_object_key_metadata(object)

    if alias:
        table_id = alias
    else:
        table_id = meta['table_id']

    date_partition_field = meta['date_partition']['field']
    date_partition_value = meta['date_partition']['value']

    table_id_tmp = normalize_table_id('_'.join([meta['table_id'],
                                      date_partition_value,
                                      gen_rand_string()]))

    query = construct_select_query(table_id_tmp, meta['date_partition'],
                                   partitions=meta['partitions'])

    # We assume that the data will have the following extensions
    if dir:
        obj = '{}/*'.format(dir)
        if object.endswith('parquet'):
            obj += 'parquet'
        elif object.endswith('internal'):
            obj += 'internal'
    else:
        obj = object

    # Create a temp table and load the data into temp table
    try:
        create_bq_table(table_id_tmp, DEFAULT_TMP_DATASET)
        load_parquet_to_bq(bucket_name, obj, table_id_tmp,
                           DEFAULT_TMP_DATASET)
    except (google.api_core.exceptions.InternalServerError,
            google.api_core.exceptions.ServiceUnavailable):
        delete_bq_table(table_id_tmp, dataset=DEFAULT_TMP_DATASET)
        logging.exception('%s: BigQuery Retryable Error.' % table_id)
        raise P2BWarning('BigQuery Retryable Error,')

    # Data is now loaded, we want to grab the schema of the table
    try:
        new_schema = generate_bq_schema(table_id_tmp,
                                        DEFAULT_TMP_DATASET,
                                        date_partition_field,
                                        meta['partitions'])
    except (google.api_core.exceptions.InternalServerError,
            google.api_core.exceptions.ServiceUnavailable):
        logging.exception('%s: GCS Retryable Error.' % table_id)
        raise P2BWarning('GCS Retryable Error.')

    # Try to create the primary BigQuery table
    if lock:
        lock.acquire()
        try:
            create_primary_bq_table(table_id, new_schema,
                                    date_partition_field, dest_dataset)
        finally:
            lock.release()
    else:
        create_primary_bq_table(table_id, new_schema,
                                date_partition_field, dest_dataset)

    # Compare temp table schema with primary table schema
    current_schema = get_bq_table_schema(table_id, dest_dataset)
    schema_additions = get_schema_diff(current_schema, new_schema)

    # If there are additions then update the primary table
    if len(schema_additions) > 0:
        update_bq_table_schema(table_id, schema_additions, dest_dataset)

    logging.info('%s: loading %s/%s to BigQuery table %s' % (table_id,
                                                             bucket_name,
                                                             obj,
                                                             table_id_tmp))
    # Try to load the temp table data into primary table
    try:
        load_bq_query_to_table(query, table_id, dest_dataset)
    except (google.api_core.exceptions.InternalServerError,
            google.api_core.exceptions.ServiceUnavailable):
        logging.exception('%s: BigQuery Retryable Error' % table_id)
        raise P2BWarning('BigQuery Retryable Error')
    finally:
        delete_bq_table(table_id_tmp)


def get_bq_table_partitions(table_id, date_partition_field,
                            dataset, partitions=[]):
    """
    Get all the partitions available in a BigQuery table.
    This is used for resume operations.
    """
    client, table_ref = get_bq_client(table_id, dataset)

    s_items = []
    reconstruct_paths = []

    s_items.append(date_partition_field)

    for partition in partitions:
        s_items.append(partition[0])

    _tmp_s = ','.join(s_items)

    query = """
    SELECT {2}
    FROM {0}.{1}
    GROUP BY {2}
    """.format(dataset, table_id, _tmp_s)

    query_job = client.query(query)
    results = query_job.result()

    for row in results:
        tmp_path = []
        for item in s_items:
            tmp_path.append('{}={}'.format(item, row[item]))
        reconstruct_paths.append('/'.join(tmp_path))

    return reconstruct_paths


def remove_loaded_objects(objects, dataset, alias):
    """
    Remove objects from list that have already been loaded
    into the BigQuery table.
    """
    initial_object_tmp = list(objects)[0]
    meta = _get_object_key_metadata(initial_object_tmp)
    path_prefix = initial_object_tmp.split('/')[:meta['first_part_idx']]

    if alias:
        table_id = alias
    else:
        table_id = meta['table_id']

    if not check_bq_table_exists(table_id, dataset):
        return objects

    object_paths = get_bq_table_partitions(table_id,
                                           meta['date_partition']['field'],
                                           dataset,
                                           meta['partitions'])

    for path in object_paths:
        spath = path.split('/')
        date_value = parse(spath[0].split('=')[1]).strftime(meta['date_partition']['format'])
        spath[0] = '='.join([meta['date_partition'][0]] + [date_value])
        key = '/'.join(path_prefix + spath)
        try:
            del objects[key]
            logging.debug('key {} already loaded into BigQuery'.format(key))
        except KeyError:
            continue

    return objects


def bulk(bucket_name, prefix, concurrency, glob_load, resume_load,
         dest_dataset=None, alias=None):
    """
    Load data into BigQuery concurrently
    Args:
        bucket_name: gcs bucket name (str)
        prefix: object key path, 'dataset/version' (str)
        concurrency: number of processes to handle the load (int)
        glob_load: load data by globbing path dirs (boolean)
        resume_load: resume load (boolean)
        dest_dataset: override default dataset location (str)
        alias: override object key dervived table name (str)
    """

    if dest_dataset:
        _dest_dataset = dest_dataset
    else:
        _dest_dataset = DEFAULT_DATASET

    logging.info('main_process: dataset set to {}'.format(_dest_dataset))

    q = JoinableQueue()
    lock = Lock()

    if glob_load:
        logging.info('main_process: loading via glob method')
        objects = get_latest_object(bucket_name, prefix)
        if resume_load:
            objects = remove_loaded_objects(objects, _dest_dataset, alias)

        for dir, object in objects.items():
            q.put((bucket_name, dir, object))
    else:
        logging.info('main_process: loading via non-glob method')
        objects = list_blobs_with_prefix(bucket_name, prefix)
        for object in objects:
            q.put((bucket_name, None, object))

    for c in range(concurrency):
        p = Process(target=_bulk_run, args=(c, lock, q, _dest_dataset, alias,))
        p.daemon = True
        p.start()

    logging.info('main_process: {} total tasks in queue'.format(q.qsize()))

    q.join()

    for c in range(concurrency):
        q.put(None)

    p.join()
    logging.info('main_process: done')


def _bulk_run(process_id, lock, q, dest_dataset, alias):
    """
    Process run job
    """
    logging.info('Process-{}: started'.format(process_id))

    for item in iter(q.get, None):
        bucket_name, dir, object = item
        try:
            o = object if dir is None else dir
            logging.info('Process-{}: running {}'.format(process_id, o))
            run(bucket_name, object, dest_dataset, dir=dir,
                lock=lock, alias=alias)
        except P2BWarning:
            q.put(item)
            logging.warning('Process-{}: Re-queued {} '
                            'due to warning'.format(process_id,
                                                    object))
        finally:
            q.task_done()
            logging.info('Process-{}: {} tasks left '
                         'in queue'.format(process_id, q.qsize()))
    q.task_done()
    logging.info('Process-{}: done'.format(process_id))
