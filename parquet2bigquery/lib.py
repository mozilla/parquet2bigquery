import logging
import re
import secrets
from dateutil.parser import parse
from datetime import datetime
from itertools import chain
from multiprocessing import Process, JoinableQueue, Lock

import google.api_core.exceptions
from google.cloud import storage, bigquery
from google.cloud.bigquery.table import TimePartitioning, TimePartitioningType


# sample message 2019-02-07 12:34:55,439 root WARNING yay
logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s %(message)s',
                    level=logging.INFO)

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
    """
    Returns a tuple that contains the BigQuery client and TableReference.

    """
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


def gen_rand_string(size=3):
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
    * Contain up to 1,024 characters
    * Contain letters (upper or lower case), numbers, and underscores

    We intentionally lower case the table_name.

    https://cloud.google.com/bigquery/docs/tables
    """
    if len(table_name) > 1024:
        raise ValueError('table_name cannot contain more than 1024 characters')
    else:
        return re.sub('\W+', '_', table_name).lower()


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

    first_part_idx = next(iter([index for index, elem in enumerate(split_key)
                          if '=' in elem]))

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
                        for elem in split_key[first_part_idx+1:]
                        if '=' in elem]
    meta['partitions'] += extra_partitions

    return meta


def create_bq_table(table_id, dataset, schema=None, partition_field=None,
                    cluster_by=()):
    """
    Create a BigQuery table.
    """

    client, table_ref = get_bq_client(table_id, dataset)
    table_def = bigquery.Table(table_ref, schema=schema)

    if partition_field:
        _tp = TimePartitioning(type_=TimePartitioningType.DAY,
                               field=partition_field)
        table_def.time_partitioning = _tp

    if cluster_by:
        table_def.clustering_fields = cluster_by

    try:
        client.create_table(table_def)
    except google.api_core.exceptions.Conflict:
        logging.info('{}: BigQuery table already exists.'.format(table_id))
        pass

    logging.info('{}: table created.'.format(table_id))


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

    table.schema = new_schema + schema_additions
    table = client.update_table(table, ['schema'])
    logging.info('{}: BigQuery table schema updated.'.format(table_id))


def get_bq_query_schema(query):
    """
    Get the schema of a BigQuery query.
    """
    job_config = bigquery.QueryJobConfig(dry_run=True)
    job = bigquery.Client().query(query, job_config=job_config)
    return [
        bigquery.SchemaField.from_api_repr(field)
        for field in job._job_statistics()['schema']['fields']
    ]


def load_parquet_to_bq(bucket, object_key, table_id, dataset, schema=None,
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

    uri = 'gs://{}/{}'.format(bucket, object_key)

    if partition:
        table_id = '{}${}'.format(table_id, partition)

    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config)

    load_job.result()
    logging.info('{}: Parquet file {} loaded '
                 'into BigQuery.'.format(table_id,
                                         object_key))


def _compare_columns(new_col, cur_col):
    """
    Compare two columns to see if they are changing.
    This is currently only checks to see if column MODE
    is changing.
    """
    if isinstance(new_col, tuple):
        for i in range(len(new_col)):
            _compare_columns(new_col[i], cur_col[i])

    if isinstance(new_col, google.cloud.bigquery.schema.SchemaField):
        if new_col.fields and cur_col.fields:
            _compare_columns(new_col.fields, cur_col.fields)

        # if mode is changing from NULLABLE to REQUIRED
        if(new_col.mode == 'REQUIRED' and cur_col.mode == 'NULLABLE'):
            logging.warn('Column mode changed from '
                         'REQUIRED to NULLABLE, ignoring.')
            return False

    return False


def get_schema_additions(current_schema, newest_schema):
    """
    Compare two BigQuery table schemas and get the additional columns.
    newest_schema should contain the latest schema and current_schema should be
    the current schema. We only append additional columns.

    Handling column changes are currently not implemented.
    """
    schema_additions = []

    schema_diff = set(newest_schema) - set(current_schema)

    for sd_col in schema_diff:
        already_exists = False
        for cs_col in current_schema:
            # check to see if the column we are attempting to add exists
            if sd_col.name == cs_col.name:
                already_exists = True
                # we found an existing column, find out what has changed
                col = (_compare_columns(sd_col, cs_col))
                if col:
                    raise NotImplementedError()
        # we found a new column, add it
        if not already_exists:
            schema_additions.append(sd_col)
    return schema_additions


def construct_select_query(table_id, date_partition_field, date_partition_value,
                           partitions=None, dataset=DEFAULT_TMP_DATASET, drop=(),
                           rename={}, replace=()):

    """
    Construct a query to select all data from a temp table, append
    the relevant partitions and output into the primary table.
    """

    select_cols = ['SELECT *']

    select_cols.append("DATE '{0}' as {1}".format(date_partition_value,
                                       date_partition_field))

    part_as = "'{1}' as {0}"
    for partition in partitions:
        select_cols.append(part_as.format(*partition))

    _select_cols = ','.join(select_cols)

    query = """
    {0}
    FROM {1}.{2}
    """.format(_select_cols, dataset, table_id)

    if drop or rename or replace:
        rename_clause = "".join("{0} AS {1}, ".format(*pair) for pair in rename.items())
        drop_fields = ",".join(chain(drop, rename.keys()))
        except_clause = " EXCEPT ({0})".format(drop_fields) if drop_fields else ""
        replace_clause = " REPLACE ({0})".format(",".join(replace)) if replace else ""
        return "SELECT {0}*{1}{2} FROM ({3})".format(rename_clause, except_clause,
                                                     replace_clause, query)

    return query


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
        client.get_table(table_ref)
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
        logging.warn('{}: table cannot be deleted'
                     'since it is not found.'.format(table_id))
        pass


def list_blobs_with_prefix(bucket_name, prefix, delimiter=None):
    """
    Return a list of all objects in a bucket prefix.
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix, delimiter=delimiter)

    object_keys = []

    for blob in blobs:
        if not ignore_key(blob.name):
            object_keys.append(blob.name)

    return object_keys


def get_latest_object(bucket_name, prefix, delimiter=None):
    """
    Get the latest object in a bucket prefix.
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix, delimiter=delimiter)

    latest_objects = {}
    _latest_objects_timestamp = {}

    for blob in blobs:
        if not ignore_key(blob.name):
            path = '/'.join(blob.name.split('/')[0:-1])
            if path not in _latest_objects_timestamp \
                    or _latest_objects_timestamp[path] < blob.updated:
                _latest_objects_timestamp[path] = blob.updated
                latest_objects[path] = blob.name

    return latest_objects


def create_primary_bq_table(table_id, dataset,
                            schema, date_partition_field, cluster_by):
    """
    Create the primary BigQuery table for a imported dataset.
    """
    if not check_bq_table_exists(table_id, dataset):
        create_bq_table(table_id, dataset, schema,
                        date_partition_field, cluster_by)


def run(bucket_name, object_key, dest_dataset, path=None, lock=None,
        alias=None, cluster_by=(), drop=(), rename={}, replace=()):
    """
    Take object(s) and load them into BigQuery.
    """

    # We don't care about these objects
    if ignore_key(object_key):
        logging.warning('Ignoring {}.'.format(object_key))
        return

    meta = _get_object_key_metadata(object_key)
    dp = meta['date_partition']

    table_id = alias or meta['table_id']

    table_id_tmp = normalize_table_id('_'.join([meta['table_id'],
                                      dp['value'],
                                      gen_rand_string()]))

    query = construct_select_query(table_id_tmp,
                                   dp['field'],
                                   dp['value'],
                                   partitions=meta['partitions'],
                                   drop=drop,
                                   rename=rename,
                                   replace=replace)

    # We assume that the data will have the following extensions
    if path:
        object_key_load = '{}/*'.format(path)
        if object_key.endswith('parquet'):
            object_key_load += 'parquet'
    else:
        object_key_load = object_key

    # Create a temp table and load the data into temp table
    try:
        create_bq_table(table_id_tmp, DEFAULT_TMP_DATASET)
        load_parquet_to_bq(bucket_name, object_key_load, table_id_tmp,
                           DEFAULT_TMP_DATASET)
    except (google.api_core.exceptions.InternalServerError,
            google.api_core.exceptions.ServiceUnavailable):
        delete_bq_table(table_id_tmp, dataset=DEFAULT_TMP_DATASET)
        logging.exception('{}: BigQuery Retryable Error.'.format(table_id))
        raise P2BWarning('BigQuery Retryable Error.')

    # Data is now loaded, we want to grab the schema of the table
    try:
        new_schema = get_bq_query_schema(query)
    except (google.api_core.exceptions.InternalServerError,
            google.api_core.exceptions.ServiceUnavailable):
        logging.exception('{}: GCS Retryable Error.'.format(table_id))
        raise P2BWarning('GCS Retryable Error.')

    # Try to create the primary BigQuery table
    with lock:
        create_primary_bq_table(table_id, dest_dataset, new_schema,
                                rename.get(dp['field'], dp['field']), cluster_by)

    # Compare temp table schema with primary table schema
    current_schema = get_bq_table_schema(table_id, dest_dataset)
    schema_additions = get_schema_additions(current_schema, new_schema)

    # If there are additions then update the primary table
    if len(schema_additions) > 0:
        update_bq_table_schema(table_id, schema_additions, dest_dataset)

    logging.info('{}: loading {}/{} to BigQuery '
                 'table {}'.format(table_id,
                                   bucket_name,
                                   object_key_load,
                                   table_id_tmp))
    # Try to load the temp table data into primary table
    try:
        load_bq_query_to_table(query, table_id, dest_dataset)
    except (google.api_core.exceptions.InternalServerError,
            google.api_core.exceptions.ServiceUnavailable):
        logging.exception('{}: BigQuery Retryable Error'.format(table_id))
        raise P2BWarning('BigQuery Retryable Error')
    finally:
        delete_bq_table(table_id_tmp)


def get_bq_table_partitions(table_id, date_partition_field,
                            date_partition_format,
                            date_partition_min,
                            date_partition_max,
                            path_prefix,
                            dataset, partitions=[], rename={}):
    """
    Get all the partitions available in a BigQuery table.
    This is used for resume operations.
    """
    client, table_ref = get_bq_client(table_id, dataset)

    select_cols = []
    reconstruct_paths = []

    dp_renamed = rename.get(date_partition_field, date_partition_field)
    reformat_dp_field = ("FORMAT_DATE('{0}', {1})"
                         " as {1}".format(date_partition_format,
                                          dp_renamed))

    for partition in partitions:
        name = partition[0]
        select_cols += [(name, rename.get(name, name))]

    _select_cols = ','.join(chain([reformat_dp_field], (c[1] for c in select_cols)))

    select_cols = [(date_partition_field, dp_renamed)] + select_cols
    group_cols = ','.join(c[1] for c in select_cols)

    query = """
    SELECT {2}
    FROM {0}.{1}
    WHERE {4} >= DATE '{5}' and {4} <= DATE '{6}'
    GROUP BY {3}
    """.format(dataset, table_id, _select_cols, group_cols,
               date_partition_field, date_partition_min, date_partition_max)

    query_job = client.query(query)
    results = query_job.result()

    for row in results:
        tmp_path = []
        tmp_path += path_prefix
        for part, field in select_cols:
            tmp_path.append('{}={}'.format(part, row[field]))
        reconstruct_paths.append('/'.join(tmp_path))

    return reconstruct_paths


def remove_loaded_objects(objects, dataset, alias, rename):
    """
    Remove objects from list that have already been loaded
    into the BigQuery table. We do this so we don't load objects
    that have already been loaded into BigQuery.

    We assume that based on `get_bq_table_partitions` returned data
    that the original load job completed successfully.

    If the BigQuery table does not exist we assume that no data
    has been loaded and return the original objects list.

    If the BigQuery table does exist we query it based on the partition
    information extracted from the object key and remove and return
    objects from the list.

    """

    initial_object_tmp = list(objects)[0]
    meta = _get_object_key_metadata(initial_object_tmp)
    dp = meta['date_partition']
    dp_values = sorted([
        parse(value).strftime('%Y-%m-%d')
        for value in {
            item.split('/')[meta['first_part_idx']].split('=')[1]
            for item in objects
        }
    ])
    dp_min, dp_max = dp_values[0], dp_values[-1]

    path_prefix = initial_object_tmp.split('/')[:meta['first_part_idx']]

    table_id = alias or meta['table_id']

    if not check_bq_table_exists(table_id, dataset):
        return objects

    object_paths = get_bq_table_partitions(table_id,
                                           dp['field'],
                                           dp['format'],
                                           dp_min,
                                           dp_max,
                                           path_prefix,
                                           dataset,
                                           meta['partitions'],
                                           rename)

    for key in object_paths:
        if objects.pop(key, False):
            logging.info('key {} already loaded into BigQuery'.format(key))

    return objects


def bulk(bucket_name, prefix, concurrency, glob_load, resume_load,
         dest_dataset=None, alias=None, cluster_by=(), drop=(), rename={}, replace=()):
    """
    Load data into BigQuery concurrently
    Args:
        bucket_name: gcs bucket name (str)
        prefix: object key path, 'dataset/version' (str)
        concurrency: number of processes to handle the load (int)
        glob_load: load data by globbing path dirs (boolean)
        resume_load: resume load (boolean)
        dest_dataset: override default dataset location (str)
        alias: override object key derived table name (str)
        cluster_by: top level fields to cluster by (Tuple[str])
        drop: top level fields to exclude (Tuple[str])
        rename: top level fields to rename (Dict[str,str])
        replace: top field replacement expressions (Tuple[str])
    """

    _dest_dataset = dest_dataset or DEFAULT_DATASET

    logging.info('main_process: dataset set to {}'.format(_dest_dataset))

    q = JoinableQueue()
    lock = Lock()

    if glob_load:
        logging.info('main_process: loading via glob method')
        object_keys = get_latest_object(bucket_name, prefix)
        if resume_load:
            object_keys = remove_loaded_objects(object_keys,
                                                _dest_dataset, alias, rename)

        for path, object_key in object_keys.items():
            q.put((bucket_name, path, object_key))
    else:
        logging.info('main_process: loading via non-glob method')
        object_keys = list_blobs_with_prefix(bucket_name, prefix)
        for object_key in object_keys:
            q.put((bucket_name, None, object_key))

    args = (lock, q, _dest_dataset, alias, cluster_by, drop, rename, replace)
    for c in range(concurrency):
        p = Process(target=_bulk_run, args=(c,) + args)
        p.daemon = True
        p.start()

    logging.info('main_process: {} total tasks in queue'.format(q.qsize()))

    q.join()

    for c in range(concurrency):
        q.put(None)

    p.join()
    logging.info('main_process: done')


def _bulk_run(process_id, lock, q, dest_dataset, alias, cluster_by, drop, rename,
              replace):
    """
    Process run job
    """
    logging.info('Process-{}: started'.format(process_id))

    for item in iter(q.get, None):
        bucket_name, path, object_key = item
        try:
            ok = object_key if path is None else path
            logging.info('Process-{}: running {}'.format(process_id, ok))
            run(bucket_name, object_key, dest_dataset, path=path,
                lock=lock, alias=alias, cluster_by=cluster_by, drop=drop,
                rename=rename, replace=replace)
        except P2BWarning:
            q.put(item)
            logging.warning('Process-{}: Re-queued {} '
                            'due to warning'.format(process_id,
                                                    ok))
        finally:
            q.task_done()
            logging.info('Process-{}: {} tasks left '
                         'in queue'.format(process_id, q.qsize()))
    q.task_done()
    logging.info('Process-{}: done'.format(process_id))
