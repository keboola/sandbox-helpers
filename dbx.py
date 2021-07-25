import os
from pyspark.dbutils import DBUtils

MANIFEST_FILE_EXT = 'file_list'

def export_parquet(parquet_path, spark):
    dbutils = DBUtils(spark)
    file_list = deep_ls(dbutils, parquet_path, 10)
    manifest = ''
    for file in file_list:
        manifest += file.path + '\n'
    dbutils.fs.put(parquet_path + '.' + MANIFEST_FILE_EXT, manifest, True)

def get_dataframe_from_parquet(parquet_path, destination, spark):
    dbutils = DBUtils(spark)
    files = dbutils.fs.ls(parquet_path + '.' + MANIFEST_FILE_EXT)
    for file in files:
        # since its input it has the file and a manifest, we only want the file
        if not file.name.endswith('.manifest'):
            manifest_path = file.path
            break
    manifest = dbutils.fs.head(manifest_path, 10000).splitlines()
    for init_file in manifest:
        path_parts = init_file.split('/')
        new_rel_path = ''
        for i, part in enumerate(reversed(path_parts)):
            if part == 'files':
                break
            if part.startswith('part-') and part.endswith('parquet'):
                part_file = part
            if i < (len(path_parts) - 1):
                new_rel_path = os.path.join(part, new_rel_path)
        copy_dir = os.path.join(destination, new_rel_path.replace('-', '_'))
        if not file_exists(dbutils, copy_dir):
            dbutils.fs.mkdirs(copy_dir)
        # now get the files to put there from our input
        input_part_path = os.path.join(parquet_path, '../', part_file.replace('-', '_'))
        for input_part in dbutils.fs.ls(input_part_path):
            if not input_part.name.endswith('manifest'):
                dbutils.fs.cp(input_part.path, copy_dir)

    df = spark.read.parquet(destination)
    return df

def file_exists(dbutils, path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise


def deep_ls(dbutils, path: str, max_depth=1, reverse=False, key=None, keep_hidden=False):
    """List all files in base path recursively.
    List all files and folders in specified path and subfolders within maximum recursion depth.
    Parameters
    ----------
    path : str
        The path of the folder from which files are listed
    max_depth : int
        The maximum recursion depth
    reverse : bool
        As used in `sorted([1, 2], reverse=True)`
    key : Callable
        As used in `sorted(['aa', 'aaa'], key=len)`
    keep_hidden : bool
        Keep files and folders starting with '_' or '.'
    Examples
    --------
    >>> from pprint import pprint
    >>> files = list(deep_ls(dbutils, '/databricks-datasets/asa/airlines'))
    >>> pprint(files) # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    [FileInfo(path='dbfs:/databricks-datasets/asa/airlines/1987.csv', name='1987.csv', size=127162942),
     ...
     FileInfo(path='dbfs:/databricks-datasets/asa/airlines/2008.csv', name='2008.csv', size=689413344)]
    >>> first, *_, last = files
    >>> first
    FileInfo(path='dbfs:/databricks-datasets/asa/airlines/1987.csv', name='1987.csv', size=127162942)
    >>> last
    FileInfo(path='dbfs:/databricks-datasets/asa/airlines/2008.csv', name='2008.csv', size=689413344)
    """

    # Hidden files may be filtered out
    condition = None if keep_hidden else lambda x: x.name[0] not in ('_', '.')

    # List all files in path and apply sorting rules
    li = sorted(filter(condition, dbutils.fs.ls(path)),
                reverse=reverse, key=key)

    # Return all files (not ending with '/')
    for x in li:
        if x.path[-1] != '/' and len(x.path) > 1:
            yield x

    # If the max_depth has not been reached, start
    # listing files and folders in subdirectories
    if max_depth > 1:
        for x in li:
            if x.path[-1] != '/':
                continue
            for y in deep_ls(dbutils, x.path, max_depth - 1, reverse, key, keep_hidden):
                yield y

    # If max_depth has been reached,
    # return the folders
    else:
        for x in li:
            if x.path[-1] == '/':
                yield x


def key(val):
    """Sort function.
    Takes a filepath:
      '/mnt/raw/store/item/year=2019/month=6/day=4/'
    Extracts the integer 4 or returns -1
    """
    try:
        return int(list(filter(bool, val.path.split('/'))).pop().split('=').pop())
    except ValueError as e:
        return -1

