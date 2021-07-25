import os
from pyspark.dbutils import DBUtils

MANIFEST_FILE_EXT = 'file_list'

def export_parquet(parquet_path, spark):
    dbutils = DBUtils(spark)
    file_list = get_dir_content(dbutils, parquet_path)
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

def get_dir_content(dbutils, ls_path):
  dir_paths = dbutils.fs.ls(ls_path)
  subdir_paths = [get_dir_content(dbutils, p.path) for p in dir_paths if p.isDir() and p.path != ls_path]
  flat_subdir_paths = [p for subdir in subdir_paths for p in subdir]
  return list(map(lambda p: p.path, dir_paths)) + flat_subdir_paths
