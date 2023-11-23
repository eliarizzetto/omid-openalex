import csv
from os import makedirs, listdir
from os.path import join, isdir
from csv import DictWriter, DictReader
from tqdm import tqdm
import pandas as pd

def read_csv_tables(*dirs, use_pandas=False):
    """
    Reads the output CSV non-compressed tables from one or more directories and yields either rows
    as dictionaries (default) or entire pandas DataFrames, depending on the `use_pandas` parameter.

    :param dirs: One or more directories to read files from, provided as variable-length arguments.
    :param use_pandas: Optional parameter specifying whether to use pandas DataFrame (default is False).
    :return: Yields rows as dictionaries or entire pandas DataFrames from all CSV files in the specified directories.
    """
    csv.field_size_limit(131072 * 12)  # increase the default field size limit
    for directory in dirs:
        if isdir(directory):
            files = [file for file in listdir(directory) if file.endswith('.csv')]
            for file in tqdm(files, desc=f"Processing {directory}", unit="file"):
                file_path = join(directory, file)
                if use_pandas:
                    df = pd.read_csv(file_path, encoding='utf-8')
                    yield df
                else:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f, dialect='unix')
                        for row in reader:
                            yield row
        else:
            raise ValueError("Each argument must be a string representing the path to an existing directory.")


class MultiFileWriter:
    """
    A context manager for writing rows to CSV files with automatic file splitting.

    :param out_dir: The directory for storing CSV files.
    :param fieldnames: Field names for the CSV file.
    :type fieldnames: List[str]
    :param max_rows_per_file: Max rows before creating a new file (default: 10,000).
    :type max_rows_per_file: int, optional
    :param file_extension: File extension for the CSV files (default: 'csv').
    :type file_extension: str, optional
    :param encoding: Encoding for writing CSV files (default: 'utf-8').
    :type encoding: str, optional
    :param dialect: CSV dialect to use (default: 'unix').
    :type dialect: str, optional

    Example::

        fieldnames = ['omid', 'type', 'omid_only']
        with FileWriter('output_data', fieldnames, max_rows_per_file=5000, file_extension='csv') as file_writer:
            for data_row in dataset:
                processed_row = process_data(data_row)
                file_writer.write_row(processed_row)
    """
    def __init__(self, out_dir, fieldnames, nrows=10000, **kwargs):
        self.out_dir = out_dir
        self.fieldnames = fieldnames
        self.max_rows_per_file = nrows  # maximum number of rows per file
        self.file_name = 0
        self.rows_written = 0
        self.current_file = None
        self.kwargs = kwargs
        makedirs(out_dir, exist_ok=True)
        csv.field_size_limit(131072 * 12)  # increase the default field size limit

    def __enter__(self):
        self._open_new_file()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _open_new_file(self):
        if self.current_file:
            self.current_file.close()
        file_extension = self.kwargs.get('file_extension', 'csv')
        file_path = join(self.out_dir, f'{self.file_name}.{file_extension}')
        encoding = self.kwargs.get('encoding', 'utf-8')
        self.current_file = open(file_path, 'w', encoding=encoding, newline='')
        dialect = self.kwargs.get('dialect', 'unix')
        self.writer = DictWriter(self.current_file, fieldnames=self.fieldnames, dialect=dialect)
        self.writer.writeheader()

    def write_row(self, row):
        self.writer.writerow(row)
        self.rows_written += 1
        if self.rows_written >= self.max_rows_per_file:
            self.file_name += 1
            self.rows_written = 0
            self._open_new_file()

    def close(self):
        if self.current_file:
            self.current_file.close()