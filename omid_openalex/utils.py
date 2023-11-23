import csv
from os import makedirs, listdir
from os.path import join
from csv import DictWriter, DictReader
from tqdm import tqdm


def read_csv_tables(*dirs):
    """
    Reads the output CSV non-compressed tables from one or more directories and yields the rows.
    :param dirs: One or more directories to read files from, provided as variable-length arguments.
    :return: Yields rows from all CSV files in the specified directories.
    """
    csv.field_size_limit(131072 * 12) # increase the default field size limit
    for directory in dirs:
        if isinstance(directory, str):
            files = [file for file in listdir(directory) if file.endswith('.csv')]
            for file in tqdm(files, desc=f"Processing {directory}", unit="file"):
                with open(join(directory, file), 'r', encoding='utf-8') as f:
                    reader = DictReader(f, dialect='unix')
                    for row in reader:
                        yield row
        else:
            raise ValueError("Each argument must be a string representing a directory path.")

class MultiFileWriter:
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