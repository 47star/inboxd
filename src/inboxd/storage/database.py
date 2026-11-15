from pathlib import Path

from rocksdict import Options, Rdict, WriteOptions

WRITE_BUFFER_BYTES = 16 * 1024 * 1024


def open_database(path: Path) -> Rdict:
    options = Options()
    options.create_if_missing(True)
    options.set_write_buffer_size(WRITE_BUFFER_BYTES)
    options.set_max_write_buffer_number(2)

    return Rdict(str(path), options)


def durable_writes() -> WriteOptions:
    options = WriteOptions()
    options.sync = True

    return options
