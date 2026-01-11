import sys
import os
import subprocess
import logging
import shutil
import tempfile
import atexit

# Fix path to allow importing sibling modules
_DIR = os.path.dirname(os.path.abspath(__file__))
if _DIR not in sys.path:
    sys.path.insert(0, _DIR)

from _gccpre2cdef import Processor
from cffi import FFI


logging.basicConfig(level=logging.INFO)

# This FFI instance will be configured at module load time.
ffibuilder = FFI()

# Create a temporary directory for build files
tmp_path = tempfile.mkdtemp(prefix="librdkafka_cffi_")
# Ensure cleanup happens when the process exits
atexit.register(shutil.rmtree, tmp_path, ignore_errors=True)

# --- CDEF Generation Logic ---
try:
    HEADER_PATH = "/usr/include/librdkafka/rdkafka.h"
    GCC_PRE_PATH = os.path.join(tmp_path, "output-gcc-pre.txt")
    CDEF_PATH = os.path.join(tmp_path, "cdef-gcc-pre.c")

    if not os.path.exists(HEADER_PATH):
        raise FileNotFoundError(
            f"librdkafka header file not found at {HEADER_PATH}. "
            "Please install librdkafka-dev (or equivalent for your system)."
        )

    logging.info("Calling GCC preprocessor")
    command = f"gcc -E {HEADER_PATH} | grep -v '^$' > {GCC_PRE_PATH}"
    subprocess.run(command, shell=True, check=True)

    logging.info("Converting header file to CDEF")
    with open(GCC_PRE_PATH) as source_file:
        with open(CDEF_PATH, "w") as target_file:
            # The Processor class handles adding necessary manual typedefs
            processor = Processor(source_file, target_file, HEADER_PATH)
            processor.process()

    logging.info("Adding extra CDEFs")
    additional_codes = [
        'typedef void PyObject;',
        'extern "Python" {',
        '    void log_callback(rd_kafka_t *rk, int level, const char *fac, const char *buf);',
        '    void rebalance_callback(rd_kafka_t *rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *partitions, void *opaque);',
        '    void delivery_report_callback(rd_kafka_t *rk, const rd_kafka_message_t *msg, void *opaque);',
        '}',
    ]
    with open(CDEF_PATH, "a") as target_file:
        for code in additional_codes:
            print(code, file=target_file)

    logging.info("Configuring FFI builder")
    with open(CDEF_PATH) as cdef_file:
        cdef_str = cdef_file.read()

    ffibuilder.cdef(cdef_str)

    ffibuilder.set_source(
        "librdkafka_cffi._librdkafka_cffi",
        f'#include "{HEADER_PATH}"',
        libraries=['rdkafka']
    )

except Exception as e:
    logging.error(f"Error during CFFI build prep: {e}")
    # We leave the ffibuilder un-configured, which will cause the build to fail
    # with a clear message from setuptools.
    pass