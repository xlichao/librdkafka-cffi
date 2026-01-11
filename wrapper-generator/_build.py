from cffi import FFI
from _gccpre2cdef import Processor
import os
import subprocess
import sys
import logging
import shutil

logger = logging.getLogger()

class Builder:
     def __init__(self,lib_name="_librdkafka_cffi",tmp_folder_name=None,ship_so_to=None) -> None:
          if tmp_folder_name is None:
               # 默认新建一个 tmp 目录来处理
               self.tmp_path = os.path.join(os.path.dirname(__file__),"__cffi__")
          else:
               self.tmp_path = os.path.join(os.path.dirname(__file__),tmp_folder_name)
          self.ship_so_to = ship_so_to
          self.HEADER_PATH = "/usr/include/librdkafka/rdkafka.h"
          self.GCC_PRE_PATH = os.path.join(self.tmp_path,"output-gcc-pre.txt")
          self.CDEF_PATH = os.path.join(self.tmp_path,"cdef-gcc-pre.c")
          self.lib_name = lib_name

     def clear(self):
          if os.path.exists(self.tmp_path):
               logger.info(f"清理中间文件 {self.tmp_path}")
               shutil.rmtree(self.tmp_path)


     def build(self):
          self.clear()
          if not os.path.exists(self.tmp_path):
               os.mkdir(self.tmp_path)
          logger.info("调用 GCC 预处理")
          gcc_process = subprocess.run(f"gcc -E '{self.HEADER_PATH}' | grep -v '^$' > {self.GCC_PRE_PATH}",shell=True,check=True)
          logger.info("转换头文件")
          with open(self.GCC_PRE_PATH) as source_file:
               with open(self.CDEF_PATH,"a") as target_file:
                    processor = Processor(source_file,target_file,self.HEADER_PATH)
                    processor.process()
          logger.info("添加一些额外的代码")
          additional_codes = [
               'typedef void PyObject;', # Add this line
               'extern "Python" void log_callback(rd_kafka_t *rk, int level, const char *fac, const char *buf);',
               'extern "Python" void rebalance_callback(rd_kafka_t *rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *partitions, void *opaque);',
               'extern "Python" void delivery_report_callback(rd_kafka_t *rk, const rd_kafka_message_t *msg, void *opaque);',
          ]
          with open(self.CDEF_PATH,"a") as target_file:
               for code in additional_codes:
                    print(code,file=target_file)
          logger.info("构建 CFFI ")
          ffibuilder = FFI()
          with open(self.CDEF_PATH) as cdef_file:
               cdef_str = cdef_file.read()
          ffibuilder.cdef(cdef_str)
          ffibuilder.set_source(self.lib_name,
          f"""
               #include "{self.HEADER_PATH}"   // the C header of the library
          """,
               libraries=['rdkafka'])   # library name, for the linker

          logger.info("编译 CFFI ")
          ffibuilder.compile(tmpdir=self.tmp_path,verbose=True)
          so_fullpath = self.get_so_fullpath()
          logger.info(f"so 位于 {so_fullpath}")
          if self.ship_so_to is not None:
               result = shutil.copy2(so_fullpath,self.ship_so_to)
               logger.info(f"so 已经分发到 {result} 清空构建目录")
               self.clear()


     
     def get_so_fullpath(self):
          file_list = os.listdir(self.tmp_path)
          so_filename = None
          if sys.platform == 'win32':
               suffix =  '.dll'
          elif sys.platform == 'darwin':
               suffix =  '.dylib'
          else:
               suffix =  '.so'
          for filename in file_list:
               if filename.startswith(self.lib_name) and filename.endswith(suffix):
                    so_filename = filename
                    break
          if so_filename is None:
               logger.warning("没有找到 .so 文件，确认构建是否成功？")
               return None
          else:
               so_fullpath = os.path.join(self.tmp_path,so_filename)
               logger.info(f"so 文件路径为 {so_fullpath}")
               return so_fullpath
          



if __name__ == "__main__":
     builder = Builder(ship_so_to=os.path.join(os.path.dirname(__file__), "librdkafka_cffi"))
     builder.build()

    