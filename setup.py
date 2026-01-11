from setuptools import setup, Extension,find_packages
from setuptools.command.build_ext import build_ext
import os
import sys

# Add current directory to path for _build import
sys.path.insert(0, os.path.dirname(__file__))

from _build import Builder
import shutil

LIB_NAME = "librdkafka_cffi"
CFFI_LIB_NAME = f"_{LIB_NAME}"

class CustomBuildExt(build_ext):
      def run(self):
            # 执行自定义构建过程，并且将构建好的东西分发到指定位置。
            current_path = os.path.dirname(__file__)
            ship_to = os.path.join(current_path,LIB_NAME)
            builder = Builder(lib_name=CFFI_LIB_NAME,ship_so_to=ship_to)
            builder.build()            


setup(name=LIB_NAME,
      version='0.0.1',
      description='helloworld!',
      author='hello',
      author_email='helloworld@outlook.com',
      requires=['cffi'], # 定义依赖哪些模块
      packages=find_packages(), # 系统自动从当前目录开始找包
      license='apache 3.0',
      ext_modules=[
            Extension('build_cffi', []), # 这个不重要，存在就行
      ],
      package_data={'': [f'{CFFI_LIB_NAME}*']},
      cmdclass={"build_ext": CustomBuildExt})