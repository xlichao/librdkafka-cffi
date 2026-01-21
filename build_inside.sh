#!/bin/bash
set -ex

# 安装编译 librdkafka C 库和 Python 扩展所需的系统依赖
apt-get update
apt-get install -y build-essential librdkafka-dev

# 安装 Python 打包工具
pip install wheel

# 安装项目依赖并执行打包
# setup.py 会调用 wrapper_generator/_build.py 来编译和链接 C 库
python setup.py bdist_wheel
