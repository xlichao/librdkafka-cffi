# librdkafka-cffi

使用 CFFI 包装 librdkafka 的所有函数，在现阶段中主要用于测试，不能用于生产环境。

## 为什么使用 CFFI ？

librdkafka 已经提供了[官方的 CPython 软件包](https://github.com/confluentinc/confluent-kafka-python/)，附有良好的文档支持，实现了绝大多数的 API 。但是使用 CFFI 包装有以下两个好处：

* 可以使用方便的形式调用 C API ，用于复现一些在使用 C API 时遇到的场景，并与 C 程序对比其结果而无需重新编译。
* 支持 Pypy 。

## 和其他的项目有什么区别 ？

edenhill 大神曾经写过一个[测试 DEMO ](https://github.com/edenhill/rdkafka-python)，提供了有限的 API 实现，并且似乎他的测试结果导向了 CFFI 效率较低，因此他不再维护该工程。目前，librdkafka 的版本已经到达了 2.6.0 并更新了大量的函数，人工编写 CFFI 的 cdef 是较为困难的。因此，本工程利用 GCC 预处理去除大多数的宏和注释，并加以少许的自动化人工修饰过程，根据 rdkafka.h 自动生成 librdkafka 的 cdef 定义，包含所有的方法（即使已经废弃）。实现了官方的消费者例子，似乎工作得比较正常。

# 依赖环境

构建过程已经在以下环境测试通过：
* Debian 6.1.115-1 (2024-11-01)
* librdkafka-dev 2.0.2 （稳定版本）
* cffi 1.15.1
* Python 3.10.9 或者 PyPy 7.3.17

需要安装 librdkafka-dev ： `apt install librdkafka-dev` 。 安装完毕后，假定头文件存在于 `/usr/include/librdkafka/rdkafka.h` ，如果不是这个路径，需要修改 _build.py 。

# 如何使用

对于简单的测试，可以执行 `python ./setup.py bdist_wheel` 执行构建，然后在工程目录中增加对应的测试代码。构建过程涉及以下文件：

* _build.py 准备 cdef 文件，并调用 CFFI 构建 .so 。构建完毕后， _librdkafka_cffi.so 文件会出现于 `librdkafka_cffi` 目录中，并根据具体的 Python 版本恰当命名。不能使用 CPython 构建，然后使用 Pypy 调用。
* _gccpre2cdef.py 将 gcc 的预处理输出转换为 cdef 。主要做了以下事情：
    * 逐行挑出属于 librdkafka 的行
    * 去除 `__attribute__` 宏
    * 添加一些额外的代码：尤其是 Python 回调

构建完毕后，在 `librdkafka_cffi` 目录中的代码即可 `from ._librdkafka_cffi import ffi,lib` 。需要注意的是，部分使用宏定义的常量不会出现在 cdef 中，因此需要手动定义，例如 `RD_KAFKA_PARTITION_UA=-1` 。

# librdkafka-cffi

This project provides a CFFI wrapper for all functions in librdkafka, primarily for testing purposes at this stage and is **not** suitable for production environments.

## Why Use CFFI?
While librdkafka already offers an [official CPython package](https://github.com/confluentinc/confluent-kafka-python/) with comprehensive documentation and implementation of most APIs, using CFFI offers these advantages:

* **Direct C API Calls:** Enables the convenient invocation of C APIs to reproduce scenarios encountered when using the C API, allowing for comparisons with C programs without recompilation.
* **Pypy Support:** Supports the Pypy Python implementation.

## Differences from Other Projects
Edenhill, the creator of librdkafka, once developed a [test demo](https://github.com/edenhill/rdkafka-python) with limited API implementation. However, his tests indicated lower efficiency with CFFI, leading him to discontinue maintenance. With librdkafka now at version 2.6.0 and numerous new functions, manually writing CFFI cdefs is quite challenging. 

This project leverages GCC preprocessing to remove most macros and comments, along with some automated manual modifications, to generate librdkafka's cdef definitions from rdkafka.h. It includes all methods, even deprecated ones. The official consumer example has been implemented and seems to work correctly.

# Dependencies
The build process has been tested in the following environment:
* Debian 6.1.115-1 (2024-11-01)
* librdkafka-dev 2.0.2 (stable version)
* cffi 1.15.1
* Python 3.10.9 or PyPy 7.3.17

You need to install librdkafka-dev: `apt install librdkafka-dev`. After installation, assuming the header file is located at `/usr/include/librdkafka/rdkafka.h`, you may need to modify _build.py if it's in a different location.

# Usage
For simple tests, execute `python ./setup.py bdist_wheel` to build. Then add your test code to the project directory. The build process involves the following files:
* **_build.py:** Prepares the cdef file and calls CFFI to build the .so file. Upon completion, the _librdkafka_cffi.so file will be generated in the librdkafka_cffi directory with an appropriate name based on the Python version. Avoid building with CPython and then using Pypy.
* **_gccpre2cdef.py:** Converts the output of GCC preprocessing into cdef. It primarily performs the following tasks:
    * Extracts lines belonging to librdkafka.
    * Removes `__attribute__` macros.
    * Adds additional code, especially for Python callbacks.

After building, you can use `from ._librdkafka_cffi import ffi,lib` in the code within the librdkafka_cffi directory. Note that constants defined using macros may not appear in cdef, so you need to define them manually, such as `RD_KAFKA_PARTITION_UA=-1`.
