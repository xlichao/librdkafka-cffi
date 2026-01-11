# 如何构建 `librdkafka_cffi` 的二进制 Wheel 包

本文档介绍如何在 Linux 环境下为 CPython 和 PyPy 构建 `librdkafka_cffi` 的二进制 `manylinux` wheel 包。

## 1. 环境准备

在开始之前，请确保您的系统已经安装了以下依赖：

*   **`librdkafka` 开发库**: wheel 包会动态链接 `librdkafka.so`，但在构建时需要其头文件。
    *   在 Debian/Ubuntu 系统上: `sudo apt-get install -y librdkafka-dev`
    *   在 RedHat/CentOS 系统上: `sudo yum install -y librdkafka-devel`
*   **Python 构建工具**:
    *   `build`: 用于构建 wheel 包。
    *   `auditwheel`: 用于将 `librdkafka.so` 注入到 wheel 中，使其符合 `manylinux` 标准。

您可以使用以下命令安装 Python 工具：

```bash
pip install build auditwheel
```

## 2. 修改 `GEMINI.md` 

在 `GEMINI.md` 的 `工程要求` 部分，添加 `* 使用 docker compose 的时候记得加上 sudo` 这部分是 cffi 动态库链接的需要。

## 3. 构建 CPython Wheel

使用您环境中的标准 Python（CPython）来构建 wheel。

```bash
# 执行构建
python3 -m build --wheel

# 'auditwheel' 会将 'librdkafka.so' 等依赖项捆绑到 wheel 中
# 并将修复后的 wheel 存放到 'wheelhouse/' 目录
auditwheel repair dist/librdkafka_cffi-*.whl

# 将最终的 wheel 包移动到 dist 目录
mv wheelhouse/*.whl dist/
```

构建完成后，您会在 `dist` 目录下找到一个适用于 CPython 的 `manylinux` wheel 文件。

## 4. 构建 PyPy Wheel

我们同样可以为 PyPy 构建一个 wheel。

首先，确保您已经为 PyPy 安装了 `pip` 和构建工具。

```bash
# 如果 PyPy 环境没有 pip，先安装
~/pypy/bin/pypy3 -m ensurepip

# 为 PyPy 安装 build 和 auditwheel
~/pypy/bin/pypy3 -m pip install build auditwheel
```

然后，使用 PyPy 执行构建和修复流程：

```bash
# 使用 pypy3 执行构建
~/pypy/bin/pypy3 -m build --wheel

# 修复 PyPy wheel
auditwheel repair dist/librdkafka_cffi-*-pp*.whl

# 将最终的 wheel 包移动到 dist 目录
mv wheelhouse/*.whl dist/
```

构建完成后，`dist` 目录下将包含适用于 PyPy 的 `manylinux` wheel 文件。

## 5. 清理

构建过程中会在 `librdkafka_cffi` 目录下生成一个临时的 `.so` 文件。您可以手动清理它，或者在每次构建前清理。

```bash
rm librdkafka_cffi/*.so
```
