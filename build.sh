#!/bin/bash
set -ex

# 基于脚本的当前位置，自动计算出项目的根目录
PROJECT_ROOT=$(cd "$(dirname "$0")" && pwd)
# 构建环境
BASE_IMAGE=pypy:3.10-7.3.16-bullseye

# 使用 sudo 运行 docker 命令，并指定 linux/arm64 平台
# 容器内命令将以 root 用户执行，以安装系统依赖
sudo docker run --rm --platform linux/arm64 \
    -v "$PROJECT_ROOT:/workspace" \
    -w /workspace \
    ${BASE_IMAGE} \
    /bin/bash ./build_inside.sh

# Docker 容器创建的文件所有者为 root，这里将其修正为当前用户
# -R 表示递归地修改整个目录
echo "Updating permissions for build artifacts..."
sudo chown -R $(id -u):$(id -g) "$PROJECT_ROOT/dist"
echo "Done."
