#!/bin/bash

# 左右两个目录
DIR1="/home/aaa"
DIR2="/home/clickhouse/test/data"

# 临时文件
TMP1=$(mktemp)
TMP2=$(mktemp)
CPU=$(nproc)

# 递归查找以metrics_output开头的文件，计算MD5并排序
find "$DIR1" -type f -name "metrics_output*" | sort | xargs -P $CPU -n 100 md5sum > "$TMP1"
find "$DIR2" -type f -name "metrics_output*" | sort | xargs -P $CPU -n 100 md5sum > "$TMP2"

# 对比
echo "对比结果："
if diff "$TMP1" "$TMP2" > /tmp/diff_result.txt; then
    echo "✅ 两个目录下所有 metrics_output* 文件内容完全一致！"
else
    echo "❌ 发现差异，详情如下："
    cat /tmp/diff_result.txt
fi

# 清理临时文件
rm -f "$TMP1" "$TMP2" /tmp/diff_result.txt