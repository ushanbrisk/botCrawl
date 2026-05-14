#!/usr/bin/env python3
"""
清理 MongoDB 中指定歌曲文件的记录
用法: python cleanup_duplicates.py --file unique_songs_by_name_part6.json
"""

import argparse
import json
import os
import sys

from dotenv import load_dotenv
from pymongo import MongoClient

# 加载 .env
load_dotenv()

# MongoDB 配置
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "netease")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "lyric")


def load_song_ids(filepath):
    """从 JSON 文件加载歌曲 ID 列表"""
    with open(filepath, "r", encoding="utf-8") as f:
        songs = json.load(f)
    # 支持两种格式：{"id": xxx} 或 {"song_id": xxx} 或直接是 [id1, id2, ...]
    song_ids = []
    for song in songs:
        if isinstance(song, dict):
            if "id" in song:
                song_ids.append(str(song["id"]))
            elif "song_id" in song:
                song_ids.append(str(song["song_id"]))
        else:
            song_ids.append(str(song))
    return set(song_ids)


def cleanup_songs(song_ids, dry_run=True):
    """删除指定歌曲 ID 的所有记录"""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    print(f"[INFO] 连接到 MongoDB: {MONGO_URI}")
    print(f"[INFO] 数据库: {MONGO_DB}, 集合: {MONGO_COLLECTION}")
    print(f"[INFO] 待清理歌曲数: {len(song_ids)}")

    if dry_run:
        print("[DRY RUN] 模拟运行，不会实际删除数据")
        # 统计这些歌曲的文档数量
        count = collection.count_documents({"song_id": {"$in": list(song_ids)}})
        print(f"[DRY RUN] 将删除 {count} 条记录")
        return

    # 执行删除
    result = collection.delete_many({"song_id": {"$in": list(song_ids)}})
    print(f"[INFO] 已删除 {result.deleted_count} 条记录")

    # 显示当前集合统计
    total = collection.count_documents({})
    print(f"[INFO] 清理后集合剩余 {total} 条记录")


def main():
    parser = argparse.ArgumentParser(description="清理 MongoDB 中指定歌曲的记录")
    parser.add_argument("--file", type=str, required=True, help="歌曲 ID 文件路径 (.json)")
    parser.add_argument("--dry-run", action="store_true", default=False, help="模拟运行，不实际删除")
    parser.add_argument("--no-dry-run", dest="dry_run", action="store_false", help="执行实际删除")
    args = parser.parse_args()

    if not os.path.exists(args.file):
        print(f"[ERROR] 文件不存在: {args.file}")
        sys.exit(1)

    print(f"[INFO] 从文件加载歌曲: {args.file}")
    song_ids = load_song_ids(args.file)
    print(f"[INFO] 加载了 {len(song_ids)} 个歌曲 ID")

    cleanup_songs(song_ids, dry_run=args.dry_run)


if __name__ == "__main__":
    main()