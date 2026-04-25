import aiohttp
import asyncio
import json
import os
from datetime import datetime
from pymongo import MongoClient
from textblob import TextBlob

# ================== 配置区 ==================
# 正式文件
SONGS_FILE = "unique_songs_by_name_part1.json"
# 测试文件
TEST_SONGS_FILE = "test_songs.json"
BASE_URL = "http://localhost:4000/comment/music"
LIMIT = 100
CONCURRENCY = 5
DELAY = 1.2
MAX_RETRIES = 3

# MongoDB 配置
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "netease"
MONGO_COLLECTION = "comments"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

semaphore = asyncio.Semaphore(CONCURRENCY)

# ================== MongoDB 客户端（单例） ==================
_mongo_client = None
_mongo_collection = None

def get_mongo_collection():
    global _mongo_client, _mongo_collection
    if _mongo_collection is None:
        _mongo_client = MongoClient(MONGO_URI)
        db = _mongo_client[MONGO_DB]
        _mongo_collection = db[MONGO_COLLECTION]
        # 创建索引以加速查询
        _mongo_collection.create_index("commentId", unique=True)
        _mongo_collection.create_index([("song_id", 1), ("commentId", 1)])
    return _mongo_collection

# ================== 工具函数 ==================
def load_songs():
    """加载歌曲列表"""
    with open(SONGS_FILE, "r", encoding="utf-8") as f:
        songs = json.load(f)
    print(f"[INFO] 加载了 {len(songs)} 首歌曲")
    return songs

def load_songs_from_file(filepath):
    """从指定文件加载歌曲列表"""
    with open(filepath, "r", encoding="utf-8") as f:
        songs = json.load(f)
    print(f"[INFO] 加载了 {len(songs)} 首歌曲 from {filepath}")
    return songs


OFFSET_DIR = "/ssd/music/comments/offset"
PROGRESS_FILE = "/ssd/music/comments/progress.json"


def get_offset_file(song_id):
    """获取某首歌的 offset 文件路径"""
    return os.path.join(OFFSET_DIR, f"{song_id}.txt")

def load_progress():
    """加载已完成的歌曲ID列表"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return set(data.get("completed_songs", []))
    return set()

def save_progress(completed_songs):
    """保存已完成的歌曲ID列表"""
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump({"completed_songs": list(completed_songs)}, f, ensure_ascii=False, indent=2)

def load_offset(song_id):
    """加载某首歌的 offset"""
    # 确保目录存在
    os.makedirs(OFFSET_DIR, exist_ok=True)
    offset_file = get_offset_file(song_id)
    if os.path.exists(offset_file):
        with open(offset_file, "r") as f:
            return int(f.read().strip())
    return 0

def save_offset(song_id, offset):
    """保存某首歌的 offset"""
    os.makedirs(OFFSET_DIR, exist_ok=True)
    offset_file = get_offset_file(song_id)
    with open(offset_file, "w") as f:
        f.write(str(offset))

def analyze_sentiment(text):
    """情感分析"""
    try:
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        if polarity > 0.1:
            return "positive", polarity
        elif polarity < -0.1:
            return "negative", polarity
        else:
            return "neutral", polarity
    except:
        return "neutral", 0.0

def parse_comment(c, song_id, song_name, comment_type):
    """解析评论数据"""
    user = c.get("user", {})
    content = c.get("content", "")
    sentiment, polarity = analyze_sentiment(content)

    return {
        "commentId": c.get("commentId"),
        "song_id": song_id,
        "song_name": song_name,
        "content": content,
        "time": c.get("time"),
        "timeStr": c.get("timeStr"),
        "likedCount": c.get("likedCount"),
        "userId": user.get("userId"),
        "nickname": user.get("nickname"),
        "vipType": user.get("vipType"),
        "location": c.get("ipLocation", {}).get("location"),
        "comment_type": comment_type,
        "sentiment": sentiment,
        "polarity": polarity
    }

def save_to_mongodb(comments):
    """批量保存到 MongoDB，返回新插入的评论数"""
    if not comments:
        return 0

    collection = get_mongo_collection()
    new_count = 0
    for comment in comments:
        try:
            result = collection.update_one(
                {"commentId": comment["commentId"]},
                {"$setOnInsert": comment},
                upsert=True
            )
            if result.upserted_id:
                new_count += 1
        except Exception as e:
            print(f"[MONGO ERROR] {e}")
    return new_count

# ================== 爬虫核心 ==================
async def fetch_page(session, song_id, song_name, offset, storage):
    """获取单页评论，返回新插入的评论数"""
    params = {
        "id": song_id,
        "limit": LIMIT,
        "offset": offset
    }

    for attempt in range(1, MAX_RETRIES + 1):
        async with semaphore:
            try:
                async with session.get(BASE_URL, params=params, headers=HEADERS, timeout=10) as resp:
                    if resp.status != 200:
                        print(f"[WARN] song={song_id}, offset={offset}, status={resp.status}")
                        continue

                    data = await resp.json()

                    comments = data.get("comments", [])
                    hot = data.get("hotComments", []) if offset == 0 else []

                    parsed_comments = []
                    for c in hot:
                        parsed_comments.append(parse_comment(c, song_id, song_name, "hot"))
                    for c in comments:
                        parsed_comments.append(parse_comment(c, song_id, song_name, "normal"))

                    # 批量保存到 MongoDB，返回新插入的评论数
                    new_count = storage(parsed_comments)

                    print(f"[OK] song={song_id}, offset={offset}, hot={len(hot)}, normal={len(comments)}, new={new_count}")

                    await asyncio.sleep(DELAY)
                    return data, new_count

            except Exception as e:
                print(f"[RETRY {attempt}] song={song_id}, offset={offset}, error={e}")
                await asyncio.sleep(2)

    return None, 0

async def crawl_song(session, song_id, song_name, storage):
    """爬取单首歌的所有评论"""
    offset = load_offset(song_id)
    print(f"[INFO] 开始爬取歌曲: {song_name} (id={song_id}), 从 offset={offset} 开始")

    # 用于统计
    total_comments = 0
    total_new_comments = 0
    page_count = 0
    consecutive_duplicate_pages = 0  # 连续重复页数计数

    while True:
        data, new_count = await fetch_page(session, song_id, song_name, offset, storage)

        if not data:
            print(f"[ERROR] song={song_id} 连续失败，终止")
            break

        # 保存当前 offset
        save_offset(song_id, offset)

        # 获取评论
        comments = data.get("comments", [])
        hot = data.get("hotComments", []) if offset == 0 else []
        all_comments = hot + comments

        if len(all_comments) == 0:
            # 如果返回空数据，说明真的没有评论了
            print(f"[INFO] song={song_id} 无更多评论, 共 {page_count} 页")
            break

        total_new_comments += new_count

        # 如果新评论数为 0，说明都是重复的
        if new_count == 0:
            consecutive_duplicate_pages += 1
            print(f"[WARN] song={song_id}, offset={offset}, 全部为重复评论 (连续 {consecutive_duplicate_pages} 页)")
            if consecutive_duplicate_pages >= 3:
                print(f"[INFO] song={song_id} 连续 3 页无新评论，停止爬取")
                break
        else:
            consecutive_duplicate_pages = 0

        if not data.get("more"):
            if offset > 0 or len(comments) < LIMIT:
                print(f"[INFO] song={song_id} 爬取完成, 共 {page_count + 1} 页")
                break

        # 计算评论数
        total_comments += len(all_comments)
        page_count += 1

        offset += LIMIT

    print(f"[INFO] 歌曲 {song_id} 完成, 总计爬取 {total_comments} 条, 新增 {total_new_comments} 条")
    return total_new_comments

async def main_with_songs(songs):
    """使用给定的歌曲列表运行"""
    async with aiohttp.ClientSession() as session:
        # 加载已完成的歌曲列表
        completed_songs = load_progress()
        print(f"[INFO] 已完成 {len(completed_songs)} 首歌曲，将跳过这些歌曲")

        for song in songs:
            song_id = song["id"]
            song_name = song["name"]

            # 跳过已完成的歌曲
            if song_id in completed_songs:
                print(f"[SKIP] 歌曲已下载完成: {song_name} (id={song_id})")
                continue

            try:
                new_count = await crawl_song(session, song_id, song_name, save_to_mongodb)
                # 爬取成功后标记为已完成
                completed_songs.add(song_id)
                save_progress(completed_songs)
            except Exception as e:
                print(f"[ERROR] 爬取歌曲 {song_id} 时出错: {e}")
                continue

            await asyncio.sleep(0.5)

# ================== 分析函数（可选） ==================
def analyze_time_distribution():
    """分析评论时间分布"""
    collection = get_mongo_collection()
    pipeline = [
        {"$group": {"_id": {"$substr": ["$timeStr", 0, 7]}, "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    result = list(collection.aggregate(pipeline))
    print("\n评论时间分布（按月）:")
    for r in result:
        print(f"{r['_id']}: {r['count']} 条")

def analyze_sentiment_distribution():
    """分析情感分布"""
    collection = get_mongo_collection()
    pipeline = [
        {"$group": {"_id": "$sentiment", "count": {"$sum": 1}}}
    ]
    result = list(collection.aggregate(pipeline))
    print("\n情感分析结果:")
    for r in result:
        print(f"{r['_id']}: {r['count']} 条")

def analyze_by_song():
    """按歌曲统计评论数"""
    collection = get_mongo_collection()
    pipeline = [
        {"$group": {"_id": {"song_id": "$song_id", "song_name": "$song_name"}, "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 20}
    ]
    result = list(collection.aggregate(pipeline))
    print("\n评论数最多的歌曲 (Top 20):")
    for r in result:
        print(f"{r['_id']['song_name']} (id={r['_id']['song_id']}): {r['count']} 条")

def get_collection_stats():
    """获取集合统计信息"""
    collection = get_mongo_collection()
    total = collection.count_documents({})
    songs = collection.distinct("song_id")
    print(f"\n=== MongoDB 统计 ===")
    print(f"总评论数: {total}")
    print(f"歌曲数量: {len(songs)}")

# ================== 入口 ==================
if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--test", action="store_true", help="使用测试文件")
    parser.add_argument("--file", type=str, help="指定歌曲文件")
    args, unknown = parser.parse_known_args()

    # 分析命令 (先处理子命令)
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "stats":
            get_collection_stats()
        elif cmd == "sentiment":
            analyze_sentiment_distribution()
        elif cmd == "time":
            analyze_time_distribution()
        elif cmd == "bysong":
            analyze_by_song()
        elif cmd == "test":
            print("[TEST MODE] 使用测试文件:", TEST_SONGS_FILE)
            songs = load_songs_from_file(TEST_SONGS_FILE)
            asyncio.run(main_with_songs(songs))
        elif args.test:
            print("[TEST MODE] 使用测试文件:", TEST_SONGS_FILE)
            songs = load_songs_from_file(TEST_SONGS_FILE)
            asyncio.run(main_with_songs(songs))
        elif args.file:
            print("[INFO] 使用指定文件:", args.file)
            songs = load_songs_from_file(args.file)
            asyncio.run(main_with_songs(songs))
        else:
            songs = load_songs()
            asyncio.run(main_with_songs(songs))
    else:
        songs = load_songs()
        asyncio.run(main_with_songs(songs))
