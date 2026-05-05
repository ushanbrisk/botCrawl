"""
歌词抓取脚本
从 unique_songs_by_name_part1.json 读取歌曲ID，调用歌词API，将结果存入MongoDB
"""

import json
import os
import re
import time
from datetime import datetime
from pathlib import Path

import pymongo
import requests
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# MongoDB配置
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "netease")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "lyrics")

# API配置
LYRIC_API_URL = os.getenv("LYRIC_API_URL", "http://localhost:4000/lyric")
SONGS_FILE = os.getenv("SONGS_FILE", "unique_songs_by_name_part1.json")

# 并发配置
CONCURRENCY = int(os.getenv("SONG_CONCURRENCY", "5"))
REQUEST_DELAY = float(os.getenv("DELAY", "0.5"))


def parse_time_tag(line: str) -> tuple[str, str]:
    """
    解析时间标签 [mm:ss.xx] 或 [mm:ss:xx]
    返回 (时间标签, 歌词内容)
    """
    # 匹配 [mm:ss.xx] 或 [mm:ss:xx] 格式
    pattern = r'\[(\d{2}:\d{2}[.:]\d{2,3})\](.*)'
    match = re.match(pattern, line)
    if match:
        return match.group(1), match.group(2).strip()
    return "", line.strip()


def parse_lyric_text(lyric_text: str) -> list[dict]:
    """
    解析LRC歌词文本，返回时间线列表
    """
    lines = lyric_text.strip().split("\n")
    result = []
    for line in lines:
        if not line.strip():
            continue
        time_tag, content = parse_time_tag(line)
        if content:
            result.append({
                "time": time_tag,
                "content": content
            })
    return result


def fetch_lyric(song_id: int) -> dict | None:
    """
    调用歌词API获取歌词
    """
    try:
        response = requests.get(
            LYRIC_API_URL,
            params={"id": song_id},
            timeout=10
        )
        if response.status_code == 200:
            return response.json()
        else:
            print(f"  [警告] 歌曲 {song_id} API返回状态码: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"  [错误] 歌曲 {song_id} 请求失败: {e}")
        return None


def process_lyric_data(song_id: int, song_name: str, data: dict) -> dict:
    """
    处理歌词API返回数据，提取需要存储的字段
    """
    # 解析歌词内容
    raw_lyric = data.get("lrc", {}).get("lyric", "")
    parsed_lyric = parse_lyric_text(raw_lyric)

    # 处理翻译歌词
    raw_trans = data.get("tlyric", {}).get("lyric", "")
    parsed_trans = parse_lyric_text(raw_trans) if raw_trans else []

    # 处理罗马字音译
    raw_roma = data.get("romalrc", {}).get("lyric", "")
    parsed_roma = parse_lyric_text(raw_roma) if raw_roma else []

    # 处理逐字歌词
    raw_karaoke = data.get("klyric", {}).get("lyric", "")
    parsed_karaoke = parse_lyric_text(raw_karaoke) if raw_karaoke else []

    # 歌词贡献者信息
    lyric_user = data.get("lyricUser", {})
    lyric_user_info = {}
    if lyric_user:
        lyric_user_info = {
            "id": lyric_user.get("id"),
            "status": lyric_user.get("status"),
            "demand": lyric_user.get("demand"),
            "user_id": lyric_user.get("userid"),
            "nickname": lyric_user.get("nickname"),
            "uptime": lyric_user.get("uptime")
        }

    return {
        "song_id": song_id,
        "song_name": song_name,
        "code": data.get("code"),
        # 标记位
        "has_original": data.get("sfy", False),  # 是否有原歌词翻译
        "has_translation": data.get("qfy", False),  # 是否有翻译
        "is_premium": data.get("sgc", False),  # 是否付费
        # 歌词内容
        "original_lyric": {
            "version": data.get("lrc", {}).get("version", 0),
            "raw": raw_lyric,
            "parsed": parsed_lyric
        },
        "translation_lyric": {
            "version": data.get("tlyric", {}).get("version", 0),
            "raw": raw_trans,
            "parsed": parsed_trans
        },
        "romaji_lyric": {
            "version": data.get("romalrc", {}).get("version", 0),
            "raw": raw_roma,
            "parsed": parsed_roma
        },
        "karaoke_lyric": {
            "version": data.get("klyric", {}).get("version", 0),
            "raw": raw_karaoke,
            "parsed": parsed_karaoke
        },
        # 贡献者信息
        "lyric_contributor": lyric_user_info,
        # 元数据
        "fetched_at": datetime.now(),
        "api_response": data  # 保留原始响应
    }


def main():
    # 读取歌曲文件
    songs_file = Path(SONGS_FILE)
    if not songs_file.exists():
        # 尝试在当前目录查找
        songs_file = Path(__file__).parent / SONG_FILE
        if not songs_file.exists():
            print(f"[错误] 歌曲文件不存在: {SONGS_FILE}")
            return

    print(f"读取歌曲文件: {songs_file}")
    with open(songs_file, "r", encoding="utf-8") as f:
        songs = json.load(f)

    print(f"共 {len(songs)} 首歌曲")

    # 连接MongoDB
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    # 创建索引
    collection.create_index("song_id", unique=True)

    # 统计
    success_count = 0
    skip_count = 0
    error_count = 0

    for i, song in enumerate(songs):
        song_id = song.get("id")
        song_name = song.get("name", "")

        if not song_id:
            print(f"[{i+1}/{len(songs)}] 跳过无效歌曲数据")
            error_count += 1
            continue

        # 检查是否已存在
        if collection.find_one({"song_id": song_id}):
            print(f"[{i+1}/{len(songs)}] 跳过已存在的歌曲: {song_name} ({song_id})")
            skip_count += 1
            continue

        print(f"[{i+1}/{len(songs)}] 获取歌词: {song_name} ({song_id})")

        data = fetch_lyric(song_id)
        if data and data.get("code") == 200:
            processed = process_lyric_data(song_id, song_name, data)
            collection.insert_one(processed)
            success_count += 1
            print(f"  -> 成功入库")
        else:
            error_count += 1
            print(f"  -> 获取失败")

        # 避免请求过快
        time.sleep(REQUEST_DELAY)

    print(f"\n完成!")
    print(f"  成功: {success_count}")
    print(f"  跳过: {skip_count}")
    print(f"  失败: {error_count}")

    client.close()


if __name__ == "__main__":
    main()
