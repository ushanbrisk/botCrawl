import aiohttp
import asyncio
import csv
import json
from datetime import datetime

SONG_ID = 186016
# SONG_ID = 210049
BASE_URL = "http://localhost:4000/comment/music"
LIMIT = 100          # 最大允许
CONCURRENCY = 5      # 并发数（不要太大）
DELAY = 1.2          # 每次请求后等待（防封）

OUTPUT_FILE = f"netease_comments_{SONG_ID}.csv"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# 全局锁，防止 CSV 写冲突
csv_lock = asyncio.Lock()
semaphore = asyncio.Semaphore(CONCURRENCY)

def parse_comment(c):
    """提取单条评论字段"""
    user = c.get("user", {})
    return {
        "commentId": c.get("commentId"),
        "content": c.get("content"),
        "time": c.get("time"),
        "timeStr": c.get("timeStr"),
        "likedCount": c.get("likedCount"),
        "userId": user.get("userId"),
        "nickname": user.get("nickname"),
        "vipType": user.get("vipType"),
        "location": c.get("ipLocation", {}).get("location"),
    }

async def fetch_page(session, offset, writer):
    url = BASE_URL
    params = {
        "id": SONG_ID,
        "limit": LIMIT,
        "offset": offset
    }

    async with semaphore:
        try:
            async with session.get(url, params=params, headers=headers, timeout=10) as resp:
                if resp.status != 200:
                    print(f"[ERROR] offset={offset}, status={resp.status}")
                    return None

                data = await resp.json()

                comments = data.get("comments", [])
                hot_comments = data.get("hotComments", []) if offset == 0 else []

                rows = []

                # 第一页：先存 hotComments
                for c in hot_comments:
                    row = parse_comment(c)
                    row["type"] = "hot"
                    rows.append(row)

                # 普通评论
                for c in comments:
                    row = parse_comment(c)
                    row["type"] = "normal"
                    rows.append(row)

                # 写 CSV（加锁）
                async with csv_lock:
                    for row in rows:
                        writer.writerow(row)

                print(f"[OK] offset={offset}, hot={len(hot_comments)}, normal={len(comments)}")

                await asyncio.sleep(DELAY)

                return data

        except Exception as e:
            print(f"[EXCEPTION] offset={offset}: {e}")
            return None

async def main():
    async with aiohttp.ClientSession() as session:
        # 打开 CSV
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "type", "commentId", "content", "time", "timeStr",
                    "likedCount", "userId", "nickname", "vipType", "location"
                ]
            )
            writer.writeheader()

            offset = 0
            total = None

            while True:
                data = await fetch_page(session, offset, writer)

                if not data:
                    break

                if total is None:
                    total = data.get("total")
                    print(f"[INFO] 总评论数: {total}")

                if not data.get("more"):
                    print("[INFO] 已爬取完毕")
                    break

                offset += LIMIT
                if offset == 700:
                    print("offset: 700")

if __name__ == "__main__":
    asyncio.run(main())