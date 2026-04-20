import aiohttp
import asyncio
import json
import sqlite3
import csv
import os
from datetime import datetime
from typing import Optional
from sqlalchemy import create_engine, Column, Integer, String, Text, BigInteger
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from pymongo import MongoClient
from textblob import TextBlob
from sqlalchemy import Float

# ================== 配置区 ==================
SONG_ID = 186016
BASE_URL = "http://localhost:4000/comment/music"
LIMIT = 100
CONCURRENCY = 5
DELAY = 1.2
MAX_RETRIES = 3

# 存储方式：sqlite / mongodb
STORAGE_TYPE = "sqlite"  # 改成 "mongodb" 即可

SQLITE_DB = "netease_comments.db"
OFFSET_FILE = "offset.txt"

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "netease"
MONGO_COLLECTION = "comments"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

semaphore = asyncio.Semaphore(CONCURRENCY)

# ================== 数据库模型 ==================
Base = declarative_base()

class Comment(Base):
    __tablename__ = "comments"

    id = Column(Integer, primary_key=True)
    commentId = Column(BigInteger, unique=True)
    content = Column(Text)
    time = Column(BigInteger)
    timeStr = Column(String)
    likedCount = Column(Integer)
    userId = Column(BigInteger)
    nickname = Column(String)
    vipType = Column(Integer)
    location = Column(String)
    comment_type = Column(String)  # hot / normal
    sentiment = Column(String)      # positive / neutral / negative
    polarity = Column(Float)

# ================== 存储抽象 ==================
class Storage:
    def save(self, comment):
        raise NotImplementedError

class SQLiteStorage(Storage):
    def __init__(self):
        self.engine = create_engine(f"sqlite:///{SQLITE_DB}")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def save(self, comment):
        session: Session = self.Session()
        try:
            exists = session.query(Comment).filter_by(commentId=comment["commentId"]).first()
            if not exists:
                c = Comment(**comment)
                session.add(c)
                session.commit()
        except Exception as e:
            session.rollback()
            print(f"[DB ERROR] {e}")
        finally:
            session.close()

class MongoStorage(Storage):
    def __init__(self):
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        self.collection = db[MONGO_COLLECTION]

    def save(self, comment):
        try:
            self.collection.update_one(
                {"commentId": comment["commentId"]},
                {"$set": comment},
                upsert=True
            )
        except Exception as e:
            print(f"[MONGO ERROR] {e}")

# ================== 工具函数 ==================
def load_offset():
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE, "r") as f:
            return int(f.read().strip())
    return 0

def save_offset(offset):
    with open(OFFSET_FILE, "w") as f:
        f.write(str(offset))

def analyze_sentiment(text):
    blob = TextBlob(text)
    polarity = blob.sentiment.polarity
    if polarity > 0.1:
        return "positive", polarity
    elif polarity < -0.1:
        return "negative", polarity
    else:
        return "neutral", polarity

def parse_comment(c, comment_type):
    user = c.get("user", {})
    content = c.get("content", "")
    sentiment, polarity = analyze_sentiment(content)

    return {
        "commentId": c.get("commentId"),
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

# ================== 爬虫核心 ==================
async def fetch_page(session, offset, storage):
    params = {
        "id": SONG_ID,
        "limit": LIMIT,
        "offset": offset
    }

    for attempt in range(1, MAX_RETRIES + 1):
        async with semaphore:
            try:
                async with session.get(BASE_URL, params=params, headers=HEADERS, timeout=10) as resp:
                    if resp.status != 200:
                        print(f"[WARN] offset={offset}, status={resp.status}")
                        continue

                    data = await resp.json()

                    comments = data.get("comments", [])
                    hot = data.get("hotComments", []) if offset == 0 else []

                    for c in hot:
                        storage.save(parse_comment(c, "hot"))
                    for c in comments:
                        storage.save(parse_comment(c, "normal"))

                    print(f"[OK] offset={offset}, hot={len(hot)}, normal={len(comments)}")

                    await asyncio.sleep(DELAY)
                    return data

            except Exception as e:
                print(f"[RETRY {attempt}] offset={offset}, error={e}")
                await asyncio.sleep(2)

    return None

# ================== 主流程 ==================
async def main():
    if STORAGE_TYPE == "sqlite":
        storage = SQLiteStorage()
    else:
        storage = MongoStorage()

    offset = load_offset()
    print(f"[INFO] 从 offset={offset} 开始续爬")

    async with aiohttp.ClientSession() as session:
        while True:
            data = await fetch_page(session, offset, storage)

            if not data:
                print("[ERROR] 连续失败，终止")
                break

            save_offset(offset)

            if not data.get("more"):
                print("[INFO] 爬取完成")
                break

            offset += LIMIT

# ================== 分析函数（可选） ==================
def analyze_time_distribution():
    if STORAGE_TYPE == "sqlite":
        conn = sqlite3.connect(SQLITE_DB)
        cur = conn.cursor()
        cur.execute("""
            SELECT substr(timeStr, 1, 7) as month, COUNT(*) 
            FROM comments 
            GROUP BY month 
            ORDER BY month
        """)
        result = cur.fetchall()
        print("\n📊 评论时间分布（按月）:")
        for row in result:
            print(f"{row[0]}: {row[1]} 条")
        conn.close()

    elif STORAGE_TYPE == "mongodb":
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        pipeline = [
            {"$group": {"_id": {"$substr": ["$timeStr", 0, 7]}, "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        result = list(db[MONGO_COLLECTION].aggregate(pipeline))
        print("\n📊 评论时间分布（按月）:")
        for r in result:
            print(f"{r['_id']}: {r['count']} 条")

def analyze_sentiment_distribution():
    if STORAGE_TYPE == "sqlite":
        conn = sqlite3.connect(SQLITE_DB)
        cur = conn.cursor()
        cur.execute("SELECT sentiment, COUNT(*) FROM comments GROUP BY sentiment")
        result = cur.fetchall()
        print("\n🎯 情感分析结果:")
        for row in result:
            print(f"{row[0]}: {row[1]} 条")
        conn.close()

    elif STORAGE_TYPE == "mongodb":
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        pipeline = [
            {"$group": {"_id": "$sentiment", "count": {"$sum": 1}}}
        ]
        result = list(db[MONGO_COLLECTION].aggregate(pipeline))
        print("\n🎯 情感分析结果:")
        for r in result:
            print(f"{r['_id']}: {r['count']} 条")

# ================== 入口 ==================
if __name__ == "__main__":
    asyncio.run(main())

    # # 分析
    # analyze_time_distribution()
    # analyze_sentiment_distribution()