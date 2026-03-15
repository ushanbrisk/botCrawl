import os
import json
import time
from typing import Dict, List, Any
import logging
from pathlib import Path
from download_music import download_song_and_meta, download_comments, cookies


# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SongDownloadManager:
    """歌曲下载管理器，支持断点续传和状态保存"""

    def __init__(self, state_file: str = "download_state.json"):
        """
        初始化下载管理器

        Args:
            state_file: 状态保存文件路径
        """
        self.state_file = state_file
        self.state: Dict[str, Any] = {}
        self.session_downloaded_ids: List[int] = []  # 本次会话下载的歌曲ID

    def load_state(self) -> None:
        """加载下载状态"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    self.state = json.load(f)
                logger.info(f"已加载状态文件: {self.state_file}")
                logger.info(f"已下载歌曲数量: {len(self.state.get('song_ids_downloaded', []))}")
            else:
                # 文件不存在，创建初始状态
                self.state = {
                    "song_ids_downloaded": [],  # 已下载的歌曲ID列表
                    "total_downloaded": 0,  # 总下载数量
                    "last_update_time": None,  # 最后更新时间
                    "failed_attempts": {}  # 失败的尝试记录
                }
                logger.info(f"状态文件不存在，创建初始状态: {self.state_file}")
        except Exception as e:
            logger.error(f"加载状态文件失败: {e}")
            raise

    def save_state(self, immediate_save: bool = True) -> None:
        """
        保存下载状态

        Args:
            immediate_save: 是否立即保存（False表示只记录，稍后保存）
        """
        if immediate_save:
            self._do_save()

    def _do_save(self) -> None:
        """实际执行保存操作"""
        try:
            # 更新状态信息
            self.state["last_update_time"] = time.strftime("%Y-%m-%d %H:%M:%S")

            # 确保目录存在
            os.makedirs(os.path.dirname(self.state_file) or '.', exist_ok=True)

            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, ensure_ascii=False, indent=2)

            logger.info(f"状态已保存到: {self.state_file}")
        except Exception as e:
            logger.error(f"保存状态文件失败: {e}")
            # 这里可以选择重试或保存到临时文件

    def is_downloaded(self, song_id: int) -> bool:
        """检查歌曲是否已下载"""
        return song_id in self.state.get("song_ids_downloaded", [])

    def mark_as_downloaded(self, song_id: int) -> None:
        """标记歌曲为已下载"""
        if not self.is_downloaded(song_id):
            self.state.setdefault("song_ids_downloaded", []).append(song_id)
            self.state["total_downloaded"] = len(self.state["song_ids_downloaded"])
            self.session_downloaded_ids.append(song_id)

    def mark_as_failed(self, song_id: int, error: str) -> None:
        """记录失败尝试"""
        failed_attempts = self.state.setdefault("failed_attempts", {})
        attempts = failed_attempts.setdefault(str(song_id), [])
        attempts.append({
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "error": error
        })

    def get_progress(self) -> Dict[str, Any]:
        """获取下载进度信息"""
        return {
            "total_downloaded": self.state.get("total_downloaded", 0),
            "session_downloaded": len(self.session_downloaded_ids),
            "last_update": self.state.get("last_update_time"),
            "failed_count": len(self.state.get("failed_attempts", {}))
        }

