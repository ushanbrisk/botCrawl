import os
import json
import random
import time
from typing import Dict, List, Any
import logging
from pathlib import Path
from songdownloadmanager import SongDownloadManager
from download_music import download_song_and_meta, download_comments, cookies

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PORT=5000
SONG_DOWNLOAD_FOLDER="/ssd/music/song_download/"
COMMENT_DOWNLOAD_FOLDER = "/ssd/music/comments_download/"
META_DOWNLOAD_FOLDER = "/ssd/music/meta_download/"

# =============================================================================
# 修改2: 添加分批次下载配置
# =============================================================================
BATCH_SIZE = 25  # 每批次下载数量（20-30首）
BATCH_REST_MIN = 2  # 每批次完成后休息最小时间（分钟）
BATCH_REST_MAX = 4  # 每批次完成后休息最大时间（分钟）


def download_song(song_id: int) -> bool:
    """
    下载单首歌曲的伪代码实现

    Args:
        song_id: 歌曲ID

    Returns:
        下载是否成功
    """
    try:
        logger.info(f"开始下载歌曲 {song_id}")

        download_song_and_meta(song_id, **cookies)

        download_comments(song_id)

        logger.info(f"歌曲 {song_id} 下载完成")
        return True

    except Exception as e:
        logger.error(f"下载歌曲 {song_id} 失败: {e}")
        return False


def is_blocked_response(response_data: dict) -> bool:
    """
    检测是否被封禁
    
    Args:
        response_data: API响应数据
    
    Returns:
        是否被封禁
    """
    # 常见封禁错误码
    blocked_codes = [-460, -501, 301, 302, 400, 401, 403, 503]
    code = response_data.get('code', 0)
    
    # 检查错误码
    if code in blocked_codes:
        return True
    
    # 检查是否返回空数据（可能是隐式封禁）
    if code == 200 and not response_data.get('data'):
        return True
        
    return False


def handle_blocked(manager: SongDownloadManager, consecutive_errors: int) -> None:
    """
    处理被封禁的情况
    
    Args:
        manager: 下载管理器
        consecutive_errors: 连续错误次数
    """
    wait_time = min(300, 60 * (2 ** consecutive_errors))  # 指数退避，最大等待5分钟
    logger.error(f"检测到可能被封禁，连续错误 {consecutive_errors} 次，等待 {wait_time} 秒后重试...")
    time.sleep(wait_time)


def main_download_task(song_list: List[Dict[str, Any]]) -> None:
    """
    主下载任务函数

    Args:
        song_list: 待下载的歌曲列表
        格式如下: [ {"id": xxx}, {"id": xxx}, ...]

    """
    manager = SongDownloadManager("download_state.json")

    try:
        # 1. 加载之前的状态
        manager.load_state()

        # 用于追踪批次
        batch_count = 0
        # 用于追踪连续错误次数
        consecutive_errors = 0

        # 2. 遍历歌曲列表
        for song_info in song_list:

            # 修改7: 添加随机化的请求间隔（3-15秒，更大的随机范围）
            sleep_time = random.uniform(3, 15)
            time.sleep(sleep_time)

            song_id = song_info["id"]

            # 检查是否已下载
            if manager.is_downloaded(song_id):
                logger.info(f"歌曲 {song_id} 已下载， 跳过")
                continue

            # 3. 尝试下载
            success = download_song(song_id)

            if success:
                # 4. 下载成功，立即保存状态
                manager.mark_as_downloaded(song_id)
                consecutive_errors = 0  # 重置连续错误计数
                
                # ===================================================================
                # 修改9: 分批次下载 - 每BATCH_SIZE首后休息一段时间
                # ===================================================================
                batch_count += 1  # 只对成功下载的歌曲计数
                if batch_count >= BATCH_SIZE:
                    # 随机休息1-2分钟
                    batch_rest_time = random.uniform(BATCH_REST_MIN * 60, BATCH_REST_MAX * 60)
                    logger.warning(f"已连续下载 {batch_count} 首，休息 {batch_rest_time/60:.1f} 分钟...")
                    time.sleep(batch_rest_time)
                    batch_count = 0  # 重置批次计数
                
                # 每下载5首保存一次，避免频繁IO
                if len(manager.session_downloaded_ids) % 5 == 0:
                    manager.save_state()
            else:
                # 记录失败尝试
                manager.mark_as_failed(song_id, "下载失败")
                consecutive_errors += 1
                
                # 连续3次失败，触发封禁检测
                if consecutive_errors >= 3:
                    handle_blocked(manager, consecutive_errors)

        # 5. 任务完成，保存最终状态
        manager.save_state()
        logger.warning(f"下载任务完成，本次下载 {len(manager.session_downloaded_ids)} 首歌曲")

    except KeyboardInterrupt:
        # 用户主动中断（Ctrl+C）
        logger.warning("检测到用户中断，正在保存当前状态...")
        manager.save_state()
        logger.warning(f"状态已保存，本次会话下载了 {len(manager.session_downloaded_ids)} 首歌曲")
        raise

    except Exception as e:
        # 其他异常
        logger.error(f"下载任务异常中断: {e}")
        logger.warning("正在保存当前状态...")
        manager.save_state()
        logger.warning(f"状态已保存，本次会话下载了 {len(manager.session_downloaded_ids)} 首歌曲")
        raise

    finally:
        # 无论成功还是异常，都显示进度
        progress = manager.get_progress()
        logger.warning(f"总进度: 已下载 {progress['total_downloaded']} 首歌曲")


def safe_download_task(song_list: List[Dict[str, Any]]) -> None:
    """
    安全的下载任务入口函数
    """
    try:
        main_download_task(song_list)
    except KeyboardInterrupt:
        logger.warning("下载任务被用户中断，状态已保存")
    except Exception as e:
        logger.error(f"下载任务异常终止: {e}")
        logger.warning("程序将在保存状态后退出")


# 使用示例
if __name__ == "__main__":

    task_song_id_list_file = "/home/luke/distributed_machine_learning/download_neteaseclude/unique_song_ids_shuffled.json"

    # 方法1：读取整个JSON文件
    with open(task_song_id_list_file, 'r', encoding='utf-8') as f:
        total_list = json.load(f)  # 直接返回Python对象

    # 运行下载任务
    safe_download_task(total_list)
