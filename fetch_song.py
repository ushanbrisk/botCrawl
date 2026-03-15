import requests

# 1. 获取歌单详情（例如：云音乐热歌榜）
playlist_id = "3778678"  # 热歌榜的ID
url = f"http://localhost:5000/playlist/detail?id={playlist_id}"
response = requests.get(url)
data = response.json()

# 2. 提取歌曲ID和歌名
songs = data['playlist']['tracks']
for song in songs:
    song_id = song['id']
    song_name = song['name']
    # 获取歌手名（可选）
    artists = [artist['name'] for artist in song['ar']]
    print(f"ID: {song_id}, 歌名: {song_name}, 歌手: {artists}")