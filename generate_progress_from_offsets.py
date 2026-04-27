import json
import os
songs = sorted([f[:-4] for f in os.listdir('/ssd/music/comments/offset') if f.endswith('.txt')])
with open('/ssd/music/comments/progress.json', 'w') as f:
    json.dump({"completed_songs": songs}, f, indent=2)

