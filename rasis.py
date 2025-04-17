import http.client
import json
import hashlib
import requests
from dotenv import load_dotenv
import os

load_dotenv()


HASHED_NEWS_FILES = os.environ.get("HASH_FILE_PATH")

def generate_queued_posts() -> list:
    def news_already_hashed(line: str) -> bool:
        try:
            with open(HASHED_NEWS_FILES, "r") as file:
                return line in file.read()
        except FileNotFoundError:
            with open(HASHED_NEWS_FILES, "w") as file:
                pass
            return False

    def append_to_hash_file(line: str):
        with open(HASHED_NEWS_FILES, "a") as file:
            file.write(line + "\n")
    url = "https://arcade-news.pinapelz.com/news.json"
    response = requests.get(url)
    queue = []
    if response.status_code == 200:
        data = response.json()
        news_posts = data["news_posts"]
        for post in news_posts:
            hash = hashlib.sha256(f"{post['identifier'] + post['content'] + post['date']}".encode('utf-8')).hexdigest()
            if news_already_hashed(hash):
                continue
            queue.append(post)
            append_to_hash_file(hash)
    else:
        print(f"Failed to download JSON. Status code: {response.status_code}")
        exit(1)
    return queue


def generate_post_content(post_data: dict) -> str:
    """
    📰 GAME - DATE
    [type] headline?
    content?
    🔗 [link]
    🖼 [image?](link)
    """
    if "IIDX" in post_data["identifier"]:
        game = "beatmania IIDX"
        tags = "#iidx #beatmania #bemani"
    elif "SOUND_VOLTEX" in post_data["identifier"]:
        game = "SOUND VOLTEX"
        tags = "#sdvx #soundvoltex #bemani"
    elif "DDR" in post_data["identifier"]:
        game = "DanceDanceRevolution"
        tags = "#ddr #dancedancerevolution #bemani"
    elif "POPN_MUSIC" in post_data["identifier"]:
        game = "pop'n music"
        tags = "#popn #bemani"
    elif "JUBEAT" in post_data["identifier"]:
        game = "jubeat"
        tags = "#jubeat #bemani"
    elif "GITADORA " in post_data["identifier"]:
        game = "GITADORA"
        tags = "#gitadora #bemani"
    elif "NOSTALGIA" in post_data["identifier"]:
        game = "NOSTALGIA"
        tags = "#bemani"
    elif "CHUNITHM_JP" in post_data["identifier"]:
        post_data['headline'] = None
        game = "CHUNITHM (JPN)"
        tags = "#chunithm"
    elif "CHUNITHM_INTL" in post_data["identifier"]:
        post_data['headline'] = None
        game = "CHUNITHM (International)"
        tags = "#chunithm"
    elif "MAIMAIDX_JP" in post_data["identifier"]:
        post_data['headline'] = None
        game = "maimai DX (JPN)"
        tags = "#maimaidx"
    elif "MAIMAIDX_INTL" in post_data["identifier"]:
        post_data['headline'] = None
        game = "maimai DX (International)"
        tags = "#maimaidx"
    elif "ONGEKI_JPN" in post_data["identifier"]:
        post_data['headline'] = None
        game = "O.N.G.E.K.I (JPN)"
        tags = "#ongeki"
    content = f"📰 {game} - {post_data['date']}\n\n"
    if post_data["type"] is not None:
        content = content + f"[{post_data['type']}] "
    if post_data["headline"] is not None and post_data["headline"]  != post_data["content"] :
        content = content + f"[{post_data['headline']}]\n\n"
    content = content + post_data["content"] + "\n\n"
    if post_data["url"] is not None:
        content = content + f"🔗 {post_data['url']}\n"
    for i in range(len(post_data["images"])):
        content = content + f"🖼 [Image{i+1}]({post_data['images'][i]['image']})\n"
    content = content + tags
    return content


def post_on_fedi(content: str):
    conn = http.client.HTTPSConnection(os.environ.get("SHARKEY_INSTANCE"))

    payload = {
        "visibility": "public",
        "text": content,
        "localOnly": False,
        "noExtractMentions": False,
        "noExtractHashtags": False,
        "noExtractEmojis": False
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + os.environ.get("SHARKEY_KEY")
    }
    conn.request("POST", "/api/notes/create", json.dumps(payload), headers)
    res = conn.getresponse()
    data = res.read()

if __name__ == "__main__":
    queued_posts = generate_queued_posts()
    for post in queued_posts:
        content = generate_post_content(post)
        cleaned = content.encode("utf-8", "replace").decode("utf-8")
        post_on_fedi(cleaned)
