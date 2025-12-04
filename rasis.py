import http.client
import json
import requests
from dotenv import load_dotenv
import os
import argparse
from datetime import datetime
from database import DatabaseManager

load_dotenv()

DB_PATH = os.environ.get("DB_PATH", "rasis.db")
START_DATE = os.environ.get("START_DATE", "")
POSTS_PER_HOUR = int(os.environ.get("POSTS_PER_HOUR", "3"))
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"

def is_post_after_start_date(post_date) -> bool:
    """Check if post date is after the configured start date"""
    if not START_DATE:
        return True
    try:
        post_timestamp = int(post_date) if isinstance(post_date, str) else int(post_date)
        post_datetime = datetime.fromtimestamp(post_timestamp)
        start_datetime = datetime.strptime(START_DATE, "%Y-%m-%d")
        return post_datetime >= start_datetime
    except (ValueError, TypeError):
        return True

def generate_post_content(post_data: dict) -> str:
    """Generate post content from post data"""
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
    elif "TAIKO" in post_data["identifier"]:
        game = "Taiko no Tatsujin"
        tags = "#taikonotatsufin"
    elif "MUSIC_DIVER" in post_data["identifier"]:
        game = "MUSIC DIVER"
        tags = "#music_diver"
    else:
        return None

    content = f"📰 {game} - {post_data['date']}\n\n"

    if post_data["is_ai_summary"]:
        content += "The information below is written by AI / 上記の情報はAIによって生成されました。\n\n"

    if post_data["type"] is not None:
        content += f"[{post_data['type']}] "

    if post_data["headline"] is not None and post_data["headline"] != post_data["content"]:
        content += f"{post_data['headline']}\n\n"

    if len(post_data["content"]) > 2500:
        truncated_content = post_data["content"][:2500] + "..."
        content += truncated_content + "\n\n"
    else:
        content += post_data["content"] + "\n\n"

    if post_data["url"] is not None:
        content += f"🔗 {post_data['url']}\n"

    content += f"[🔗 MORE DETAILS HERE](https://ac.moekyun.me/news?post={post_data['archive_hash']})\n"
    content += tags
    return content

def post_on_fedi(content: str, dry_run: bool = False) -> bool:
    """Post content to Fediverse"""
    print(f"[DRY RUN] Would post:\n{'-' * 60}\n{content}\n{'-' * 60}")
    try:
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

        if res.status == 200:
            return True
        else:
            print(f"Failed to post: {res.status} - {data.decode()}")
            return False
    except Exception as e:
        print(f"Error posting to Fediverse: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='RASIS - Simple Arcade News Bot')
    parser.add_argument('--dry-run', action='store_true', help='Simulate without posting')
    parser.add_argument('--status', action='store_true', help='Show current status')
    parser.add_argument('--cleanup', action='store_true', help='Clean up old data (90+ days)')

    args = parser.parse_args()
    dry_run = args.dry_run or DRY_RUN

    db = DatabaseManager(DB_PATH)

    if args.status:
        posts_in_hour = db.get_posts_count_last_hour()
        print(f"Posts in last hour: {posts_in_hour}/{POSTS_PER_HOUR}")
        print(f"Can post more: {db.can_post_more(POSTS_PER_HOUR)}")

        next_post_time = db.get_next_post_time(POSTS_PER_HOUR)
        if next_post_time:
            time_until = next_post_time - datetime.now()
            minutes = int(time_until.total_seconds() / 60)
            print(f"Next post can be made at: {next_post_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Time until next post: {minutes} minutes")
        else:
            print("Can post now!")
        return

    if args.cleanup:
        print("Cleaning up old data...")
        db.cleanup_old_data()
        return

    if not db.can_post_more(POSTS_PER_HOUR):
        posts_made = db.get_posts_count_last_hour()
        print(f"Rate limit reached: {posts_made}/{POSTS_PER_HOUR} posts in the last hour")
        return

    print(f"Starting to process posts (rate limit: {POSTS_PER_HOUR}/hour)")
    url = "https://arcade-news.pinapelz.com/news.json"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to download JSON. Status code: {response.status_code}")
        return

    data = response.json()
    news_posts = sorted(data["news_posts"], key=lambda x: x.get('timestamp', 0))

    posts_to_make = []
    for post in news_posts:
        if not is_post_after_start_date(post.get('timestamp', '')):
            continue

        archive_hash = post["archive_hash"]
        if db.is_posted(archive_hash):
            continue

        content = generate_post_content(post)
        if content is None:
            continue

        posts_to_make.append((post, content, archive_hash))

    if not posts_to_make:
        print("No new posts to make")
        return

    print(f"Found {len(posts_to_make)} new posts to potentially make")

    posts_made_this_run = 0
    for post_data, content, archive_hash in posts_to_make:
        if not db.can_post_more(POSTS_PER_HOUR):
            current_count = db.get_posts_count_last_hour()
            print(f"Rate limit reached: {current_count}/{POSTS_PER_HOUR}. Stopping.")
            break

        game = post_data.get('identifier', 'Unknown')
        date = post_data.get('date', 'Unknown date')

        print(f"Posting: {game} - {date}")

        if post_on_fedi(content, dry_run):
            if not dry_run:
                db.mark_as_posted(archive_hash)
            posts_made_this_run += 1
            print(f"  ✓ Posted successfully ({posts_made_this_run} posted this run)")
        else:
            print("  ✗ Failed to post")
            break

    posts_in_hour = db.get_posts_count_last_hour()
    print(f"\nStatus: {posts_in_hour}/{POSTS_PER_HOUR} posts made in last hour")

if __name__ == "__main__":
    main()
