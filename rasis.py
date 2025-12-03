import http.client
import json
import hashlib
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
        if isinstance(post_date, str):
            post_timestamp = int(post_date)
        else:
            post_timestamp = int(post_date)
        post_datetime = datetime.fromtimestamp(post_timestamp)
        start_datetime = datetime.strptime(START_DATE, "%Y-%m-%d")
        return post_datetime >= start_datetime
    except (ValueError, TypeError):
        return True

def generate_queued_posts(db: DatabaseManager, dry_run: bool = False) -> list:
    """Fetch new posts and add them to the queue"""
    url = "https://arcade-news.pinapelz.com/news.json"
    response = requests.get(url)
    new_posts = []
    if response.status_code == 200:
        data = response.json()
        news_posts = data["news_posts"]
        for post in news_posts:
            if not is_post_after_start_date(post.get('timestamp', '')):
                continue
            post_hash = hashlib.sha256(
                f"{post['identifier'] + post['content'] + post['date']}".encode('utf-8')
            ).hexdigest()
            if db.is_hash_processed(post_hash):
                continue
            content = generate_post_content(post)
            if content is None: # skip if we do not handle the game
                continue
            if not dry_run:
                db.add_to_queue(post, content)
                db.add_processed_hash(post_hash)
            else:
                print(f"[DRY RUN] Would add to queue: {post['identifier']} - {post['date']}")
            new_posts.append(post)

    else:
        print(f"Failed to download JSON. Status code: {response.status_code}")
        return new_posts

    return new_posts

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
    else:
        return None
    content = f"📰 {game} - {post_data['date']}\n\n"
    if post_data["is_ai_summary"]:
        content = content + "The information below is written by AI / 上記の情報はAIによって生成されました。\n\n"
    if post_data["type"] is not None:
        content = content + f"[{post_data['type']}] "
    if post_data["headline"] is not None and post_data["headline"] != post_data["content"]:
        content = content + f"{post_data['headline']}\n\n"
    if len(post_data["content"]) > 2500:
        truncated_content = post_data["content"][:2500] + "..."
        content = content + truncated_content + "\n\n"
    else:
        content = content + post_data["content"] + "\n\n"

    if post_data["url"] is not None:
        content = content + f"🔗 {post_data['url']}\n"

    for i in range(len(post_data["images"])):
        content = content + f"🖼 [Image{i+1}]({post_data['images'][i]['image']})\n"
    content = content + tags
    return content

def post_on_fedi(content: str, dry_run: bool = False) -> bool:
    """Post content to Fediverse"""
    if dry_run:
        print("[DRY RUN] Would post to Fediverse:")
        print("-" * 50)
        print(content)
        print("-" * 50)
        return True

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

        # Check if post was successful
        if res.status == 200:
            return True
        else:
            print(f"Failed to post: {res.status} - {data.decode()}")
            return False
    except Exception as e:
        print(f"Error posting to Fediverse: {e}")
        return False

def process_queue(db: DatabaseManager, dry_run: bool = False):
    """Process the posting queue with rate limiting"""
    if not db.can_post_more(POSTS_PER_HOUR):
        posts_made = db.get_posts_in_last_hour()
        print(f"Rate limit reached: {posts_made}/{POSTS_PER_HOUR} posts made in the last hour")
        return

    posts_made = db.get_posts_in_last_hour()
    available_slots = max(0, POSTS_PER_HOUR - posts_made)

    pending_posts = db.get_pending_posts(limit=available_slots)

    if not pending_posts:
        print("No pending posts to process")
        return

    print(f"Processing {len(pending_posts)} posts (rate limit: {posts_made + len(pending_posts)}/{POSTS_PER_HOUR})")

    for post in pending_posts:
        post_id = post['id']
        content = post['content']
        post_data = post['post_data']

        cleaned_content = content.encode("utf-8", "replace").decode("utf-8")

        game_name = post_data.get('identifier', 'Unknown')
        print(f"Processing: {game_name} - {post_data.get('date', 'Unknown date')}")

        if post_on_fedi(cleaned_content, dry_run):
            if not dry_run:
                db.mark_post_as_posted(post_id)
            print("✓ Posted successfully" if not dry_run else "✓ Would post successfully")
        else:
            print("✗ Failed to post")

def show_status(db: DatabaseManager):
    """Show current queue and rate limit status"""
    stats = db.get_queue_stats()

    print(f"""
=== RASIS Status ===
Pending posts: {stats['pending']}
Posted posts: {stats['posted']}
Posts in last hour: {stats['posts_last_hour']}/{POSTS_PER_HOUR}
Rate limit slots available: {max(0, POSTS_PER_HOUR - stats['posts_last_hour'])}
Database path: {DB_PATH}
""")

def main():
    parser = argparse.ArgumentParser(description='RASIS - Arcade News Posting Bot')
    parser.add_argument('--dry-run', action='store_true', help='Simulate posting without actually posting')
    parser.add_argument('--status', action='store_true', help='Show current status and exit')
    parser.add_argument('--process-only', action='store_true', help='Only process queue, don\'t fetch new posts')
    parser.add_argument('--fetch-only', action='store_true', help='Only fetch new posts, don\'t process queue')
    parser.add_argument('--cleanup', action='store_true', help='Clean up old data from database')

    args = parser.parse_args()
    dry_run = args.dry_run or DRY_RUN

    if dry_run:
        print("🧪 DRY RUN MODE - No actual posting will occur")

    db = DatabaseManager(DB_PATH)

    if args.status:
        show_status(db)
        return

    if args.cleanup:
        print("Cleaning up old data...")
        db.cleanup_old_data()
        print("Cleanup complete")
        return

    if not args.process_only:
        print("Fetching new posts...")
        new_posts = generate_queued_posts(db, dry_run)
        print(f"Added {len(new_posts)} new posts to queue")
    if not args.fetch_only:
        print("Processing queue...")
        process_queue(db, dry_run)
    show_status(db)

if __name__ == "__main__":
    main()
