import sqlite3
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import os

class DatabaseManager:
    def __init__(self, db_path: str = "rasis.db"):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """Initialize the database with required tables"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processed_hashes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    hash TEXT UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS post_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    post_data TEXT NOT NULL,
                    content TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    posted_at TIMESTAMP NULL,
                    status TEXT DEFAULT 'pending'
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS posting_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    queue_id INTEGER,
                    FOREIGN KEY (queue_id) REFERENCES post_queue (id)
                )
            """)
            conn.commit()

    def is_hash_processed(self, hash_value: str) -> bool:
        """Check if a news post hash has already been processed"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM processed_hashes WHERE hash = ?", (hash_value,))
            return cursor.fetchone() is not None

    def add_processed_hash(self, hash_value: str):
        """Add a hash to the processed hashes table"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR IGNORE INTO processed_hashes (hash) VALUES (?)",
                (hash_value,)
            )
            conn.commit()

    def add_to_queue(self, post_data: Dict, content: str) -> int:
        """Add a post to the queue and return the queue ID"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO post_queue (post_data, content) VALUES (?, ?)",
                (json.dumps(post_data), content)
            )
            conn.commit()
            return cursor.lastrowid

    def get_pending_posts(self, limit: Optional[int] = None) -> List[Dict]:
        """Get pending posts from the queue"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            query = """
                SELECT id, post_data, content, created_at
                FROM post_queue
                WHERE status = 'pending'
                ORDER BY created_at ASC
            """
            if limit:
                query += f" LIMIT {limit}"

            cursor.execute(query)
            rows = cursor.fetchall()

            return [
                {
                    'id': row[0],
                    'post_data': json.loads(row[1]),
                    'content': row[2],
                    'created_at': row[3]
                }
                for row in rows
            ]

    def mark_post_as_posted(self, queue_id: int):
        """Mark a queued post as posted"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            cursor.execute(
                "UPDATE post_queue SET status = 'posted', posted_at = ? WHERE id = ?",
                (now, queue_id)
            )
            cursor.execute(
                "INSERT INTO posting_log (queue_id) VALUES (?)",
                (queue_id,)
            )
            conn.commit()

    def get_posts_in_last_hour(self) -> int:
        """Get the number of posts made in the last hour"""
        one_hour_ago = (datetime.now() - timedelta(hours=1)).isoformat()

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM posting_log WHERE posted_at >= ?",
                (one_hour_ago,)
            )
            return cursor.fetchone()[0]

    def can_post_more(self, max_per_hour: int) -> bool:
        """Check if we can post more based on rate limit"""
        return self.get_posts_in_last_hour() < max_per_hour

    def get_queue_stats(self) -> Dict:
        """Get statistics about the queue"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Get pending count
            cursor.execute("SELECT COUNT(*) FROM post_queue WHERE status = 'pending'")
            pending = cursor.fetchone()[0]

            # Get posted count
            cursor.execute("SELECT COUNT(*) FROM post_queue WHERE status = 'posted'")
            posted = cursor.fetchone()[0]

            # Get posts in last hour
            posts_last_hour = self.get_posts_in_last_hour()

            return {
                'pending': pending,
                'posted': posted,
                'posts_last_hour': posts_last_hour
            }

    def cleanup_old_data(self, days_to_keep: int = 30):
        """Clean up old data to keep database size manageable"""
        cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).isoformat()

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Clean up old posted posts
            cursor.execute(
                "DELETE FROM post_queue WHERE status = 'posted' AND posted_at < ?",
                (cutoff_date,)
            )

            # Clean up old posting logs
            cursor.execute(
                "DELETE FROM posting_log WHERE posted_at < ?",
                (cutoff_date,)
            )

            conn.commit()
