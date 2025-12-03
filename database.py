import sqlite3
from datetime import datetime, timedelta
from typing import Optional

class DatabaseManager:
    def __init__(self, db_path: str = "rasis.db"):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """Initialize the database with a simple posted_posts table"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS posted_posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    archive_hash TEXT UNIQUE NOT NULL,
                    posted_at TIMESTAMP NOT NULL
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_posted_at
                ON posted_posts(posted_at)
            """)
            conn.commit()

    def is_posted(self, archive_hash: str) -> bool:
        """Check if we've already posted this hash"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM posted_posts WHERE archive_hash = ?", (archive_hash,))
            return cursor.fetchone() is not None

    def mark_as_posted(self, archive_hash: str):
        """Mark a post as posted"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR IGNORE INTO posted_posts (archive_hash, posted_at) VALUES (?, ?)",
                (archive_hash, datetime.now().isoformat())
            )
            conn.commit()

    def get_posts_count_last_hour(self) -> int:
        """How many posts did we make in the last hour?"""
        one_hour_ago = datetime.now() - timedelta(hours=1)
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM posted_posts WHERE posted_at >= ?",
                (one_hour_ago.isoformat(),)
            )
            return cursor.fetchone()[0]

    def can_post_more(self, max_per_hour: int) -> bool:
        """Can we post more within the rate limit?"""
        return self.get_posts_count_last_hour() < max_per_hour

    def get_next_post_time(self, max_per_hour: int) -> Optional[datetime]:
        """Get the time when the next post can be made"""
        if self.can_post_more(max_per_hour):
            return None  # Can post now
        one_hour_ago = datetime.now() - timedelta(hours=1)
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT posted_at FROM posted_posts WHERE posted_at >= ? ORDER BY posted_at ASC LIMIT 1",
                (one_hour_ago.isoformat(),)
            )
            result = cursor.fetchone()
            if result:
                oldest_post_time = datetime.fromisoformat(result[0])
                return oldest_post_time + timedelta(hours=1)
        return None

    def cleanup_old_data(self, days_to_keep: int = 90):
        """Optional: Clean up very old entries to keep DB small"""
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM posted_posts WHERE posted_at < ?",
                (cutoff_date.isoformat(),)
            )
            conn.commit()
