import praw
from datetime import datetime
import pandas as pd
import config
from typing import List, Tuple

class RedditScraper:
    def __init__(self):
        self.reddit = praw.Reddit(
            client_id=config.REDDIT_API_CLIENT_ID,
            client_secret=config.REDDIT_API_SECRET,
            user_agent=config.REDDIT_USER_AGENT,
        )

    def get_hot_posts_this_week(self, subreddit: str, count: int) -> Tuple[List[dict], List[dict], List[dict]]:
        """
        Gets the top count hot posts this week from a certain subreddit
        Looks for posts in the last week, hence will need to be run weekly
        """
        hot_posts = self.reddit.subreddit(subreddit).top(time_filter="week", limit=count)
        image_posts = [
            {
                "title": post.title,
                "url": post.url,
                "audio_url": None,
                "filename": str(hash(post.url))[:15] + ".jpg"
                if ".jpg" in post.url
                else str(hash(post.url))[:15] + ".png",
                "score": post.score,
                "source": subreddit,
                "type": "image",
                "last_updated": datetime.now(),
                "added": False,
                "approved": None,
            }
            for post in hot_posts
            if (".jpg" in post.url) or (".png" in post.url)
        ]
        gif_posts = [
            {
                "title": post.title,
                "url": post.url,
                "audio_url": None,
                "filename": str(hash(post.url))[:15] + ".gif",
                "score": post.score,
                "source": subreddit,
                "type": "gif",
                "last_updated": datetime.now(),
                "added": False,
                "approved": None,
            }
            for post in hot_posts
            if ".gif" in post.url
        ]
        video_posts = [
            {
                "title": post.title,
                "url": post.media.get("reddit_video").get("fallback_url"),
                "audio_url": None,
                "filename": str(hash(post.url))[:15] + ".mp4",
                "score": post.score,
                "source": subreddit,
                "type": "video",
                "last_updated": datetime.now(),
                "added": False,
                "approved": None,
            }
            for post in hot_posts
            if post.media and post.media.get("reddit_video") and post.media.get("reddit_video").get("fallback_url")
        ]
        return image_posts, gif_posts, video_posts

    def convert_posts_to_dataframe(self, posts: list) -> pd.DataFrame:
        """
        Converts a list of posts to a dataframe
        'title', 'url', 'filename', 'score', 'source', 'type'
        """
        columns = [
            "title",
            "url",
            "audio_url",
            "filename",
            "score",
            "source",
            "type",
            "added",
            "approved",
        ]
        dataframe = pd.DataFrame(posts, columns=columns)
        return dataframe

    def scrape_reddit_for_top_posts(
        self, list_of_subreddits_to_search: dict = None
    ):
        """
        Scrapes a bunch of top posts from reddit and pushed them to a temp table in gbq and then updates the main table
        using the temp table
        """
        if list_of_subreddits_to_search is None:
            list_of_subreddits_to_search = [
                {"subreddit": "memes", "count": 250},
                {"subreddit": "me_irl", "count": 100},
                {"subreddit": "dankmemes", "count": 100},
                {"subreddit": "memeeconomy", "count": 100},
                {"subreddit": "holup", "count": 100},
                {"subreddit": "funny", "count": 100},
                {"subreddit": "adviceanimals", "count": 25},
                {"subreddit": "oddlysatisfying", "count": 100},
                {"subreddit": "facepalm", "count": 100},
            ]

        all_posts = []
        for subreddit in list_of_subreddits_to_search:
            image_posts, gif_posts, video_posts = self.get_hot_posts_this_week(
                subreddit["subreddit"], subreddit["count"]
            )
            all_posts.extend(image_posts)
            all_posts.extend(gif_posts)
            all_posts.extend(video_posts)

        # return all of the posts from this week as a dataframe
        return self.convert_posts_to_dataframe(all_posts)
