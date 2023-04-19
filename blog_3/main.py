from fastapi import FastAPI
from typing import List
from bq import BigQuery
import pandas as pd
from models import Post

app = FastAPI()
bq = BigQuery()

@app.get("/posts", response_model=None)
async def get_posts_table_as_dataframe() -> pd.DataFrame:
    """
    Gets the posts table as a dataframe
    """
    return bq.get_posts_table_as_dataframe().astype(str).replace('nan', None)

@app.get("/posts/approved/image", response_model=None)
async def get_single_approved_image_post_as_df() -> pd.DataFrame:
    """
    Gets a single approved image post as a dataframe from the posts table
    """
    return bq.get_single_approved_image_post_as_df().astype(str).replace('nan', None)

@app.get("/posts/unapproved/image", response_model=None)
async def get_unapproved_image_posts_as_df() -> pd.DataFrame:
    """
    Gets the unapproved image posts as a dataframe
    """
    return bq.get_unapproved_image_posts_as_df().astype(str).replace('nan', None)

@app.post("/posts", response_model=None)
async def update_posts_using_df(posts: List[Post]):
    """
    Pushes a list of posts to the temp table and then uses it to update the main posts table
    """
    new_posts_df = pd.DataFrame([post.dict() for post in posts])
    bq.update_posts_using_df(new_posts_df)