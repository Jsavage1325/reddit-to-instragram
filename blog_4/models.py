from typing import List, Optional
from pydantic import BaseModel

class Post(BaseModel):
    """
    Model to contain information on the posts we are using
    """
    title: str
    url: str
    audio_url: Optional[str] = None
    filename: Optional[str] = None
    score: int
    source: Optional[str] = None
    type: Optional[str] = None
    added: Optional[bool] = None
    approved: Optional[bool] = None

class User(BaseModel):
    """
    User class containing information on users, will be used to store users in a DB
    """
    username: str
    email: str = None
    hashed_password: str
    disabled: bool = False