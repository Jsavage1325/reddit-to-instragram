from typing import List, Optional
from pydantic import BaseModel

class Post(BaseModel):
    title: str
    url: str
    audio_url: Optional[str] = None
    filename: Optional[str] = None
    score: int
    source: Optional[str] = None
    type: Optional[str] = None
    added: Optional[bool] = None
    approved: Optional[bool] = None