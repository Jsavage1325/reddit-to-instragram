from fastapi import FastAPI, Depends, HTTPException, status
from typing import List, Annotated
from bq import BigQuery
import pandas as pd
from models import Post, User
from auth import Auth
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import json

app = FastAPI()
auth = Auth()
bq = BigQuery()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def df_to_list_of_dict(df: pd.DataFrame) -> list[dict]:
    """
    Converts as dataframe to a list of dict, used to directly output from an API endpoint
    """
    res = df.to_json(orient="records")
    parsed = json.loads(res)
    return parsed

async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]):
    """
    Decodes an access token, and authenticates user.
    Returns 401 if access token is invalid.
    """
    payload = auth.decode_access_token(token)
    email = payload.get("sub")
    password = payload.get("password")
    if email is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid access token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    # here we have the password set to blank which is erroring
    user = auth.authenticate_user(email=email, password=password)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid access token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user

async def get_current_active_user(current_user: Annotated[User, Depends(get_current_user)]):
    """
    Checks if user is active, allows for turning on and off of individual users.
    """
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user

async def login(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
    """
    Logs in to return an access token when supplied with valid username and pass. 
    Returns 400 if login details incorrect.
    """
    user = auth.authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=400, detail="Incorrect email or password")
    access_token = auth.create_access_token(
        data={"sub": user.email, "password": form_data.password}
    )
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/posts", response_model=None)
async def get_posts_table_as_dataframe(_: User = Depends(get_current_active_user)):
    """
    Gets the posts table as a dataframe
    """
    return df_to_list_of_dict(bq.get_posts_table_as_dataframe(max_posts=5).astype(str).replace('nan', None))

@app.get("/posts/approved/image", response_model=None)
async def get_single_approved_image_post_as_df(_: User = Depends(get_current_active_user)) -> pd.DataFrame:
    """
    Gets a single approved image post as a dataframe from the posts table
    """
    return df_to_list_of_dict(bq.get_single_approved_image_post_as_df().astype(str).replace('nan', None))

@app.get("/posts/unapproved/image", response_model=None)
async def get_unapproved_image_posts_as_df(_: User = Depends(get_current_active_user)) -> pd.DataFrame:
    """
    Gets the unapproved image posts as a dataframe
    """
    return df_to_list_of_dict(bq.get_unapproved_image_posts_as_df().astype(str).replace('nan', None))

@app.post("/posts", response_model=None)
async def update_posts_using_df(posts: List[Post], _: User = Depends(get_current_active_user)):
    """
    Pushes a list of posts to the temp table and then uses it to update the main posts table
    """
    new_posts_df = pd.DataFrame([post.dict() for post in posts])
    bq.update_posts_using_df(new_posts_df)

@app.post("/token")
async def auth_token(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
    """
    Generates an authorization token using the login function and form data.
    """
    return await login(form_data=form_data)