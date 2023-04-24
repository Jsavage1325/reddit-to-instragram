from passlib.context import CryptContext
from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
from fastapi import HTTPException, status
# Local imports
from models import User

class Auth():
    """
    Class to handle FastAPI server authentication using JWT authentication
    """
    def __init__(self):
        # Set secret key, algorithm, expiry time and api key header
        self.SECRET_KEY = "my-secret-key"  # Change this to your own secret key
        self.ALGORITHM = "HS256"
        self.ACCESS_TOKEN_EXPIRE_MINUTES = 30
        self.API_KEY_HEADER = "X-API-Key"  # Change this to the name of your API key header

        # Create a hashing context to securely hash and check passwords
        self.pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

        # Later we will set this up to pull from a database, but for now we use this
        self.FAKE_USERS_DB = {
            "example@example.com": {
            "username": "john",
            "email": "example@example.com",
            "hashed_password": self.hash_password("secret"),
            "api_key": "some-api-key",
            "disabled": False,
            }
        }

    # Define a function to hash a password
    def hash_password(self, password: str):
        """
        Returns a hashed password using the pwd_context
        """
        return self.pwd_context.hash(password)

    # Define a function to check a password hash
    def verify_password(self, password: str, hashed_password: str):
        """
        Checks a password hash against the password
        """
        return self.pwd_context.verify(password, hashed_password)

    # Define a function to create a new JWT access token
    def create_access_token(self, data: dict, expires_delta: Optional[timedelta] = None):
        """
        Creates an access token with expiry, allows for custom expiry time, otherwise default expiry time
        """
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=self.ACCESS_TOKEN_EXPIRE_MINUTES)
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, self.SECRET_KEY, algorithm=self.ALGORITHM)
        return encoded_jwt

    # Define a function to decode and verify a JWT access token
    def decode_access_token(self, token: str):
        """
        Decodes the access token using the secret key, returns a HTTPException if the token is invalid
        """
        try:
            payload = jwt.decode(token, self.SECRET_KEY, algorithms=[self.ALGORITHM])
            return payload
        except JWTError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid access token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    def authenticate_user(self, email: str, password: str):
        """
        Checks the user exists then authenticates using password.
        Returns User if authenticated successfully, otherwise returns None.
        """
        if email not in self.FAKE_USERS_DB:
            return None
        user_dict = self.FAKE_USERS_DB[email]
        if not self.verify_password(password, user_dict["hashed_password"]):
            return None
        user = User(**user_dict)
        return user



