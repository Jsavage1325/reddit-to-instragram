# Start with the uvicorn-gunicorn-fastapi image, which comes with a lot of requirements pre-installed
FROM tiangolo/uvicorn-gunicorn-fastapi:python3.9

# Set the working directory in the Docker container
WORKDIR /code

# Copy the requirements from local to the container
COPY ./requirements.txt /code/requirements.txt

# Install any requirements which are not included in the base image
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# Copy the code across to the 
COPY ./app /code/app

# Set the PYHTONPATH env var
ENV PYTHONPATH "${PYTHONPATH}:/code/app"

# Create tag on Dockerfile
# docker tag a0c4e287642a gcr.io/erc20-data/reddit-to-instagram-api

# Set the entrypoint for the app by running a uvicorn server
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]