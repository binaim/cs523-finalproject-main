# Use an official Python runtime as a parent image
FROM python:3.9

# Set the working directory to /app
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Expose the port that the application will listen on
EXPOSE 8050

# RUN apt-get update && apt-get install -y iputils-ping

# Start the application
#CMD gunicorn -b 0.0.0.0:8050 main:server --timeout 200 --workers 3
CMD waitress-serve --host 0.0.0.0 --port 8050 main:server
#CMD tail -F /dev/null
