# Use a base image with Python pre-installed
FROM python:3.8

# Set the working directory inside the container
WORKDIR /src

# Copy the requirements.txt file into the container
COPY requirements.txt .

# Install Python packages using pip
RUN pip install -r requirements.txt

# Copy the rest of your application code into the container
COPY . .

# Define the command to run your application and keep the container running
CMD ["bash", "-c", "python -m pipelines.fuel_pipeline && tail -f /dev/null"]
