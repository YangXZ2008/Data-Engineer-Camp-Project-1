# Use a base image with Python pre-installed
FROM python:3.8

ARG APIKEY
ARG APISECRET
ARG AUTHORIZATIONHEADER
ARG DB_USERNAME
ARG DB_PASSWORD
ARG DB_SERVER_NAME
ARG DB_DATABASE_NAME
ARG PORT

ENV APIKEY=${APIKEY}
ENV APISECRET=${APISECRET}
ENV AUTHORIZATIONHEADER=${AUTHORIZATIONHEADER}
ENV DB_USERNAME=${DB_USERNAME}
ENV DB_PASSWORD=${DB_PASSWORD}
ENV DB_SERVER_NAME=${DB_SERVER_NAME}
ENV DB_DATABASE_NAME=${DB_DATABASE_NAME}
ENV PORT=${PORT}

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
