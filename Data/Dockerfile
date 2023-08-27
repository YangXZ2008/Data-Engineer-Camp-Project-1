FROM continuumio/miniconda3

# Set the working directory inside the container
WORKDIR /app

# Copy environment.yml and main_app.py into the container
COPY environment.yml .
COPY main.py .

# Create the environment
RUN conda env create -f environment.yml

# Activate the environment and run the main app
CMD ["/bin/bash", "-c", "source activate etl && python main.py"]

