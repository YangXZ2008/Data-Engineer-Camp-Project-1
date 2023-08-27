FROM continuumio/miniconda3

WORKDIR /app

COPY environment.yml environment.yml
COPY app/requirements.txt requirements.txt
RUN conda env create -f environment.yml && \
    conda run -n my_env pip install -r requirements.txt

COPY . .

SHELL ["conda", "run", "-n", "my_env", "/bin/bash", "-c"]

CMD ["python", "main_app.py"]
