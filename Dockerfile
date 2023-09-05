FROM python:3.9 

WORKDIR /src 

COPY /src .

COPY requirements.txt .

RUN pip install -r requirements.txt 

CMD ["python", "-m", "etl_project.pipelines.main"]