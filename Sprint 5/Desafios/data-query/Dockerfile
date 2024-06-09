FROM python:3.12.3-slim-bullseye

WORKDIR /app

RUN pip install boto3

COPY ./data_query.py ./script.py
COPY ./s3select_query.sql ./query.sql

CMD [ "python", "script.py"]