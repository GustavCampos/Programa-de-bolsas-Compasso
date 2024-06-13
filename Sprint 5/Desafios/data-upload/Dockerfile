FROM python:3.12.3-slim-bullseye

WORKDIR /app

RUN pip install boto3

COPY ./data_upload.py ./script.py

CMD [ "python", "script.py" ]
