FROM python:3.12.3-slim-bullseye

WORKDIR /app

RUN pip install pandas

COPY ./data_processing.py /app/script.py

CMD [ "python", "script.py" ]
