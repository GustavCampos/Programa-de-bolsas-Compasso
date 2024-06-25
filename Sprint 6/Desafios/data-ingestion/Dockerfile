FROM python:3.12.3-alpine3.19

WORKDIR /app

# Updating pip
RUN python -m pip install --upgrade pip

# Installing boto3
RUN pip install boto3

# Copyng credentials file
COPY script.py /app/

CMD [ "python", "script.py" ]