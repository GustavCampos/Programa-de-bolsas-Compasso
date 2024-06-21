FROM python:3.12.3-alpine3.19

WORKDIR /app

# Copyng requirements file
COPY requirements.txt /app/
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copyng credentials file
COPY script.py /app/

CMD [ "python", "script.py" ]