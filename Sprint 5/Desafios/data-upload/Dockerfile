FROM python:3.12.3-slim-bullseye

# USER root

# # Add user
# ARG USER_NAME=python_user
# RUN useradd -m ${USER_NAME}

# # Add AWS credentials
# COPY ./credentials /home/${USER_NAME}/.aws/

# WORKDIR /home/${USER_NAME}/app

# USER ${USER_NAME}

WORKDIR /app

RUN pip install boto3

#COPY ./data_upload.py ./script.py

CMD [ "python", "script.py" ]