# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster

WORKDIR /client

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

EXPOSE 5000
# EXPOSE 4000
EXPOSE 5555

ENV FLASK_APP=client.py

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]