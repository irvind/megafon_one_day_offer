FROM python:3.9-slim

WORKDIR /opt

ADD requirements.txt requirements.txt 
RUN pip install -r requirements.txt

ADD main.py main.py

CMD ["python", "main.py"]
