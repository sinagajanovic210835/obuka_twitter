FROM python:3.7-slim

WORKDIR /usr/src/app

COPY . .
RUN mkdir files
RUN pip install --no-cache-dir -r requirements.txt

ENV KAFKA_BROKER="kafka1:19092"
ENV BEARER_TOKEN="AAAAAAAAAAAAAAAAAAAAADMQeAEAAAAAqg8li4EedH9xlwQODgtq8BMppjY%3DaWvQeHVBe6PKH8JMWGcMHmYebyfJNc6ZdWeuXYtcw576JksAGi"

CMD ["python", "-u", "./main.py"]


