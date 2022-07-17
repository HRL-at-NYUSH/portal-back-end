from python:3.8
RUN apt-get update && apt-get install -y curl vim unzip
WORKDIR /app
COPY . .
RUN pip3 install -r requirements.txt
WORKDIR /app/src
CMD ["sanic", "server.app", "--host=0.0.0.0",  "--port=8000"]
EXPOSE 8000