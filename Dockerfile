FROM python:3.9.15-bullseye

WORKDIR /app
RUN mkdir -p "Dataset/ETF" "Dataset/pages"

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY . .

