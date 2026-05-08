FROM python:3.12-slim

WORKDIR /app

RUN pip3 install psycopg[binary]
COPY . /app
RUN pip3 install /app

ENV PORT=8080
ENV HOST=0.0.0.0

EXPOSE 8080
CMD ["actionq-server"]
