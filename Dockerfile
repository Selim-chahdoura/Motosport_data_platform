FROM apache/spark:4.1.1

USER root

RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install pandas pyarrow requests

WORKDIR /app

COPY . /app

CMD ["/opt/spark/bin/spark-submit", "/app/src/pipelines/gold_driver_stats.py"]