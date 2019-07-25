FROM python:3
ENV PYTHONPATH "${PYTHONPATH}:/"
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt
COPY app/lib /app/lib
COPY app/workflows/run_status_crawler.py /app/workflows/run_status_crawler.py
RUN mkdir /app/etc
RUN mkdir /nfs

#RUN python3 /app/workflows/run_status_crawler.py -s /app/etc/sources_config.json -g /app/etc/client_secret.json