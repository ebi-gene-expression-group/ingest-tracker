FROM python:3
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt
COPY app/lib /home/app/lib
COPY app/workflows/run_status_crawler.py /home/app/workflows/run_status_crawler.py
RUN mkdir /home/app/etc

RUN echo Starting crawl...
RUN python3 home/app/workflows/run_status_crawler.py -s /home/app/etc/sources_config.json -g /home/app/etc/client_secret.json
RUN echo Finished crawling...