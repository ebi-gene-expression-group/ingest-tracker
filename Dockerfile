FROM python:3
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt
COPY app/ /home/app/
RUN mkdir /appdata
RUN mkdir /appdata/sources_config
RUN mkdir /appdata/client_secret
#CMD python run_status_crawler.py -s /appdata/sources_config/sources_config.json -g /appdata/client_secret/client_secret.json