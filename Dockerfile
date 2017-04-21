FROM python:3.5

RUN \
    wget https://github.com/Yelp/dumb-init/releases/download/v1.0.0/dumb-init_1.0.0_amd64.deb && \
    dpkg -i dumb-init_*.deb && \
    rm dumb-init_*.deb

RUN pip install --upgrade pip && \
    pip install --upgrade setuptools

WORKDIR /usr/src/app
COPY requirements.txt /usr/src/app
RUN pip install -r requirements.txt
COPY plugin.py /usr/src/app
COPY setup.py /usr/src/app
COPY skygear_event_tracking /usr/src/app
RUN python setup.py develop

ENV PYTHONUNBUFFERED 0
ENTRYPOINT ["dumb-init"]
