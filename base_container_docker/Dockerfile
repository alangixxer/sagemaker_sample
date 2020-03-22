FROM ubuntu:16.04

RUN apt-get -y update && apt-get install -y --no-install-recommends \
         wget \
         python \
         nginx \
         ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN wget https://bootstrap.pypa.io/get-pip.py && python get-pip.py && \
    pip install numpy==1.16.2 scipy==1.2.1 pandas flask gevent gunicorn && \
        (cd /usr/local/lib/python2.7/dist-packages/scipy/.libs; rm *; ln ../../numpy/.libs/* .) && \
        rm -rf /root/.cache