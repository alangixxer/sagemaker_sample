FROM 111016121260.dkr.ecr.us-east-2.amazonaws.com/ubuntu-16.04-base-with-pip

ENV PYTHONUNBUFFERED=TRUE
ENV PYTHONDONTWRITEBYTECODE=TRUE
ENV PATH="/opt/program:${PATH}"

COPY code /opt/program
WORKDIR /opt/program
