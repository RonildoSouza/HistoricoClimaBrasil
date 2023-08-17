# docker build . -f Dockerfile --pull --tag puc-airflow:2.6.3

FROM apache/airflow:2.6.3
USER root

RUN echo "deb http://deb.debian.org/debian/ sid main" >> /etc/apt/sources.list
RUN apt-get update && apt-get install -y openjdk-8-jdk

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
RUN export JAVA_HOME

USER airflow
RUN pip install --upgrade pip

COPY ./requirements.txt .
RUN pip install -r ./requirements.txt