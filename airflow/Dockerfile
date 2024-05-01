FROM python:3.11

RUN apt install -y wget apt-transport-https gpg
RUN wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor | tee /etc/apt/trusted.gpg.d/adoptium.gpg > /dev/null
RUN echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list
RUN apt update
RUN apt install -y temurin-17-jdk
COPY constraints.txt constraints.txt
RUN pip install  apache-airflow==2.9.0 --constraint constraints.txt
RUN pip install apache-airflow-providers-postgres
RUN pip install apache-airflow-providers-apache-spark
RUN pip install minio
RUN pip install matplotlib

WORKDIR /root/airflow
COPY ./dags ./dags
COPY sp_script.py sp_script.py
EXPOSE 8080

CMD airflow standalone