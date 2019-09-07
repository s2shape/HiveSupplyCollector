FROM nagasuga/docker-hive
MAINTAINER burilovmv@gmail.com

ADD data.sql /
ADD data_maps.txt /

EXPOSE 10000/tcp
EXPOSE 10002/tcp

RUN cd /usr/local/hive && ./bin/hive < /data.sql
