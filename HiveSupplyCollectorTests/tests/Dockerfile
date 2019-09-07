FROM nagasuga/docker-hive
MAINTAINER burilovmv@gmail.com

ADD data.sql /
ADD data_maps.txt /

RUN cd /usr/local/hive && ./bin/hive < /data.sql
