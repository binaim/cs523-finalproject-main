FROM openjdk:8

ENV HBASE_VERSION 2.4.17
ENV HBASE_HOME=/hbase-${HBASE_VERSION}

RUN curl -O https://dlcdn.apache.org/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz
RUN tar -zvxf hbase-${HBASE_VERSION}-bin.tar.gz
RUN rm hbase-${HBASE_VERSION}-bin.tar.gz


ENV PATH $HBASE_HOME/bin:$PATH

# update hbase configuration
COPY config/* /config/
COPY config/hbase-site.xml ${HBASE_HOME}/conf
#COPY config/zoo.cfg ${HBASE_HOME}/conf
COPY config/hbase-env.sh ${HBASE_HOME}/conf


EXPOSE 2181 8080 8085 9090 9095 16010 16030

COPY script/* /script/
CMD /bin/bash /script/start-pseudo.sh
#CMD ["hbase", "master", "start"]
