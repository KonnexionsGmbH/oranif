FROM erlang:24.3.4-slim as base

RUN apt-get -y update \
    && apt-get -y install \
        build-essential \
        git \
        lcov \
        wget \
        alien \
        libaio-dev

RUN wget --quiet --no-check-certificate -nv https://download.oracle.com/otn_software/linux/instantclient/217000/oracle-instantclient-basic-21.7.0.0.0-1.x86_64.rpm
RUN wget --quiet --no-check-certificate -nv https://download.oracle.com/otn_software/linux/instantclient/217000/oracle-instantclient-sqlplus-21.7.0.0.0-1.x86_64.rpm
RUN wget --quiet --no-check-certificate -nv https://download.oracle.com/otn-pub/otn_software/db-express/oracle-database-xe-21c-1.0-1.ol8.x86_64.rpm

RUN alien -i --scripts *.rpm \
    && rm -rf *.x86_64.rpm \
    && sqlplus -V \
    && apt-get -y autoclean \
    && apt-get -y autoremove \
    && rm -rf /tmp/*

FROM erlang:24.3.4-slim

COPY --from=base / /

RUN echo "LISTENER_PORT=1521" > /etc/sysconfig/oracle-xe-21c.conf \
    && echo "EM_EXPRESS_PORT=5500" >> /etc/sysconfig/oracle-xe-21c.conf \
    && echo "CHARSET=AL32UTF8" >> /etc/sysconfig/oracle-xe-21c.conf \
    && echo "DBFILE_DEST=" >> /etc/sysconfig/oracle-xe-21c.conf \
    && echo "DB_DOMAIN=" >> /etc/sysconfig/oracle-xe-21c.conf \
    && echo "SKIP_VALIDATIONS=false" >> /etc/sysconfig/oracle-xe-21c.conf \
    && echo "ORACLE_PASSWORD=Abcd1234" >> /etc/sysconfig/oracle-xe-21c.conf \
    && /etc/init.d/oracle-xe-21c configure

COPY . /

RUN rebar3 compile