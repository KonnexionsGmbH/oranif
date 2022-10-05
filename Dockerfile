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

RUN alien -i --scripts *.rpm \
    && rm -rf *.x86_64.rpm \
    && sqlplus -V \
    && apt-get -y autoclean \
    && apt-get -y autoremove \
    && rm -rf /tmp/*

FROM erlang:24.3.4-slim

COPY --from=base / /

COPY . /

RUN rebar3 compile

# RUN sqlplus scott/regit@oranif_db:1521/XE