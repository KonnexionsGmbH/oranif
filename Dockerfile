FROM erlang:24.3.4-slim as base

SHELL ["/bin/bash", "-c"]

ARG VERSION_ORACLE_INSTANT_CLIENT_1=21
ARG VERSION_ORACLE_INSTANT_CLIENT_2=7

ENV ORACLE_HOME=/usr/lib/oracle/${VERSION_ORACLE_INSTANT_CLIENT_1}/client64
ENV LD_LIBRARY_PATH="${ORACLE_HOME}/lib:${LD_LIBRARY_PATH}" \
    PATH="${ORACLE_HOME}:${PATH}"
    
RUN apt-get -y update \
    && apt-get -y install \
        build-essential \
        alien \
        git \
        lcov \
        libaio-dev \
        libaio1 \
        wget \
    && eval echo 'export PATH=${PATH}' >> ~/.bashrc \
    && /bin/bash -c "source ~/.bashrc"

RUN wget --quiet --no-check-certificate -nv https://download.oracle.com/otn_software/linux/instantclient/217000/oracle-instantclient-basic-${VERSION_ORACLE_INSTANT_CLIENT_1}.${VERSION_ORACLE_INSTANT_CLIENT_2}.0.0.0-1.x86_64.rpm
RUN wget --quiet --no-check-certificate -nv https://download.oracle.com/otn_software/linux/instantclient/217000/oracle-instantclient-sqlplus-${VERSION_ORACLE_INSTANT_CLIENT_1}.${VERSION_ORACLE_INSTANT_CLIENT_2}.0.0.0-1.x86_64.rpm

RUN alien -i --scripts *.rpm \
    && rm -rf *.x86_64.rpm \
    && sqlplus -V \
    && apt-get -y autoclean \
    && apt-get -y autoremove \
    && rm -rf /tmp/*

FROM erlang:24.3.4-slim

COPY --from=base / /

COPY . /

SHELL ["/bin/bash", "-c"]

RUN rebar3 compile

CMD ["/bin/bash"]
