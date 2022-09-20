FROM ubuntu:20.04

RUN apt-get update && apt-get install -y \
    curl

RUN curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh

ENV CLUSTER_VERSION=v6.1.0

RUN /root/.tiup/bin/tiup install \
playground \
pd:$CLUSTER_VERSION \
tidb:$CLUSTER_VERSION \
tikv:$CLUSTER_VERSION

ENTRYPOINT /root/.tiup/bin/tiup playground $CLUSTER_VERSION --tiflash 0 --without-monitor
