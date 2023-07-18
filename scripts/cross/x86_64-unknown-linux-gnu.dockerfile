FROM ghcr.io/cross-rs/x86_64-unknown-linux-gnu:0.2.5-centos

COPY bootstrap-centos.sh .
COPY entrypoint-centos.sh .
COPY install-protoc.sh .
RUN ./bootstrap-centos.sh
RUN ./install-protoc.sh

ENTRYPOINT [ "/entrypoint-centos.sh" ]
