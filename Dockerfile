FROM golang:1.18 AS Builder

RUN mkdir -p /src
COPY . /src/
WORKDIR /src/

RUN go build -o /src/pikksilm .

FROM debian:bullseye
COPY --from=Builder /src/pikksilm /usr/local/bin/

ENV PIKKSILM_HOME "/var/lib/pikksilm"
ENV PIKKSILM_USER pikksilm
ENV PIKKSILM_USER_ID 999

RUN groupadd -g $PIKKSILM_USER_ID $PIKKSILM_USER && \
  useradd -d $PIKKSILM_HOME --uid $PIKKSILM_USER_ID --gid $PIKKSILM_USER_ID -ms /bin/false $PIKKSILM_USER && \
  mkdir -p $PIKKSILM_HOME && chown $PIKKSILM_USER $PIKKSILM_HOME

USER $PIKKSILM_USER

VOLUME $PIKKSILM_HOME
ENTRYPOINT [ "/usr/local/bin/pikksilm", "run" ]
