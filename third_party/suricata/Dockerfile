FROM ubuntu:focal
LABEL maintainer="Markus Kont <alias013@gmail.com>"

ENV APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=1

RUN apt-get update \
  && apt-get -y install \
  libpcre3 \
  libpcre3-dbg \
  libyaml-0-2 \
  zlib1g \
  libcap-ng0 \
  libhyperscan5 \
  libhiredis0.14 \
  libluajit-5.1-2 \
  libmagic1 \
  libpcap0.8 \
  libnet1 \
  libjansson4 \
  libnss3 \
  python3 \
  && apt-get -y autoremove && apt-get -y autoclean && apt-get clean

RUN apt-get update \
  && apt-get install -y software-properties-common gpg \
  && add-apt-repository ppa:oisf/suricata-stable \
  && apt-get update \
  && apt-get install -y suricata \
  && apt-get remove --purge -y software-properties-common gpg \
  && apt-get -y autoremove && apt-get -y autoclean && apt-get clean

ENTRYPOINT ["suricata"]
