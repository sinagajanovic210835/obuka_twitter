ARG DRUID_VER=0.22.1

FROM apache/druid:${DRUID_VER} AS druid

FROM ubuntu:bionic
RUN apt-get update && \
    apt-get install --yes openjdk-8-jre-headless perl-modules && \
    apt-get clean

RUN addgroup --system -gid 1000 druid \
 && adduser --system --uid 1000 --disabled-password --home /opt/druid --shell /bin/bash --group druid

COPY --from=druid --chown=druid:druid /opt/druid /opt/druid

WORKDIR /opt/druid

USER druid

EXPOSE 8888/tcp
EXPOSE 8081/tcp

#CMD ["bash", "-c", "/opt/druid/bin/start-nano-quickstart"]
CMD ["bash", "-c", "/opt/druid/bin/start-micro-quickstart"]

