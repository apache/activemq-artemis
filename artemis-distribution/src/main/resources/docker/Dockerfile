# ActiveMQ Artemis

##########################################################
## Build Image                                           #
##########################################################
FROM openjdk:8u171-jdk-stretch as builder
LABEL maintainer="Apache ActiveMQ Team"

ENV JMX_EXPORTER_VERSION=0.3.1
ENV JGROUPS_KUBERNETES_VERSION=0.9.3

# See https://github.com/hadolint/hadolint/wiki/DL4006
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get -qq -o=Dpkg::Use-Pty=0 update && \
  apt-get -qq -o=Dpkg::Use-Pty=0 install -y --no-install-recommends \
    libaio1=0.3.110-3 \
    xmlstarlet=1.6.1-2 \
    jq=1.5+dfsg-1.3 \
    ca-certificates=20161130+nmu1+deb9u1 \
    wget=1.18-5+deb9u2

# I like to be able to verify files within a docker container
RUN apt-get install -y vim
RUN apt-get install -y screen
#RUN rm -rf /var/lib/apt/lists/*

# Make sure pipes are considered to detemine success, see: https://github.com/hadolint/hadolint/wiki/DL4006
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Uncompress and validate
WORKDIR /opt

##########################################################
## Run Image                                             #
##########################################################
FROM openjdk:8
LABEL maintainer="Apache ActiveMQ Team"

ENV ARTEMIS_USER artemis
ENV ARTEMIS_PASSWORD artemis
ENV ANONYMOUS_LOGIN false
ENV CREATE_ARGUMENTS --user ${ARTEMIS_USER} --password ${ARTEMIS_PASSWORD} --silent --http-host 0.0.0.0 --relax-jolokia

# add user and group for artemis
RUN groupadd -g 1000 -r artemis && useradd -r -u 1000 -g artemis artemis

RUN apt-get -qq -o=Dpkg::Use-Pty=0 update && \
  apt-get -qq -o=Dpkg::Use-Pty=0 install -y --no-install-recommends \
    libaio1=0.3.110-3 \
    xmlstarlet=1.6.1-2 \
    jq=1.5+dfsg-1.3 \
    gettext-base=0.19.8.1-2 \
    dumb-init=1.2.0-1

RUN apt-get install -y vim
RUN apt-get install -y screen

RUN rm -rf /var/lib/apt/lists/*

USER artemis

COPY "/opt/activemq-artemis/" "/opt/activemq-artemis"

# Web Server
EXPOSE 8161

# JMX Exporter
EXPOSE 9404

# Port for CORE,MQTT,AMQP,HORNETQ,STOMP,OPENWIRE
EXPOSE 61616

# Port for HORNETQ,STOMP
EXPOSE 5445

# Port for AMQP
EXPOSE 5672

# Port for MQTT
EXPOSE 1883

#Port for STOMP
EXPOSE 61613

USER root

RUN mkdir /var/lib/artemis-instance
RUN chown -R artemis.artemis /var/lib/artemis-instance
COPY assets/docker-entrypoint.sh /

USER artemis


# Expose some outstanding folders
VOLUME ["/var/lib/artemis-instance"]
WORKDIR /var/lib/artemis-instance


ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["artemis-server"]
