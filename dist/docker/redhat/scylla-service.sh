#!/bin/bash

. /usr/lib/scylla/scylla_prepare

export SCYLLA_HOME SCYLLA_CONF

exec /usr/bin/scylla --options-file /etc/scylla/scylla.docker.yaml $SCYLLA_ARGS $SEASTAR_IO $DEV_MODE $CPUSET $SCYLLA_DOCKER_ARGS
