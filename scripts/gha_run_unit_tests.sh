#!/bin/bash
##===----------------------------------------------------------------------===##
##
## This source file is part of the Swift Cassandra Client open source project
##
## Copyright (c) 2017-2018 Apple Inc. and the Swift Cassandra Client project authors
## Licensed under Apache License v2.0
##
## See LICENSE.txt for license information
## See CONTRIBUTORS.txt for the list of Swift Cassandra Client project authors
##
## SPDX-License-Identifier: Apache-2.0
##
##===----------------------------------------------------------------------===##

set -e

if [ -z "${I_AM_RUNNING_IN_CI}" ]
then
    echo "Not running in CI"
    exit 1
fi

printf '#!/bin/sh\nexit 0' > /usr/sbin/policy-rc.d
echo "deb [signed-by=/etc/apt/keyrings/apache-cassandra.asc] https://debian.cassandra.apache.org 41x main" | tee -a /etc/apt/sources.list.d/cassandra.sources.list
curl -o /etc/apt/keyrings/apache-cassandra.asc https://downloads.apache.org/cassandra/KEYS
apt-get update
apt install -y -q default-jre
apt-get install -y -q cassandra
swift build --explicit-target-dependency-import-check error
while ! nodetool status 2>/dev/null
do
    echo Waiting for Cassandra..
    sleep 3
done

export CASSANDRA_USER=cassandra
export CASSANDRA_PASSWORD=cassandra
export CASSANDRA_KEYSPACE=cassandra
swift test --explicit-target-dependency-import-check error
