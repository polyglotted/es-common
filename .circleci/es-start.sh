#!/usr/bin/env bash

export ES_JAVA_OPTS="-Xms1600m -Xmx1600m"

if [[ ! -e elasticsearch-${es_version} ]]; then
    wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-${es_version}.tar.gz \
        && tar -xvf elasticsearch-${es_version}.tar.gz
fi

mkdir -p /tmp/es-common
mkdir -p ${artifact_dir}/eslogs
nohup elasticsearch-${es_version}/bin/elasticsearch -Ecluster.name=test-rest -Epath.data=/tmp/es-common \
    -Epath.logs=${artifact_dir}/eslogs -Ehttp.host=0.0.0.0 -Etransport.host=127.0.0.1 > ${artifact_dir}/es.txt 2>&1 & echo $! > es-pid.txt
sleep 10 && wget --waitretry=5 --retry-connrefused  -t 10 -v http://localhost:9200/