#!/bin/bash

export AMBARI_ADMIN_USER_NAME=admin
export AMBARI_ADMIN_PASSWORD=admin
export ATLAS_USER_NAME=admin
export ATLAS_PASSWORD=admin
export DPS_ADMIN_USER_NAME=admin
export DPS_ADMIN_PASSWORD=admin
export DPS_HOST=$(hostname -f)

java -jar target/DLMAtlasMetadataReplicator.jar