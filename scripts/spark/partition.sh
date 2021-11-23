#!/bin/bash

function parse_yaml {
   local prefix=$2
   local s='[[:space:]]*' w='[a-zA-Z0-9_]*' fs=$(echo @|tr @ '\034')
   sed -ne "s|^\($s\):|\1|" \
        -e "s|^\($s\)\($w\)$s:$s[\"']\(.*\)[\"']$s\$|\1$fs\2$fs\3|p" \
        -e "s|^\($s\)\($w\)$s:$s\(.*\)$s\$|\1$fs\2$fs\3|p"  $1 |
   awk -F$fs '{
      indent = length($1)/2;
      vname[indent] = $2;
      for (i in vname) {if (i > indent) {delete vname[i]}}
      if (length($3) > 0) {
         vn=""; for (i=0; i<indent; i++) {vn=(vn)(vname[i])("_")}
         printf("%s%s%s=\"%s\"\n", "'$prefix'",vn, $2, $3);
      }
   }'
}

akka_gps_home="/home/atrostan/Workspace/repos/akka-gps"
partitionDriverJarPath="${akka_gps_home}/out/artifacts/akka_gps_jar/akka-gps.jar"
graphDir="${akka_gps_home}/src/main/resources/graphs/email-Eu-core"
graphPath="\"${graphDir}/orig.net\""
graphYaml="${graphDir}/stats.yml"
outputPartitionsPath="\"${graphDir}/partitions\""
sep="\" \""
threshold=100
numPartitions=4
partitioners=(
  1 # 1d
  2 # 2d
  3 # Hybrid
)
partitionBys=(
  "\"true\"" # partition by source
  "\"false\"" # partition by destination
)
eval $(parse_yaml $graphYaml)

# programmatically partition an input graph
for partitioner in "${partitioners[@]}"; do
  for partitionBySource in "${partitionBys[@]}"; do
    javaJarStr="java -jar ${partitionDriverJarPath} --nNodes ${Nodes} --nEdges ${Edges} --inputFilename ${graphPath} --outputDirectoryName ${outputPartitionsPath} --sep ${sep} --partitioner ${partitioner}   --threshold ${threshold} --numPartitions ${numPartitions} --partitionBySource ${partitionBySource}"
    echo ${javaJarStr}
#    eval ${javaJarStr}
  done
done
