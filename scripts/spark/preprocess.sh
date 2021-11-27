#!/bin/bash

helpFunction()
{
  echo ""
  echo "Usage: $0 -c compressParam -p partitionParam"
  echo -e "\t-c 1 or 0. Whether to compress or not"
  echo -e "\t-p 1 or 0. Whether to partition or not"
  exit 1 # Exit script after printing help
}

function parse_yaml 
{
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

while getopts "c:p:" opt
do
  case "$opt" in
      c ) compressParam="$OPTARG" ;;
      p ) partitionParam="$OPTARG" ;;
      ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
  esac
done

# Print helpFunction in case parameters are empty
if [ -z "$compressParam" ] || [ -z "$partitionParam" ] 
then
  echo "Some or all of the parameters are empty";
  helpFunction
fi

# Compress a graph
# Then, partition the graph using all partition algorithms
akka_gps_home="/home/atrostan/Workspace/repos/akka-gps"

partitionDriverJarPath="${akka_gps_home}/out/artifacts/akka_gps_partitioner_jar/akka-gps.jar"
compressorDriverJarPath="${akka_gps_home}/out/artifacts/akka_gps_compressor_jar/akka-gps.jar"

# directory that stores the graph

graphName="8rmat"
graphDir="${akka_gps_home}/src/main/resources/graphs/${graphName}"

# original, uncompressed edgelist
origGraphPath="\"${graphDir}/orig.net\"" 

# directory that will store the compressed edgelist
compressedDirName="compressed"
outputFilename="\"${graphDir}/${compressedDirName}\"" 
compressedGraphPath="\"${graphDir}/${compressedDirName}/part-00000\""

sep="\" \""

# whether the original edge list stores weights or not (true or false)
isWeighted="\"false\""

graphYaml="${graphDir}/stats.yml"
outputPartitionsPath="\"${graphDir}/partitions\""
threshold=7
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

# compress the graph
compressJavaJarStr="java -jar ${compressorDriverJarPath} --inputFilename ${origGraphPath} --outputFilename ${outputFilename} --sep ${sep} --isWeighted ${isWeighted}"
# echo ${compressJavaJarStr}

if [ $compressParam -eq 1 ]
then 
  eval ${compressJavaJarStr}
fi
# read nNodes, nEdges from stats.yml
eval $(parse_yaml $graphYaml)

if [ $partitionParam -eq 1 ]
then 
  # programmatically partition an input graph
  for partitioner in "${partitioners[@]}"; do
    for partitionBySource in "${partitionBys[@]}"; do
      printf "\n"
      logStr="Partitioning ${compressedGraphPath} with ${Nodes} Nodes and ${Edges} Edges.\nPartitioner: ${partitioner}, Partitioning By Source: ${partitionBySource}, Number of Partitions: ${numPartitions}, Threshold: ${threshold}, isWeighted: ${isWeighted}"
      echo -e ${logStr}
      printf "\n"

      javaJarStr="java -jar ${partitionDriverJarPath} --nNodes ${Nodes} --nEdges ${Edges} --inputFilename ${compressedGraphPath} --outputDirectoryName ${outputPartitionsPath} --sep ${sep} --partitioner ${partitioner}   --threshold ${threshold} --numPartitions ${numPartitions} --partitionBySource ${partitionBySource} --isWeighted ${isWeighted}" 
      # echo ${javaJarStr}
      eval ${javaJarStr}
    done
  done
fi