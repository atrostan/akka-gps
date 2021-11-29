#!/bin/bash

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

helpFunction()
{
  echo ""
  echo "Usage: $0 -c compressParam -p partitionParam -a aggregateParam -deployMode deployParam"
  echo -e "\t-c 1 or 0. Whether to compress or not"
  echo -e "\t-p 1 or 0. Whether to partition or not"
  echo -e "\t-a 1 or 0. Whether to aggregate or not"
  echo -e "\t-d 1 or 0. 1: Deploy on cluster, 0: Deploy locally"
  exit 1 # Exit script after printing help
}

while getopts "c:p:a:d:" opt
do
  case "$opt" in
      c ) compressParam="$OPTARG" ;;
      p ) partitionParam="$OPTARG" ;;
      a ) aggregateParam="$OPTARG" ;;
      d ) deployParam="$OPTARG" ;;
      ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
  esac
done

# Print helpFunction in case parameters are empty
if [ -z "$compressParam" ] || [ -z "$partitionParam" ] || [ -z "$aggregateParam" ] || [ -z "$deployParam" ]
then
  echo "Some or all of the parameters are empty";
  helpFunction
fi

# set your home directories containing akka-gps and spark-submit
akka_gps_home="/home/atrostan/Workspace/akka-gps"
spark_home="/home/atrostan/Workspace/repos/spark/spark-3.1.2-bin-hadoop3.2"

akka_gps_jar="${akka_gps_home}/target/scala-2.12/akka-gps.jar"
spark_submit="${spark_home}/bin/spark-submit"

if [ $deployParam -eq 1 ]; then 
  # set master url on ec2 cluster
  master_url="spark://ec2-107-21-87-63.compute-1.amazonaws.com:7077"
  # "local[*] = all cores on local machine, spark://"
  master="${master_url}"
  # "client" = local, "cluster" = on worker nodes
  deploy_mode="cluster" 
else 
  # "local[*] = all cores on local machine, spark://"
  master="local[*]"
  # "client" = local, "cluster" = on worker nodes
  deploy_mode="client" 
fi

preprocessingPackage="com.preprocessing"
compressorClass="${preprocessingPackage}.edgeList.Driver"
partitionerClass="${preprocessingPackage}.partitioning.Driver"
aggregatorClass="${preprocessingPackage}.aggregation.Driver"

# directory that stores the graph
graphName="symmRmat"
graphDir="${akka_gps_home}/src/main/resources/graphs/${graphName}"

# original, uncompressed edgelist
origGraphPath="\"${graphDir}/orig.net\"" 

# directory that will store the compressed edgelist
compressedDirName="compressed"
outputFilename="\"${graphDir}/${compressedDirName}\"" 
compressedGraphPath="\"${outputFilename}.parquet\""

sep="\" \""

# whether the original edge list stores weights or not (true or false)
isWeighted="\"false\""

graphYaml="${graphDir}/stats.yml"
outputPartitionsPath="\"${graphDir}/partitions\""
threshold=7
numPartitions=4

partitioners=(
#  1 # 1d
#  2 # 2d
  3 # Hybrid
)

partitionBys=(
  "\"true\"" # partition by source
#  "\"false\"" # partition by destination
)

# Compress a graph
compressSubmit="${spark_submit} \
--class ${compressorClass} \
--master ${master} \
--deploy-mode ${deploy_mode} \
${akka_gps_jar} \
--inputFilename ${origGraphPath} --outputFilename ${outputFilename} --sep ${sep} --isWeighted ${isWeighted}"

echo ${compressSubmit}
if [ $compressParam -eq 1 ]
then 
  eval ${compressSubmit}
fi

# read nNodes, nEdges from stats.yml
eval $(parse_yaml $graphYaml)

# Then, partition the graph using all partition algorithms
if [ $partitionParam -eq 1 ]
then 
  # programmatically partition an input graph
  for partitioner in "${partitioners[@]}"; do
    for partitionBySource in "${partitionBys[@]}"; do
      printf "\n"
      logStr="Partitioning ${compressedGraphPath} with ${Nodes} Nodes and ${Edges} Edges.\nPartitioner: ${partitioner}, Partitioning By Source: ${partitionBySource}, Number of Partitions: ${numPartitions}, Threshold: ${threshold}, isWeighted: ${isWeighted}"
      echo -e ${logStr}
      printf "\n"

      javaJarStr="java -cp ${partitionDriverJarPath} --nNodes ${Nodes} --nEdges ${Edges} --inputFilename ${compressedGraphPath} --outputDirectoryName ${outputPartitionsPath} --sep ${sep} --partitioner ${partitioner}   --threshold ${threshold} --numPartitions ${numPartitions} --partitionBySource ${partitionBySource} --isWeighted ${isWeighted}"
      
      partitionSubmit="${spark_submit} \
        --class ${partitionerClass} \
        --master ${master} \
        --deploy-mode ${deploy_mode} \
        ${akka_gps_jar} \
        --nNodes ${Nodes} --nEdges ${Edges} --inputFilename ${compressedGraphPath} --outputDirectoryName ${outputPartitionsPath} --sep ${sep} --partitioner ${partitioner}   --threshold ${threshold} --numPartitions ${numPartitions} --partitionBySource ${partitionBySource} --isWeighted ${isWeighted}"
      echo ${partitionSubmit}
      eval ${partitionSubmit}
    done
  done
fi

partitionerFolder="hybrid"
modeFolder="bySrc"

partitionFolder="\"${outputPartitionsPath}/${partitionerFolder}/${modeFolder}/\""
# path to yaml file that contains address of workers in cluster
workerPaths="\"${akka_gps_home}/src/main/resources/paths.yaml\""
aggregateJavaJarStr="java -cp ${aggregatorDriverJarPath} --partitionFolder ${partitionFolder} --numPartitions ${numPartitions} --sep ${sep} --workerPaths ${workerPaths}"

aggregateSubmit="${spark_submit} \
  --class ${aggregatorClass} \
  --master ${master} \
  --deploy-mode ${deploy_mode} \
  ${akka_gps_jar} \
  --partitionFolder ${partitionFolder} --numPartitions ${numPartitions} --sep ${sep} --workerPaths ${workerPaths}"

if [ $aggregateParam -eq 1 ]
then
  echo ${aggregateSubmit}
  eval ${aggregateSubmit}
fi