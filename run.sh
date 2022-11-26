#!/bin/bash
input=./tests/data/input/
output=./output/
filter=True

if [ ! -z "$1" ]; then input=$1;  fi
if [ ! -z "$2" ]; then output=$2; fi
if [ ! -z "$3" ]; then filter=$3; fi

echo "Resolved args; input: $input; output: $output, filter: $filter"

echo "Cleaning output dir: $output"
rm -rf $output

echo "Spark app is starts"
spark-submit \
   --master local \
   --deploy-mode client \
   --py-files src/schemas.py,src/transforms.py \
   src/main.py --input_path $input --output_path $output --partition_filter $filter
echo "Spark app finished, output saved to: $output"

