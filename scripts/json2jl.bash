#!/bin/bash

source optparse.bash

optparse.define short=i long=input desc="Input JSON file" variable=inputPath
optparse.define short=o long=output desc="Output JSON Lines file" variable=outputPath

source $( optparse.build )

if [[ -z $inputPath ]]; then
    echo "No input provided."
    exit 1
fi

if [[ -z $outputPath ]]; then
    echo "No output provided."
    exit 1
fi

absInputPath=$(realpath $inputPath)
absOutputPath=$(realpath $outputPath)

jq -c . $absInputPath > $absOutputPath
