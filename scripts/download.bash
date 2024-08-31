#!/bin/bash

source optparse.bash
optparse.define short=p long=path desc="Directory to store dataset" variable=PATH default="."
source $( optparse.build )

ABS_PATH=$(realpath $PATH)

kaggle datasets download --unzip Cornell-University/arxiv -p $ABS_PATH
