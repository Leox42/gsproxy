#!/bin/bash

arg1=$1

echo "File to verify : $arg1"

verif_result=$(gpg --verify "$arg1")

echo $verif_result
