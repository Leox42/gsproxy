#!/bin/bash

arg1=$1
arg2=$2

echo "File to sign : $arg2"

sign_result=$(gpg --pinentry-mode=loopback --passphrase "$arg1" --sign "$arg2")

echo $sign_result
