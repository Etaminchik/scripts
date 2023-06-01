#!/bin/bash

logins=($(cat data.csv | awk -F ";" '{ print $1 }'))

for i in ${logins[@]}; do
	fdpi_ctrl del --bind_multi --login $i

done
