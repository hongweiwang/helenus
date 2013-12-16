#!/bin/bash
. include.sh
for ip in `./list_clients.sh`
do
	$SSH $ip "$@" &
done
wait
