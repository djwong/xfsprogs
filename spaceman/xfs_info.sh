#!/bin/sh -f
#
# Copyright (c) 2000-2001 Silicon Graphics, Inc.  All Rights Reserved.
#

OPTS=""
USAGE="Usage: xfs_info [-V] [-t mtab] [mountpoint|device|file]"

while getopts "t:V" c
do
	case $c in
	t)	OPTS="-t $OPTARG" ;;
	V)	xfs_spaceman -p xfs_info -V
		status=$?
		exit $status
		;;
	*)	echo $USAGE 1>&2
		exit 2
		;;
	esac
done
set -- extra "$@"
shift $OPTIND
case $# in
	1)
		if [ -b "$1" ] || [ -f "$1" ]; then
			xfs_db -p xfs_info -c "info" $OPTS "$1"
			status=$?
		else
			xfs_spaceman -p xfs_info -c "info" $OPTS "$1"
			status=$?
		fi
		;;
	*)	echo $USAGE 1>&2
		exit 2
		;;
esac
exit $status
