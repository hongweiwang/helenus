if [ -e /usr/testbed ]
then
	while read line
	do
		case $line in
			*client-* )
			ip=`echo $line|cut -d' ' -f1`
			echo $ip
			;;
		esac
	done < /etc/hosts
else
	cat `dirname $0`/clients.list
fi
