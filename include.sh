if [ ! -e /usr/testbed ]
then
	SSHI="-i ec2-wintermute.pem"
fi
SSHOPT="-o StrictHostKeyChecking=no $SSHI"
SSH="ssh $SSHOPT"
SCP="scp $SSHOPT -C"
