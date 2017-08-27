#!/bin/bash
openvpnPid="vpn-pid.txt"
if [ -f "$openvpnPid" ];then
	sudo kill `cat vpn-pid.txt`
	rm vpn-pid.txt
else
	echo "the openvpn service is not running"
fi
