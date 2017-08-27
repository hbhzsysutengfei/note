#!/bin/bash
openvpnPid="vpn-pid.txt"
if [ ! -f "$openvpnPid" ];then
	sudo nohup  openvpn /etc/openvpn/client.ovpn & echo $! > vpn-pid.txt
else
	echo "please stop the openvpn first by the stop-vpn.sh"
fi


