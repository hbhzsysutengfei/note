iptables -t nat -F
iptables -P INPUT ACCEPT
iptables -P FORWARD ACCEPT
iptables -t nat -A PREROUTING -d 10.8.0.6 -p tcp --dport 8080 -j DNAT --to-destination 192.168.1.64:80
iptables -t nat -A PREROUTING -d 10.8.0.6 -p tcp --dport 8000 -j DNAT --to-destination 192.168.1.64:8000
iptables -t nat -A POSTROUTING -s 192.168.1.64 -p tcp  -j SNAT --to-source 10.8.0.6
