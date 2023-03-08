/**
 * Copyright (c) 2016
 * Copyright Owner: Biztech
 * Author: bizbase_bill@sogou-inc.com
 */

#include <iostream>
#include <arpa/inet.h>
#include <net/if.h>
#include <net/if_arp.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>

/**
 * get local ip for eth0
 */
inline char* GetLocalIpStr()
{/*{{{*/
	std::string return_ip_str;
	char* ip = NULL;
	int fd;         
	int if_len;     
	struct ifreq buf[16];
	struct ifconf ifc;

	if((fd = socket(AF_INET, SOCK_DGRAM, 0)) == -1) {
		return ip;
	}

	ifc.ifc_len = sizeof(buf);
	ifc.ifc_buf = (caddr_t) buf;

	if(ioctl(fd, SIOCGIFCONF, (char *) &ifc) == -1) {
		close(fd);
		return ip;
	}

	if_len = ifc.ifc_len / sizeof(struct ifreq);
	if(if_len < 1) {
		close(fd);
		return ip;
	}

	int count = if_len;
	std::string target_prefix = "eth";
	while(count-- > 0) {
		std::string _prefix = buf[count].ifr_name;
		if(_prefix.find("bond") != std::string::npos) {
			target_prefix = "bond";
		}
	}

	while(if_len-- > 0) {
		std::string _prefix = buf[if_len].ifr_name;
		if(_prefix.find(target_prefix) == std::string::npos) {
			continue;
		}
		/*
		   if(0 != strcmp("eth0", buf[if_len].ifr_name)) {
		   continue;
		   }
		   */
		if(!(ioctl(fd, SIOCGIFFLAGS, (char *) &buf[if_len]))) {
			if(! buf[if_len].ifr_flags & IFF_UP) {
				close(fd);
				return ip;
			}
		} else {
			close(fd);
			return ip;
		}
		if(!(ioctl(fd, SIOCGIFADDR, (char *) &buf[if_len]))) {
			ip = (char*)inet_ntoa(((struct sockaddr_in*) (&buf[if_len].ifr_addr))->sin_addr);
			if( ip != NULL ) {
				return_ip_str = return_ip_str + std::string(",") + std::string(ip);
			}
			//close(fd);
		} else {
			close(fd);
			return ip;
		}
	}//while

	close(fd);

	if(return_ip_str == "") {
		return NULL;
	}
	return_ip_str = return_ip_str.substr(1);
	return const_cast<char*>(return_ip_str.c_str());
}/*}}}*/
