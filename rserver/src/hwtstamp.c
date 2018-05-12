#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

#include <sys/time.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/ioctl.h>
#include <arpa/inet.h>
#include <net/if.h>
#include <linux/net_tstamp.h>

#ifndef SIOCSHWTSTAMP
# define SIOCSHWTSTAMP 0x89b0
#endif


struct sockaddr getifaceaddr(char* interface) {
    struct ifreq device;
    int fd;
    fd = socket(AF_INET, SOCK_DGRAM, 0);
    memset(&device, 0, sizeof(device));
    strncpy(device.ifr_name, interface, sizeof(device.ifr_name));
    if (ioctl(fd, SIOCGIFADDR, &device) < 0) {
        perror("getting interface IP address");
    }
    close(fd);

    return device.ifr_addr;
}

/**
 * \brief Enables HW Timestamping mechanism on the NIC and for the socket
 **/
int enable_hwtstamp(int sock, char* interface)
{
    struct ifreq hwtstamp;
    struct hwtstamp_config hwconfig, hwconfig_requested;

    memset(&hwtstamp, 0, sizeof(hwtstamp));
    strncpy(hwtstamp.ifr_name, interface, sizeof(hwtstamp.ifr_name));
    hwtstamp.ifr_data = (void *)&hwconfig;

    memset(&hwconfig, 0, sizeof(hwconfig));
    hwconfig.tx_type = HWTSTAMP_TX_ON;
    hwconfig.rx_filter = HWTSTAMP_FILTER_ALL;
    
    hwconfig_requested = hwconfig;
    
    int so_timestamping_flags = 0;
    if (ioctl(sock, SIOCSHWTSTAMP, &hwtstamp) < 0) {
        printf("Hardware TX failed, enable software timestamping instead...\n");
        so_timestamping_flags |= SOF_TIMESTAMPING_SOFTWARE;
        so_timestamping_flags |= SOF_TIMESTAMPING_TX_SOFTWARE;
        so_timestamping_flags |= SOF_TIMESTAMPING_RX_SOFTWARE;
    }
    else {
        so_timestamping_flags |= SOF_TIMESTAMPING_RAW_HARDWARE;
        so_timestamping_flags |= SOF_TIMESTAMPING_TX_HARDWARE;
        so_timestamping_flags |= SOF_TIMESTAMPING_RX_HARDWARE;
    }

    if (hwconfig_requested.tx_type != hwconfig.tx_type || hwconfig_requested.rx_filter != hwconfig.rx_filter) {
        fprintf(stderr, "SIOCSHWTSTAMP: tx_type %d requested, got %d; rx_filter %d requested, got %d\n",
            hwconfig_requested.tx_type, hwconfig.tx_type,
            hwconfig_requested.rx_filter, hwconfig.rx_filter);
    }

    if (setsockopt(sock, SOL_SOCKET, SO_TIMESTAMPING, &so_timestamping_flags, sizeof(so_timestamping_flags)) < 0) {
        return -2;
    }

    // request IP_PKTINFO for debugging purposes
    int enabled = 1;
    if (setsockopt(sock, SOL_IP, IP_PKTINFO,
               &enabled, sizeof(enabled)) < 0) {
        return -3;
    }

    return 0;
}
