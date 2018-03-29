#define _GNU_SOURCE // for recvmmsg

#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h> // for fprintf()
#include <stdlib.h>
#include <string.h> // for strncmp

#include <sys/epoll.h> // for epoll_create1(), epoll_ctl(), struct epoll_event
#include <sys/ioctl.h>
#include <unistd.h> // for close(), read()
#include <linux/net_tstamp.h>
#include <linux/sockios.h>
#include <net/if.h>

#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "common.h"

#define MAX_EVENTS 5

#define MTU_SIZE (2048 - 64 * 2)
#define MAX_MSG 512

struct state {
  int fd;
  volatile uint64_t bps;
  volatile uint64_t pps;
} __attribute__((aligned(64)));

struct scm_timestamping {
  struct timespec ts[3];
};

struct state *state_init(struct state *s) {
  s->bps = 0;
  s->pps = 0;
  return s;
}

// NUMA node0 CPU(s): 0,2,4,6,8,10,12,14,16,18,20,22,24,26,
// 28,30,32,34,36,38,40,42,44,46,48,50,52,54 NUMA node1 CPU(s):
// 1,3,5,7,9,11,13,15,17,19,21,23,25,27,
// 29,31,33,35,37,39,41,43,45,47,49,51,53,55

#define PINNING_PAIRS 1

#if PINNING_PAIRS
int thread_pairs[MAX_CORES] = {
    0,  28, 2,  30, 4,  32, 6,  34, 8,  36, 10, 38, 12, 40, 14, 42, 16, 44, 18,
    46, 20, 48, 22, 50, 24, 52, 26, 54, 1,  29, 3,  31, 5,  33, 7,  35, 9,  37,
    11, 39, 13, 41, 15, 43, 17, 45, 19, 47, 21, 49, 23, 51, 25, 53, 27, 55};
#endif

static void app_thread_loop(void *userdata) { printf("App thread spawned\n"); }

static void epoll_thread_loop(void *userdata) {
  struct state *state = userdata;
  struct epoll_event event, events[MAX_EVENTS];
  int running = 1;
  int event_count = 0;

  size_t bytes_read = 0;

  int epoll_fd = epoll_create1(0);
  if (epoll_fd == -1) {
    fprintf(stderr, "Failed to create epoll file descriptor\n");
    return;
  }

  event.events = EPOLLIN;
  event.data.fd = state->fd;

  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, state->fd, &event)) {
    fprintf(stderr, "Failed to add file descriptor to epoll\n");
    close(epoll_fd);
    return;
  }

  struct msghdr *msgh = calloc(1, sizeof(struct msghdr));
  struct iovec *iovec = calloc(1, sizeof(struct iovec));
  struct sockaddr* saddr = calloc(1, sizeof(struct sockaddr));

	struct cmsghdr *cmsghdr = calloc(1, CMSG_SPACE(sizeof(int)));
	cmsghdr->cmsg_len = CMSG_LEN(sizeof(int));

  static char read_buffer[PACKET_SIZE + 1];
  iovec->iov_base = (void *)read_buffer;
  iovec->iov_len = PACKET_SIZE;

  msgh->msg_name = saddr;
  msgh->msg_namelen = sizeof(struct sockaddr);
  msgh->msg_iov = iovec;
  msgh->msg_iovlen = 1;
 	msgh->msg_control = cmsghdr;
	msgh->msg_controllen = CMSG_LEN(sizeof(int));

  while (1) {
    event_count = epoll_wait(epoll_fd, events, 1, 30000);

    for (int i = 0; i < event_count; i++) {
      bytes_read = recvmsg(events[i].data.fd, msgh, 0);

      //printf("%s:%s:%d: got in buffer: %s\n", __FILE__, __FUNCTION__, __LINE__,
      //       msgh->msg_iov->iov_base);

      struct cmsghdr *cmsg;
      for (cmsg = CMSG_FIRSTHDR(msgh); cmsg != NULL;
           cmsg = CMSG_NXTHDR(msgh, cmsg)) {
        if (cmsg->cmsg_level == SOL_SOCKET &&
            cmsg->cmsg_type == SCM_TIMESTAMPING) {
          struct scm_timestamping *tstmp =
              (struct scm_timestamping *)CMSG_DATA(cmsg);
          printf("%s:%s:%d: timestamp0: %zu\n", __FILE__, __FUNCTION__,
                 __LINE__, tstmp->ts[0]);
          printf("%s:%s:%d: timestamp1: %zu\n", __FILE__, __FUNCTION__,
                 __LINE__, tstmp->ts[1]);
          printf("%s:%s:%d: timestamp2: %zu\n", __FILE__, __FUNCTION__,
                 __LINE__, tstmp->ts[2]);
          break;
        }
        //printf("%s:%s:%d: got auxilary data: level: %d type: %d\n", __FILE__,
        //       __FUNCTION__, __LINE__, cmsg->cmsg_level, cmsg->cmsg_type);
      }

      //struct sockaddr* sockad = (struct sockaddr*)msgh->msg_name;
      //printf("%s:%s:%d: state->fd = %d sockaddr %s msgh->msg_iov->iov_len %d\n",
      //  __FILE__, __FUNCTION__, __LINE__, 
      //  state->fd, sockad->sa_data, msgh->msg_iov->iov_len);

      int s = sendto(state->fd, msgh->msg_iov->iov_base, msgh->msg_iov->iov_len, 0, msgh->msg_name, msgh->msg_namelen);
      if (s == -1) {
        PFATAL("Can't send something back...");
      }
      //assert(s == 64);
      // echo back stuff...
      /*events[i].data.fd
      int s = sendmsg(state->fd, msgh, 0);
      */

    }

    __atomic_fetch_add(&state->pps, 1, 0);
    __atomic_fetch_add(&state->bps, bytes_read, 0);
  }

  if (close(epoll_fd)) {
    fprintf(stderr, "Failed to close epoll file descriptor\n");
    return;
  }
}

static void enable_hw_timestamp(int fd) {
  printf("%s:%s:%d:\n", __FILE__, __FUNCTION__, __LINE__);
  uint32_t val = SOF_TIMESTAMPING_RX_HARDWARE | SOF_TIMESTAMPING_TX_HARDWARE |
                 SOF_TIMESTAMPING_RAW_HARDWARE;
  int err = setsockopt(fd, SOL_SOCKET, SO_TIMESTAMPING, &val, sizeof(val));
  if (err != 0) {
    PFATAL("Enabling HW timestamps failed");
  }

  char* interface = "enp130s0";
 	struct ifreq hwtstamp;
   memset(&hwtstamp, 0, sizeof(hwtstamp));

  struct hwtstamp_config hwconfig;
  memset(&hwconfig, 0, sizeof(hwconfig));
  struct hwtstamp_config hwconfig_requested; 
  memset(&hwconfig_requested, 0, sizeof(hwconfig_requested));
  
  hwtstamp.ifr_data = (void *)&hwconfig;
  strncpy(hwtstamp.ifr_name, interface, sizeof(hwtstamp.ifr_name));
  hwtstamp.ifr_data = (void *)&hwconfig;


  hwconfig.tx_type = HWTSTAMP_TX_ON;
  hwconfig.rx_filter = HWTSTAMP_FILTER_PTP_V1_L4_SYNC;
  hwconfig_requested = hwconfig;

  if (ioctl(fd, SIOCSHWTSTAMP, &hwtstamp) < 0) {
    if ((errno == EINVAL || errno == ENOTSUP) &&
        hwconfig_requested.tx_type == HWTSTAMP_TX_OFF &&
        hwconfig_requested.rx_filter == HWTSTAMP_FILTER_NONE)
      printf("SIOCSHWTSTAMP: disabling hardware time stamping not possible\n");
    else {
      PFATAL("SIOCSHWTSTAMP didn't work as expected");
    }
  }
}

int main(int argc, char **argv) {
  const char *listen_addr_str = "0.0.0.0:4321";
  int recv_buf_size = 4096 * 128;
  int thread_num = 1;
  int reuseport = 0;

  switch (argc) {
  case 4:
    reuseport = atoi(argv[3]);
  case 3:
    thread_num = atoi(argv[2]);
  case 2:
    listen_addr_str = argv[1];
  case 1:
    break;
  default:
    error("Usage: %s [listen ip:port] [fork cnt] [reuseport]", argv[0]);
  }

  struct net_addr listen_addr;
  parse_addr(&listen_addr, listen_addr_str);

  int main_fd = -1;
  if (reuseport == 0) {
    fprintf(stderr, "[*] Starting udpreceiver on %s, recv buffer %iKiB\n",
            addr_to_str(&listen_addr), recv_buf_size / 1024);

    main_fd = net_bind_udp(&listen_addr, 0);
    net_set_buffer_size(main_fd, recv_buf_size, 0);
  }

  struct state *array_of_states = calloc(thread_num, sizeof(struct state));

  int t;
  for (t = 0; t < thread_num; t += 2) {
    struct state *state = &array_of_states[t];
    state_init(state);
    if (reuseport == 0) {
      state->fd = main_fd;
    } else {
      fprintf(stderr, "[*] Starting udpreceiver on %s, recv buffer %iKiB\n",
              addr_to_str(&listen_addr), recv_buf_size / 1024);

      int fd = net_bind_udp(&listen_addr, 1);
      net_set_buffer_size(fd, recv_buf_size, 0);
      enable_hw_timestamp(fd);
      state->fd = fd;
    }

    char epoll_name[32];
    char app_name[32];
    snprintf(epoll_name, 32, "epoll%d", thread_pairs[t]);
    snprintf(app_name, 32, "app%d", thread_pairs[t]);

    assert(t < MAX_CORES);

#if PINNING
    int core1 = thread_pairs[t];
    int core2 = thread_pairs[t + 1];
#else
    int core1 = -1;
    int core2 = -1;
#endif

    thread_spawn(epoll_name, epoll_thread_loop, NULL, state, core1);
    thread_spawn(app_name, app_thread_loop, NULL, state, core2);
  }

  uint64_t last_pps = 0;
  uint64_t last_bps = 0;

  while (1) {
    struct timeval timeout = NSEC_TIMEVAL(MSEC_NSEC(1000UL));
    while (1) {
      int r = select(0, NULL, NULL, NULL, &timeout);
      if (r != 0) {
        continue;
      }
      if (TIMEVAL_NSEC(&timeout) == 0) {
        break;
      }
    }

    uint64_t now_pps = 0, now_bps = 0;
    for (t = 0; t < thread_num; t++) {
      struct state *state = &array_of_states[t];
      now_pps += __atomic_load_n(&state->pps, 0);
      now_bps += __atomic_load_n(&state->bps, 0);
    }

    double delta_pps = now_pps - last_pps;
    double delta_bps = now_bps - last_bps;
    last_pps = now_pps;
    last_bps = now_bps;

    printf("%7.3fM pps, %7.3fMiB, %7.3fMb\n", delta_pps / 1000.0 / 1000.0,
           delta_bps / 1024.0 / 1024.0, delta_bps * 8.0 / 1000.0 / 1000.0);
  }

  return 0;
}