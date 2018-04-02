#define _GNU_SOURCE

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

#include "common.h"

const char *str_quote(const char *s)
{
	static char buf[1024];
	int r = snprintf(buf, sizeof(buf), "\"%.*s\"", (int)sizeof(buf) - 4, s);
	if (r >= (int)sizeof(buf)) {
		buf[sizeof(buf) - 1] = 0;
	}
	return buf;
}


void net_set_buffer_size(int cd, int max, int send)
{
	int i, flag;

	if (send) {
		flag = SO_SNDBUF;
	} else {
		flag = SO_RCVBUF;
	}

	for (i = 0; i < 10; i++) {
		int bef;
		socklen_t size = sizeof(bef);
		if (getsockopt(cd, SOL_SOCKET, flag, &bef, &size) < 0) {
			PFATAL("getsockopt(SOL_SOCKET)");
			break;
		}
		if (bef >= max) {
			break;
		}

		size = bef * 2;
		if (setsockopt(cd, SOL_SOCKET, flag, &size, sizeof(size)) < 0) {
			// don't log error, just break
			break;
		}
	}
}

void parse_addr(struct net_addr *netaddr, const char *addr) {
	char *colon = strrchr(addr, ':');
	if (colon == NULL) {
		error("You forgot to specify a port.");
	}
	int port = atoi(colon+1);
	if (port < 0 || port > 65535) {
		error("Invalid port number %d", port);
	}
	char host[255];
	int addr_len = colon-addr > 254 ? 254 : colon-addr;
	strncpy(host, addr, addr_len);
	host[addr_len] = '\0';
	net_gethostbyname(netaddr, host, port);
}

void net_gethostbyname(struct net_addr *shost, const char *host, int port)
{
	memset(shost, 0, sizeof(struct net_addr));

	struct in_addr in_addr;
	struct in6_addr in6_addr;

	/* Try ipv4 address first */
	if (inet_pton(AF_INET, host, &in_addr) == 1) {
		goto got_ipv4;
	}

	/* Then ipv6 */
	if (inet_pton(AF_INET6, host, &in6_addr) == 1) {
		goto got_ipv6;
	}

	error("inet_pton(%s)", str_quote(host));
	return;

got_ipv4:
	shost->ipver = 4;
	shost->sockaddr = (struct sockaddr*)&shost->sin4;
	shost->sockaddr_len = sizeof(shost->sin4);
	shost->sin4.sin_family = AF_INET;
	shost->sin4.sin_port = htons(port);
	shost->sin4.sin_addr = in_addr;
	return;

got_ipv6:
	shost->ipver = 6;
	shost->sockaddr = (struct sockaddr*)&shost->sin6;
	shost->sockaddr_len = sizeof(shost->sin4);
	shost->sin6.sin6_family = AF_INET6;
	shost->sin6.sin6_port = htons(port);
	shost->sin6.sin6_addr = in6_addr;
	return;
}

const char *addr_to_str(struct net_addr *addr) {
	char dst[INET6_ADDRSTRLEN + 1];
	int port = 0;

	switch (addr->ipver) {
	case 4: {
		inet_ntop(AF_INET, &addr->sin4.sin_addr, dst, INET6_ADDRSTRLEN);
		port = ntohs(addr->sin4.sin_port);
	} break;
	case 16: {
		inet_ntop(AF_INET6, &addr->sin6.sin6_addr, dst, INET6_ADDRSTRLEN);
		port = ntohs(addr->sin6.sin6_port);
	} break;
	default:
		dst[0] = '?';
		dst[1] = 0x00;
	}

	static char buf[255];
	snprintf(buf, sizeof(buf), "%s:%i", dst, port);
	return buf;
}

int net_bind_udp(struct net_addr *shost, int reuseport)
{
	int sd = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (sd < 0) {
		PFATAL("socket()");
	}

	int one = 1;
	int r = setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, (char *)&one,
			   sizeof(one));
	if (r < 0) {
		PFATAL("setsockopt(SO_REUSEADDR)");
	}

	if (reuseport) {
		one = 1;
		r = setsockopt(sd, SOL_SOCKET, SO_REUSEPORT, (char*)&one, sizeof(one));
		if (r < 0) {
			PFATAL("setsockopt(SO_REUSEPORT)");
		}
	}

	if (bind(sd, shost->sockaddr, shost->sockaddr_len) < 0) {
		PFATAL("bind()");
	}

	return sd;
}

#define THREAD_NAME_MAX_LEN 32

struct thread {
	pthread_t thread_id;
	int on_core;
    thread_fn callback;
	cleanup_fn cleanup;
	void *userdata;
	pthread_barrier_t initialized;
	char name[32];
};

static void cleanup_barrier(void *barrier)
{
    pthread_barrier_destroy((pthread_barrier_t *)barrier);
}

static void *thread_start(void *userdata)
{
	struct thread *thread = userdata;

	/* Direct all signals to main thread. */
	sigset_t set;
	sigfillset(&set);
	int r = pthread_sigmask(SIG_SETMASK, &set, NULL);
	if (r != 0) {
		PFATAL("pthread_sigmask()");
	}

    int rv = pthread_setname_np(pthread_self(), thread->name);
    if (rv) {
        PFATAL("[ERROR] pthread_setname_np()");
    }

    /*pthread_cleanup_push(free, thread);
    pthread_cleanup_push(cleanup_barrier, (void *)&thread->initialized);
    pthread_cleanup_push(thread->cleanup, thread->userdata);*/

    rv = pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    if (rv) {
        PFATAL("pthread_setcancelstate()");
    }

    rv = pthread_barrier_wait(&thread->initialized);
    if (rv != PTHREAD_BARRIER_SERIAL_THREAD && rv != 0) {
        PFATAL("pthread_barrier()");
    }

	thread->callback(thread->userdata);
	return NULL;
}

struct thread* thread_spawn(char* name, thread_fn callback, cleanup_fn cleanup,
                            void *userdata, int on_core)
{
	assert(callback != NULL);
	assert(name != NULL);
	assert(strlen(name) < THREAD_NAME_MAX_LEN);
	assert(on_core < MAX_CORES);
	
	struct thread *thread = calloc(1, sizeof(struct thread));
	thread->callback = callback;
	thread->userdata = userdata;
	thread->on_core = on_core;
	thread->cleanup = cleanup;
	strncpy(thread->name, name, THREAD_NAME_MAX_LEN);
	thread->name[THREAD_NAME_MAX_LEN-1] = '\0';


    pthread_barrier_init(&thread->initialized, NULL, 2);

	pthread_attr_t attr;
	pthread_attr_init(&attr);

    // Pin the thread
	if (thread->on_core != -1) {
		printf("Setting thread affinity.\n");
		cpu_set_t set;
		CPU_ZERO(&set);
		CPU_SET(on_core, &set);
		pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set);
	}

	int r = pthread_create(&thread->thread_id, (thread->on_core != -1) ? &attr : NULL,
	                       thread_start, thread);
	if (r != 0) {
		PFATAL("pthread_create()");
		return NULL;
	}

    // Wait until thread is running
    pthread_barrier_wait(&thread->initialized);

	// Destroy the attr we used for the affinity
    int rv = pthread_attr_destroy(&attr);
    if (rv != 0) {
        PFATAL("pthread_attr_destroy()");
        return NULL;
    }

	return thread;
}

int net_connect_udp(struct net_addr *shost, int src_port)
{
	int sd = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (sd < 0) {
		PFATAL("socket()");
	}

	if (src_port > 1 && src_port < 65536) {
		struct net_addr src;
		memset(&src, 0, sizeof(struct net_addr));
		char buf[32];
		snprintf(buf, sizeof(buf), "0.0.0.0:%d", src_port);
		parse_addr(&src, buf);
		if (bind(sd, src.sockaddr, src.sockaddr_len) < 0) {
			PFATAL("bind()");
		}
	}

	if (-1 == connect(sd, shost->sockaddr, shost->sockaddr_len)) {
		/* is non-blocking, so we don't get error at that point yet */
		if (EINPROGRESS != errno) {
			PFATAL("connect()");
			return -1;
		}
	}

	return sd;
}
