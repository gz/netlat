#define MAX_CORES 56
#define PACKET_SIZE 64

typedef void (*thread_fn)(void *);
typedef void (*cleanup_fn)(void *);

#define EOUT(x...) fprintf(stderr, x)

#define error(msg...) \
        do { EOUT(msg); exit(EXIT_FAILURE); } while (0)

#define PFATAL(x...)                                                   \
	do {                                                               \
		EOUT("[ERROR] " x);                                          \
		EOUT("\n\tLocation : %s(), %s:%u\n", __FUNCTION__, __FILE__, \
		       __LINE__);                                              \
		perror("OS message ");                                         \
		EOUT("\n");                                                  \
		exit(EXIT_FAILURE);                                            \
	} while (0)

#define TIMESPEC_NSEC(ts) ((ts)->tv_sec * 1000000000ULL + (ts)->tv_nsec)
#define TIMEVAL_NSEC(ts)                                                       \
	((ts)->tv_sec * 1000000000ULL + (ts)->tv_usec * 1000ULL)
#define NSEC_TIMESPEC(ns)                                                      \
	(struct timespec) { (ns) / 1000000000ULL, (ns) % 1000000000ULL }
#define NSEC_TIMEVAL(ns)                                                       \
	(struct timeval)                                                       \
	{                                                                      \
		(ns) / 1000000000ULL, ((ns) % 1000000000ULL) / 1000ULL         \
	}
#define MSEC_NSEC(ms) ((ms)*1000000ULL)


/* net.c */
struct net_addr
{
	int ipver;
	struct sockaddr_in sin4;
	struct sockaddr_in6 sin6;
	struct sockaddr *sockaddr;
	int sockaddr_len;
};

void parse_addr(struct net_addr *netaddr, const char *addr);
const char *addr_to_str(struct net_addr *addr);
int net_bind_udp(struct net_addr *addr, int reuseport);
void net_set_buffer_size(int cd, int max, int send);
void net_gethostbyname(struct net_addr *shost, const char *host, int port);
int net_connect_udp(struct net_addr *addr, int src_port);

struct thread;
struct thread *thread_spawn(char* name, thread_fn callback, cleanup_fn cleanup,
                            void *userdata, int on_core);
