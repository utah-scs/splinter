#include <rte_config.h>
#include <rte_eal.h>
#include <rte_ethdev.h>
#include <rte_pci.h>
#include <rte_ip.h>
#include "mempool.h"

#include <pthread.h>

// Set of receive spinlocks. Required to allow any core to receive packets from
// any rx queue on the port.
pthread_spinlock_t rx_locks[16];

#define HW_RXCSUM 0
#define HW_TXCSUM 0
#define MIN(a, b) ((a) < (b) ? (a) : (b))
static const struct rte_eth_conf default_eth_conf = {
    .link_speeds = ETH_LINK_SPEED_AUTONEG, /* auto negotiate speed */
    /*.link_duplex = ETH_LINK_AUTONEG_DUPLEX,	[> auto negotiation duplex <]*/
    .lpbk_mode = 0,
    .rxmode =
        {
            .mq_mode        = ETH_MQ_RX_NONE,/* Use RSS without DCB or VMDQ */
            .max_rx_pkt_len = 0,             /* valid only if jumbo is on */
            .split_hdr_size = 0,             /* valid only if HS is on */
            .header_split   = 0,             /* Header Split off */
            .hw_ip_checksum = 0,             /* IP checksum offload */
            .hw_vlan_filter = 0,             /* VLAN filtering */
            .hw_vlan_strip  = 0,             /* VLAN strip */
            .hw_vlan_extend = 0,             /* Extended VLAN */
            .jumbo_frame    = 0,             /* Jumbo Frame support */
        },
    .txmode =
        {
            .mq_mode = ETH_MQ_TX_NONE, /* Disable DCB and VMDQ */
        },
    .fdir_conf =
        {
            .mode    = RTE_FDIR_MODE_PERFECT,
            .pballoc = RTE_FDIR_PBALLOC_64K,
            .mask =
                {
                    .dst_port_mask = 0xffff,
                },
        },
    /* No interrupt */
    .intr_conf =
        {
            .lsc = 0,
        },
};

int num_pmd_ports() {
    return rte_eth_dev_count();
}

int get_pmd_ports(struct rte_eth_dev_info* info, int len) {
    int num_ports   = rte_eth_dev_count();
    int num_entries = MIN(num_ports, len);
    for (int i = 0; i < num_entries; i++) {
        memset(&info[i], 0, sizeof(struct rte_eth_dev_info));
        rte_eth_dev_info_get(i, &info[i]);
    }
    return num_entries;
}

int get_rte_eth_dev_info(int dev, struct rte_eth_dev_info* info) {
    if (dev >= rte_eth_dev_count()) {
        return -ENODEV;
    } else {
        rte_eth_dev_info_get(dev, info);
        return 0;
    }
}

int max_rxqs(int dev) {
    struct rte_eth_dev_info info;
    if (get_rte_eth_dev_info(dev, &info) != 0) {
        return -ENODEV;
    } else {
        return info.max_rx_queues;
    }
}

int max_txqs(int dev) {
    struct rte_eth_dev_info info;
    if (get_rte_eth_dev_info(dev, &info) != 0) {
        return -ENODEV;
    } else {
        return info.max_tx_queues;
    }
}

void enumerate_pmd_ports() {
    int num_dpdk_ports = rte_eth_dev_count();
    int i;

    printf("%d DPDK PMD ports have been recognized:\n", num_dpdk_ports);
    for (i = 0; i < num_dpdk_ports; i++) {
        struct rte_eth_dev_info dev_info;

        memset(&dev_info, 0, sizeof(dev_info));
        rte_eth_dev_info_get(i, &dev_info);

        printf("DPDK port_id %d (%s)   RXQ %hu TXQ %hu  ", i, dev_info.driver_name,
               dev_info.max_rx_queues, dev_info.max_tx_queues);

        if (dev_info.pci_dev) {
            printf("%04hx:%02hhx:%02hhx.%02hhx %04hx:%04hx  ", dev_info.pci_dev->addr.domain,
                   dev_info.pci_dev->addr.bus, dev_info.pci_dev->addr.devid, dev_info.pci_dev->addr.function,
                   dev_info.pci_dev->id.vendor_id, dev_info.pci_dev->id.device_id);
        }

        printf("\n");
    }
}

int init_pmd_port(int port, int rxqs, int txqs, int rxq_core[], int txq_core[], int nrxd, int ntxd,
                  int loopback, int tso, int csumoffload) {
    struct rte_eth_dev_info dev_info = {};
    struct rte_eth_conf eth_conf;
    struct rte_eth_rxconf eth_rxconf;
    struct rte_eth_txconf eth_txconf;
    int ret, i, q;

    // If there are more than 16 receive queues, then return because we
    // currently support only 16 receive locks.
    if (rxqs > 16) {
        return -EINVAL;
    }

    // Initialize all receive locks.
    for (q = 0; q < 16; q++) {
        pthread_spin_init(&(rx_locks[q]), PTHREAD_PROCESS_PRIVATE);
    }

    /* Need to accesss rte_eth_devices manually since DPDK currently
     * provides no other mechanism for checking whether something is
     * attached */
    if (port >= RTE_MAX_ETHPORTS || rte_eth_devices[port].state != RTE_ETH_DEV_ATTACHED) {
        printf("Port not found %d\n", port);
        return -ENODEV;
    }

    eth_conf           = default_eth_conf;
    eth_conf.lpbk_mode = !(!loopback);

    /* Use defaut rx/tx configuration as provided by PMD drivers,
     * with minor tweaks */
    rte_eth_dev_info_get(port, &dev_info);

    eth_rxconf = dev_info.default_rxconf;
    /* Drop packets when no descriptors are available */
    eth_rxconf.rx_drop_en = 1;

    eth_txconf           = dev_info.default_txconf;
    tso                  = !(!tso);
    csumoffload          = !(!csumoffload);
    eth_txconf.txq_flags = ETH_TXQ_FLAGS_NOVLANOFFL | ETH_TXQ_FLAGS_NOMULTSEGS * (1 - tso) |
                           ETH_TXQ_FLAGS_NOXSUMS * (1 - csumoffload);

    ret = rte_eth_dev_configure(port, rxqs, txqs, &eth_conf);
    if (ret != 0) {
        return ret; /* Don't need to clean up here */
    }

    /* Disable promiscuous mode */
    rte_eth_promiscuous_disable(port);

    for (i = 0; i < rxqs; i++) {
        int sid = rte_lcore_to_socket_id(rxq_core[i]);
        ret = rte_eth_rx_queue_setup(port, i, nrxd, sid, &eth_rxconf, get_pframe_pool(rxq_core[i], sid));
        if (ret != 0) {
            printf("Failed to initialize rxq\n");
            return ret; /* Clean things up? */
        }
    }

    for (i = 0; i < txqs; i++) {
        int sid = rte_lcore_to_socket_id(txq_core[i]);

        ret = rte_eth_tx_queue_setup(port, i, ntxd, sid, &eth_txconf);
        if (ret != 0) {
            printf("Failed to initialize txq\n");
            return ret; /* Clean things up */
        }
    }

    ret = rte_eth_dev_start(port);
    if (ret != 0) {
        printf("Failed to start \n");
        return ret; /* Clean up things */
    }

    /* Flow director setup */
    int retval = 0;

    /*
     * First, setup details regarding the flow to be filtered. Restrict to
     * IP+UDP packets, and look at only the UDP destination port.
     */
    /*
    struct rte_eth_fdir_filter_info filter_info;
    memset(&filter_info, 0, sizeof(filter_info));
    filter_info.info_type = RTE_ETH_FDIR_FILTER_INPUT_SET_SELECT;
    filter_info.info.input_set_conf.flow_type = RTE_ETH_FLOW_NONFRAG_IPV4_UDP;
    filter_info.info.input_set_conf.inset_size = 1;
    filter_info.info.input_set_conf.field[0] =
                                            RTE_ETH_INPUT_SET_L4_UDP_DST_PORT;
    filter_info.info.input_set_conf.op = RTE_ETH_INPUT_SET_SELECT;
    retval = rte_eth_dev_filter_ctrl(port, RTE_ETH_FILTER_FDIR,
                    RTE_ETH_FILTER_SET, &filter_info);
    if (retval != 0) {
        rte_exit(EXIT_FAILURE, "Could not set fdir info: %s\n",
                        strerror(-retval));
    }
    */

    /*
     * Next, configure a rule for each receive queue. Redirect packets with UDP
     * destination port 'i' to receive queue 'i'.
     */
    for (i = 0; i < rxqs; i++) {
        struct rte_eth_fdir_filter fdirf;
        memset(&fdirf, 0, sizeof(fdirf));
        fdirf.soft_id = i;
        fdirf.input.flow_type = RTE_ETH_FLOW_NONFRAG_IPV4_UDP;
        fdirf.input.flow.udp4_flow.dst_port = rte_cpu_to_be_16(i);
        fdirf.action.rx_queue = i;
        fdirf.action.behavior = RTE_ETH_FDIR_ACCEPT;
        fdirf.action.report_status = RTE_ETH_FDIR_NO_REPORT_STATUS;
        retval = rte_eth_dev_filter_ctrl(port, RTE_ETH_FILTER_FDIR,
                        RTE_ETH_FILTER_ADD, &fdirf);
        if (retval != 0) {
            rte_exit(EXIT_FAILURE, "Could not add fdir UDP filter: %s\n",
                            strerror(-retval));
        }
    }

    return 0;
}

void free_pmd_port(int port) {
    rte_eth_dev_stop(port);
    rte_eth_dev_close(port);

    int q = 0;
    // Destroy all receive locks.
    for (q = 0; q < 16; q++) {
        pthread_spin_destroy(&(rx_locks[q]));
    }
}

int recv_pkts(int port, int qid, mbuf_array_t pkts, int len) {
    // Try to acquire a receive lock. If the lock is busy, then return.
    if (pthread_spin_trylock(&(rx_locks[qid])) != 0) {
        return 0;
    }

    int ret = rte_eth_rx_burst(port, qid, (struct rte_mbuf**)pkts, len);

    // Release the spinlock.
    pthread_spin_unlock(&(rx_locks[qid]));

/* Removed prefetching since the benefit in performance for single core was
 * outweighed by the loss in performance with several cores. */
#if 0
    for (int i = 0; i < ret; i++) {
        rte_prefetch0(rte_pktmbuf_mtod(pkts[i], void*));
    }
#endif
    return ret;
}

int send_pkts(int port, int qid, mbuf_array_t pkts, int len) {
    return rte_eth_tx_burst(port, (uint16_t)qid, (struct rte_mbuf**)pkts, (uint16_t)len);
}

int find_port_with_pci_address(const char* pci) {
    struct rte_pci_addr addr;
    char devargs[1024];
    int ret;
    uint8_t port_id;

    // Cannot parse address
    if (eal_parse_pci_DomBDF(pci, &addr) != 0 && eal_parse_pci_BDF(pci, &addr) != 0) {
        return -1;
    }

    int n_devices = rte_eth_dev_count();
    for (int i = 0; i < n_devices; i++) {
        struct rte_eth_dev_info dev_info;
        rte_eth_dev_info_get(i, &dev_info);

        if (dev_info.pci_dev) {
            if (rte_eal_compare_pci_addr(&addr, &dev_info.pci_dev->addr)) {
                return i;
            }
        }
    }

    /* If not found, maybe the device has not been attached yet */

    snprintf(devargs, 1024, "%04x:%02x:%02x.%02x", addr.domain, addr.bus, addr.devid, addr.function);

    ret = rte_eth_dev_attach(devargs, &port_id);

    if (ret < 0) {
        return -1;
    }
    return (int)port_id;
}

/* Attach a device with a given name (useful when attaching virtual devices). Returns either the
   port number of the
   device or an error if not found. */
int attach_pmd_device(const char* devname) {
    uint8_t port = 0;
    printf("Devname: \"%s\"\n", devname);
    int error = rte_eth_dev_attach(devname, &port);

    if (error != 0) {
        // Could not attach
        return -ENODEV;
    }
    return (int)port;
}

/* FIXME: Add function to modify RSS hash function using
 * rte_eth_dev_rss_hash_update */
