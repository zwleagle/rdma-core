#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>

#define PORT "18515"

/* 连接建立时通过 CM Private Data 交换的内存元数据 */
struct cm_private_data {
    uint64_t addr;
    uint32_t rkey;
};

/* 辅助函数：等待并获取指定的 CM Event */
static struct rdma_cm_event *get_cm_event(struct rdma_event_channel *channel, enum rdma_cm_event_type expected_type) {
    struct rdma_cm_event *event = NULL;
    if (rdma_get_cm_event(channel, &event) != 0) {
        perror("[Error] rdma_get_cm_event failed");
        return NULL;
    }
    if (event->event != expected_type) {
        fprintf(stderr, "[Error] Unexpected CM Event: %s (expected %s)\n",
                rdma_event_str(event->event), rdma_event_str(expected_type));
        rdma_ack_cm_event(event);
        return NULL;
    }
    return event;
}

/* ==================== Server 逻辑 ==================== */
static int run_server() {
    struct rdma_event_channel *channel = NULL;
    struct rdma_cm_id *listen_id = NULL, *cm_id = NULL;
    struct rdma_cm_event *event = NULL;
    struct ibv_pd *pd = NULL;
    struct ibv_cq *cq = NULL;
    struct ibv_mr *mr = NULL;
    uint64_t *buf = NULL;
    struct sockaddr_in addr;

    // 1. 创建 Event Channel 与 Listen ID
    channel = rdma_create_event_channel();
    if (rdma_create_id(channel, &listen_id, NULL, RDMA_PS_TCP)) {
        perror("[Server] rdma_create_id failed");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(atoi(PORT));
    addr.sin_addr.s_addr = INADDR_ANY;

    if (rdma_bind_addr(listen_id, (struct sockaddr *)&addr)) {
        perror("[Server] rdma_bind_addr failed");
        return 1;
    }

    if (rdma_listen(listen_id, 1)) {
        perror("[Server] rdma_listen failed");
        return 1;
    }
    printf("[Server] Listening on port %s for CM connections...\n", PORT);

    // 2. 等待 Client 的连接请求 (CONNECT_REQUEST)
    event = get_cm_event(channel, RDMA_CM_EVENT_CONNECT_REQUEST);
    if (!event) return 1;
    cm_id = event->id;
    rdma_ack_cm_event(event);

    // 3. 分配 8 字节对齐内存并注册 MR
    if (posix_memalign((void **)&buf, 8, sizeof(uint64_t)) != 0) {
        fprintf(stderr, "[Server] Memory allocation failed\n");
        return 1;
    }
    *buf = 100; // 目标内存初始值置为 100
    printf("[Server] Target memory initialized to %lu\n", *buf);

    pd = ibv_alloc_pd(cm_id->verbs);
    cq = ibv_create_cq(cm_id->verbs, 10, NULL, NULL, 0);
    mr = ibv_reg_mr(pd, buf, sizeof(uint64_t),
                    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                    IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);

    // 4. 创建 QP (librdmacm 会关联该 QP 到 cm_id)
    struct ibv_qp_init_attr qp_attr = {
        .send_cq = cq,
        .recv_cq = cq,
        .cap     = { .max_send_wr = 10, .max_recv_wr = 10, .max_send_sge = 1, .max_recv_sge = 1 },
        .qp_type = IBV_QPT_RC
    };
    if (rdma_create_qp(cm_id, pd, &qp_attr)) {
        perror("[Server] rdma_create_qp failed");
        return 1;
    }

    // 5. 接受连接请求，并在 private_data 中捎带 Server 端的 addr 和 rkey
    struct cm_private_data server_data = {
        .addr = (uint64_t)(uintptr_t)buf,
        .rkey = mr->rkey
    };
    struct rdma_conn_param conn_param = {
        .responder_resources = 1, // 允许接收 RDMA Read/Atomic
        .initiator_depth     = 1,
        .private_data        = &server_data,
        .private_data_len    = sizeof(server_data)
    };

    if (rdma_accept(cm_id, &conn_param)) {
        perror("[Server] rdma_accept failed");
        return 1;
    }

    // 6. 等待 ESTABLISHED 事件
    event = get_cm_event(channel, RDMA_CM_EVENT_ESTABLISHED);
    if (!event) return 1;
    rdma_ack_cm_event(event);
    printf("[Server] Connection ESTABLISHED successfully!\n");

    // 7. 等待 Client 执行完 CAS 后断开连接
    event = get_cm_event(channel, RDMA_CM_EVENT_DISCONNECTED);
    if (event) rdma_ack_cm_event(event);

    printf("[Server] Final memory value after CAS = %lu\n", *buf);

    // 清理资源
    rdma_destroy_qp(cm_id);
    ibv_dereg_mr(mr);
    ibv_destroy_cq(cq);
    ibv_dealloc_pd(pd);
    free(buf);
    rdma_destroy_id(cm_id);
    rdma_destroy_id(listen_id);
    rdma_destroy_event_channel(channel);
    return 0;
}

/* ==================== Client 逻辑 ==================== */
static int run_client(const char *server_ip) {
    struct rdma_event_channel *channel = NULL;
    struct rdma_cm_id *cm_id = NULL;
    struct rdma_cm_event *event = NULL;
    struct ibv_pd *pd = NULL;
    struct ibv_cq *cq = NULL;
    struct ibv_mr *mr = NULL;
    uint64_t *buf = NULL;
    struct addrinfo *res = NULL;

    // 1. 解析目标 IP 并创建 cm_id
    if (getaddrinfo(server_ip, PORT, NULL, &res)) {
        fprintf(stderr, "[Client] getaddrinfo failed\n");
        return 1;
    }

    channel = rdma_create_event_channel();
    if (rdma_create_id(channel, &cm_id, NULL, RDMA_PS_TCP)) {
        perror("[Client] rdma_create_id failed");
        return 1;
    }

    // 2. 解析地址与路由 (librdmacm 会自动映射 IPv4 到对应的 RoCE GID)
    if (rdma_resolve_addr(cm_id, NULL, res->ai_addr, 2000)) {
        perror("[Client] rdma_resolve_addr failed");
        return 1;
    }
    freeaddrinfo(res);

    event = get_cm_event(channel, RDMA_CM_EVENT_ADDR_RESOLVED);
    if (!event) return 1;
    rdma_ack_cm_event(event);

    if (rdma_resolve_route(cm_id, 2000)) {
        perror("[Client] rdma_resolve_route failed");
        return 1;
    }
    event = get_cm_event(channel, RDMA_CM_EVENT_ROUTE_RESOLVED);
    if (!event) return 1;
    rdma_ack_cm_event(event);

    // 3. 准备内存与 QP
    if (posix_memalign((void **)&buf, 8, sizeof(uint64_t)) != 0) return 1;
    *buf = 0; // 存放 CAS 替换返回的前旧值

    pd = ibv_alloc_pd(cm_id->verbs);
    cq = ibv_create_cq(cm_id->verbs, 10, NULL, NULL, 0);
    mr = ibv_reg_mr(pd, buf, sizeof(uint64_t), IBV_ACCESS_LOCAL_WRITE);

    struct ibv_qp_init_attr qp_attr = {
        .send_cq = cq,
        .recv_cq = cq,
        .cap     = { .max_send_wr = 10, .max_recv_wr = 10, .max_send_sge = 1, .max_recv_sge = 1 },
        .qp_type = IBV_QPT_RC
    };
    if (rdma_create_qp(cm_id, pd, &qp_attr)) {
        perror("[Client] rdma_create_qp failed");
        return 1;
    }

    // 4. 发起连接 (rdma_connect 内部会自动把 QP 从 RESET 提升至 RTS)
    struct rdma_conn_param conn_param = {
        .responder_resources = 1,
        .initiator_depth     = 1,
        .retry_count         = 7,
        .rnr_retry_count     = 7
    };
    if (rdma_connect(cm_id, &conn_param)) {
        perror("[Client] rdma_connect failed");
        return 1;
    }

    // 5. 等待 ESTABLISHED 并获取 Server 在 private_data 中返回的 addr 与 rkey
    event = get_cm_event(channel, RDMA_CM_EVENT_ESTABLISHED);
    if (!event) return 1;

    struct cm_private_data *server_data = (struct cm_private_data *)event->param.conn.private_data;
    uint64_t remote_addr = server_data->addr;
    uint32_t remote_rkey = server_data->rkey;
    rdma_ack_cm_event(event);

    printf("[Client] Connected to Server. Remote Addr: 0x%lx, Remote RKey: 0x%x\n", remote_addr, remote_rkey);

    // 6. 执行 CAS 操作 (Compare=100, Swap=200)
    struct ibv_sge sge = {
        .addr   = (uint64_t)(uintptr_t)buf,
        .length = sizeof(uint64_t),
        .lkey   = mr->lkey
    };

    struct ibv_send_wr wr = {
        .wr_id      = 2001,
        .sg_list    = &sge,
        .num_sge    = 1,
        .opcode     = IBV_WR_ATOMIC_CMP_AND_SWP,
        .send_flags = IBV_SEND_SIGNALED,
        .wr.atomic  = {
            .remote_addr = remote_addr,
            .rkey        = remote_rkey,
            .compare_add = 100,
            .swap        = 200
        }
    };

    struct ibv_send_wr *bad_wr = NULL;
    printf("[Client] Posting CAS request (Compare=100, Swap=200)...\n");
    if (ibv_post_send(cm_id->qp, &wr, &bad_wr)) {
        perror("[Client] ibv_post_send failed");
        return 1;
    }

    // 轮询 CQ
    struct ibv_wc wc;
    while (ibv_poll_cq(cq, 1, &wc) == 0);

    if (wc.status == IBV_WC_SUCCESS) {
        printf("[Client] CAS SUCCESS! Server returned old value = %lu\n", *buf);
    } else {
        fprintf(stderr, "[Client] CAS Work Completion failed: %s\n", ibv_wc_status_str(wc.status));
    }

    // 7. 断开连接并清理
    rdma_disconnect(cm_id);
    rdma_destroy_qp(cm_id);
    ibv_dereg_mr(mr);
    ibv_destroy_cq(cq);
    ibv_dealloc_pd(pd);
    free(buf);
    rdma_destroy_id(cm_id);
    rdma_destroy_event_channel(channel);
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc == 1) {
        return run_server();
    } else {
        return run_client(argv[1]);
    }
}
