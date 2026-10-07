#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>

#define PORT 18515

/* 用于 TCP 握手交换元数据的结构体（包含 GID 支持 RoCE/Soft-RoCE） */
struct rdma_dest {
    int lid;
    int qpn;
    int psn;
    uint64_t addr;
    uint32_t rkey;
    union ibv_gid gid; /* 支持 RoCE / Soft-RoCE GID 路由 */
};

struct context {
    struct ibv_context *ctx;
    struct ibv_pd      *pd;
    struct ibv_mr      *mr;
    struct ibv_cq      *cq;
    struct ibv_qp      *qp;
    uint64_t           *buf; /* 必须 8 字节对齐 */
};

/* TCP Out-of-band 交换元数据 */
static int sock_sync_data(int sock, int size, char *local_data, char *remote_data) {
    if (write(sock, local_data, size) < size) return -1;
    if (read(sock, remote_data, size) < size) return -1;
    return 0;
}

/* 包含完整日志与 GRH/GID 配置的 QP 状态转换函数 */
static int modify_qp_to_rts(struct ibv_qp *qp, struct rdma_dest *dest, int sgid_index) {
    struct ibv_qp_attr attr;
    int flags;
    int rc;

    /* 1. RESET -> INIT */
    memset(&attr, 0, sizeof(attr));
    attr.qp_state        = IBV_QPS_INIT;
    attr.pkey_index      = 0;
    attr.port_num        = 1;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE |
                           IBV_ACCESS_REMOTE_READ |
                           IBV_ACCESS_REMOTE_WRITE |
                           IBV_ACCESS_REMOTE_ATOMIC; /* 必须显式开启远程原子操作权限 */
    flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc) {
        fprintf(stderr, "[Error] Failed to modify QP to INIT: %s (ret %d)\n", strerror(rc), rc);
        return -1;
    }

    /* 2. INIT -> RTR (带 RoCE GRH/GID 配置) */
    memset(&attr, 0, sizeof(attr));
    attr.qp_state              = IBV_QPS_RTR;
    attr.path_mtu              = IBV_MTU_1024;
    attr.dest_qp_num           = dest->qpn;
    attr.rq_psn                = dest->psn;
    attr.max_dest_rd_atomic    = 1;
    attr.min_rnr_timer         = 12;

    /* Soft-RoCE (rdma_rxe) 必须显式开启 GRH 并设置 GID */
    attr.ah_attr.is_global     = 1;
    attr.ah_attr.grh.dgid      = dest->gid;
    attr.ah_attr.grh.sgid_index= sgid_index;
    attr.ah_attr.grh.hop_limit = 1;
    attr.ah_attr.dlid          = dest->lid;
    attr.ah_attr.sl            = 0;
    attr.ah_attr.port_num      = 1;

    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
            IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc) {
        fprintf(stderr, "[Error] Failed to modify QP to RTR: %s (ret %d)\n", strerror(rc), rc);
        return -1;
    }

 /* 3. RTR -> RTS */
    memset(&attr, 0, sizeof(attr));
    attr.qp_state      = IBV_QPS_RTS;
    attr.timeout       = 14; // 重传超时时间 (4.096us * 2^14 ≈ 67ms)
    attr.retry_cnt     = 7;  // 传输失败最大重试次数
    attr.rnr_retry     = 7;  // 接收端未准备好(RNR)时的重试次数
    attr.sq_psn        = 0;
    attr.max_rd_atomic = 1;

    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc) {
        fprintf(stderr, "[Error] Failed to modify QP to RTS: %s (ret %d)\n", strerror(rc), rc);
        return -1;
    }

    return 0;
}

int main(int argc, char *argv[]) {
    int is_server = (argc == 1);
    char *server_ip = is_server ? NULL : argv[1];
    int sock_fd;
    struct context ctx = {0};
    struct ibv_device **dev_list;
    struct ibv_port_attr port_attr;
    struct ibv_device_attr dev_attr;
    struct rdma_dest local_dest, remote_dest;
    int sgid_index = 1; // Soft-RoCE 默认使用 sgid_index 1

    // 1. 获取并打开 RDMA 设备
    dev_list = ibv_get_device_list(NULL);
    if (!dev_list || !dev_list[0]) {
        fprintf(stderr, "[Error] No RDMA devices found! Please check rdma link.\n");
        return 1;
    }

    ctx.ctx = ibv_open_device(dev_list[0]);
    if (!ctx.ctx) {
        fprintf(stderr, "[Error] Failed to open device %s\n", dev_list[0]->name);
        return 1;
    }
    printf("[Info] Using RDMA Device: %s\n", dev_list[0]->name);
    ibv_free_device_list(dev_list);

    // 2. 查询设备 Capability (校验是否支持 Atomic)
    if (ibv_query_device(ctx.ctx, &dev_attr)) {
        fprintf(stderr, "[Error] Failed to query device attributes\n");
        return 1;
    }
    if (dev_attr.atomic_cap == IBV_ATOMIC_NONE) {
        fprintf(stderr, "[Warning] Device atomic_cap == IBV_ATOMIC_NONE! Post send may fail.\n");
    } else {
        printf("[Info] Device Atomic Capability level: %d\n", dev_attr.atomic_cap);
    }

    // 3. 创建 PD & CQ
    ctx.pd = ibv_alloc_pd(ctx.ctx);
    if (!ctx.pd) {
        fprintf(stderr, "[Error] Failed to allocate Protection Domain\n");
        return 1;
    }

    ctx.cq = ibv_create_cq(ctx.ctx, 10, NULL, NULL, 0);
    if (!ctx.cq) {
        fprintf(stderr, "[Error] Failed to create CQ\n");
        return 1;
    }

    // 4. 分配 8 字节严格对齐的内存
    if (posix_memalign((void **)&ctx.buf, 8, sizeof(uint64_t)) != 0) {
        fprintf(stderr, "[Error] Failed to allocate aligned memory\n");
        return 1;
    }

    // 5. 注册 Memory Region
    int mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    if (is_server) {
        mr_flags |= IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
        *ctx.buf = 100; // Server 初始化目标数据为 100
        printf("[Server] Initialized target memory value = %lu\n", *ctx.buf);
    } else {
        *ctx.buf = 0;   // Client 准备接收旧值
    }

    ctx.mr = ibv_reg_mr(ctx.pd, ctx.buf, sizeof(uint64_t), mr_flags);
    if (!ctx.mr) {
        fprintf(stderr, "[Error] Failed to register MR: %s (errno %d)\n", strerror(errno), errno);
        return 1;
    }

    // 6. 创建 RC 类型的 Queue Pair
    struct ibv_qp_init_attr qp_init_attr = {
        .send_cq = ctx.cq,
        .recv_cq = ctx.cq,
        .cap     = { .max_send_wr = 10, .max_recv_wr = 10, .max_send_sge = 1, .max_recv_sge = 1 },
        .qp_type = IBV_QPT_RC
    };
    ctx.qp = ibv_create_qp(ctx.pd, &qp_init_attr);
    if (!ctx.qp) {
        fprintf(stderr, "[Error] Failed to create QP: %s (errno %d)\n", strerror(errno), errno);
        return 1;
    }

    // 7. 查询 Port LID & GID
    if (ibv_query_port(ctx.ctx, 1, &port_attr)) {
        fprintf(stderr, "[Error] Failed to query port 1\n");
        return 1;
    }

    if (ibv_query_gid(ctx.ctx, 1, sgid_index, &local_dest.gid)) {
        fprintf(stderr, "[Error] Failed to query GID at index %d\n", sgid_index);
        return 1;
    }

    local_dest.lid  = port_attr.lid;
    local_dest.qpn  = ctx.qp->qp_num;
    local_dest.psn  = 0;
    local_dest.addr = (uint64_t)(uintptr_t)ctx.buf;
    local_dest.rkey = ctx.mr->rkey;

    // 8. TCP Socket 建连与交换元数据
    if (is_server) {
        int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        int opt = 1;
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        struct sockaddr_in serv_addr = { .sin_family = AF_INET, .sin_port = htons(PORT), .sin_addr.s_addr = INADDR_ANY };
        if (bind(listen_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            fprintf(stderr, "[Error] Socket bind failed: %s\n", strerror(errno));
            return 1;
        }
        listen(listen_fd, 1);
        printf("[Server] Waiting for Client connection on TCP port %d...\n", PORT);
        sock_fd = accept(listen_fd, NULL, NULL);
        close(listen_fd);
    } else {
        sock_fd = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in serv_addr = { .sin_family = AF_INET, .sin_port = htons(PORT) };
        inet_pton(AF_INET, server_ip, &serv_addr.sin_addr);
        if (connect(sock_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            fprintf(stderr, "[Error] Connect to %s failed: %s\n", server_ip, strerror(errno));
            return 1;
        }
    }

    if (sock_sync_data(sock_fd, sizeof(struct rdma_dest), (char *)&local_dest, (char *)&remote_dest)) {
        fprintf(stderr, "[Error] Failed to exchange metadata over socket\n");
        return 1;
    }

    // 9. 将 QP 转换至 RTS 状态 (必须校验返回值)
    if (modify_qp_to_rts(ctx.qp, &remote_dest, sgid_index)) {
        fprintf(stderr, "[Error] Aborting due to QP status transit failure.\n");
        return 1;
    }
    printf("[Info] QP transit to RTS successful.\n");

    // 10. 执行 CAS 操作 (Client 发起)
    if (!is_server) {
        struct ibv_sge sge = {
            .addr   = (uint64_t)(uintptr_t)ctx.buf,
            .length = sizeof(uint64_t), // 必须 8 字节
            .lkey   = ctx.mr->lkey
        };

        struct ibv_send_wr wr = {
            .wr_id      = 1001,
            .sg_list    = &sge,
            .num_sge    = 1,
            .opcode     = IBV_WR_ATOMIC_CMP_AND_SWP,
            .send_flags = IBV_SEND_SIGNALED,
            .wr.atomic  = {
                .remote_addr = remote_dest.addr,
                .rkey        = remote_dest.rkey,
                .compare_add = 100, // 期望 Server 当前值为 100
                .swap        = 200  // 匹配成功时将 Server 值修改为 200
            }
        };

        struct ibv_send_wr *bad_wr = NULL;
        printf("[Client] Posting CAS request: Compare=100, Swap=200...\n");
        
        int post_ret = ibv_post_send(ctx.qp, &wr, &bad_wr);
        if (post_ret != 0) {
            /* 关键错误排查日志 */
            fprintf(stderr, "[Error] ibv_post_send failed with ret=%d (%s), errno=%d (%s)\n", 
                    post_ret, strerror(post_ret), errno, strerror(errno));
            if (bad_wr) {
                fprintf(stderr, "[Error] Bad WR Opcode: %d, WR ID: %lu\n", bad_wr->opcode, bad_wr->wr_id);
            }
            return 1;
        }

        // 轮询 CQ 等待完成
        struct ibv_wc wc;
        int poll_count = 0;
        while ((poll_count = ibv_poll_cq(ctx.cq, 1, &wc)) == 0);

        if (poll_count < 0) {
            fprintf(stderr, "[Error] ibv_poll_cq error: %d\n", poll_count);
            return 1;
        }

        if (wc.status == IBV_WC_SUCCESS) {
            printf("[Client] CAS Work Completion SUCCESS!\n");
            printf("[Client] Returned previous value from Server = %lu\n", *ctx.buf);
            if (*ctx.buf == 100) {
                printf("[Client] SUCCESS: Server value was 100, successfully swapped to 200.\n");
            } else {
                printf("[Client] MISMATCH: Server value was %lu, no swap occurred.\n", *ctx.buf);
            }
        } else {
            fprintf(stderr, "[Error] CAS Completion Failed with Status: %s (%d), vendor_err=0x%x\n",
                    ibv_wc_status_str(wc.status), wc.status, wc.vendor_err);
        }

        write(sock_fd, "done", 5);
    } else {
        char ack[10];
        read(sock_fd, ack, sizeof(ack));
        printf("[Server] Final target memory value after Client's CAS = %lu\n", *ctx.buf);
    }

    // 清理资源
    close(sock_fd);
    ibv_destroy_qp(ctx.qp);
    ibv_destroy_cq(ctx.cq);
    ibv_dereg_mr(ctx.mr);
    ibv_dealloc_pd(ctx.pd);
    ibv_close_device(ctx.ctx);
    free(ctx.buf);

    return 0;
}
