/*
 * Copyright (c) 2005 Ammasso, Inc. All rights reserved.
 * Copyright (c) 2006 Open Grid Computing, Inc. All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * OpenIB.org BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
#define _GNU_SOURCE
#include <endian.h>
#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <sys/poll.h>
#include <sys/eventfd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <semaphore.h>
#include <pthread.h>
#include <inttypes.h>
#include <rdma/rdma_cma.h>
#include "common.h"

static int debug = 1;
#define DEBUG_LOG if (debug) printf

#define RPING_WR_ID_CONTROL 0x100
#define RPING_WR_ID_DATA    0x200
/*
 * rping "ping/pong" loop (RDMA_WRITE_WITH_IMM 改造版):
 * 	1. client 在 connect 时将本地 recv_buf 的 rkey/addr 作为 CM private_data 发送给 server
 * 	2. server 在 accept 时将本地 recv_buf 的 rkey/addr 作为 CM private_data 发送给 client
 * 	3. client 使用 RDMA_WRITE_WITH_IMM (带 imm_data=RDMA_READ_ADV) 将 source rkey/addr/len 写入 server recv_buf
 * 	4. server 收到 IBV_WC_RECV_RDMA_WITH_IMM 事件，读取通知，发起 RDMA Read
 * 	5. server 完成 RDMA Read 后，使用 RDMA_WRITE_WITH_IMM (带 imm_data=RDMA_WRITE_ADV) 通知 client
 * 	6. client 收到通知后，使用 RDMA_WRITE_WITH_IMM (带 imm_data=RDMA_WRITE_ADV) 将 sink rkey/addr/len 写入 server recv_buf
 * 	7. server 发起 RDMA Write 回传 pong 数据
 * 	8. server 完成 RDMA Write 后，使用 RDMA_WRITE_WITH_IMM (带 imm_data=RDMA_WRITE_COMPLETE) 通知 client
 * 	<repeat loop>
 */

enum test_state {
	IDLE = 1,
	CONNECT_REQUEST,
	ADDR_RESOLVED,
	ROUTE_RESOLVED,
	CONNECTED,
	RDMA_READ_ADV,
	RDMA_READ_COMPLETE,
	RDMA_WRITE_ADV,
	RDMA_WRITE_COMPLETE,
	DISCONNECTED,
	ERROR
};

struct rping_rdma_info {
	__be64 buf;
	__be32 rkey;
	__be32 size;
};

#define RPING_BUFSIZE 64*1024
#define RPING_SQ_DEPTH 16

#define _stringify( _x ) # _x
#define stringify( _x ) _stringify( _x )

#define RPING_MSG_FMT           "rdma-ping-%d: "
#define RPING_MIN_BUFSIZE       sizeof(stringify(INT_MAX)) + sizeof(RPING_MSG_FMT)

struct rping_cb {
	int server;			/* 0 if client */
	pthread_t cqthread;
	pthread_t persistent_server_thread;
	struct ibv_comp_channel *channel;
	struct ibv_cq *cq;
	struct ibv_pd *pd;
	struct ibv_qp *qp;

	struct ibv_recv_wr rq_wr;	/* recv work request record */
	struct ibv_sge recv_sgl;	/* recv single SGE */
	struct rping_rdma_info recv_buf;/* malloc'd buffer */
	struct ibv_mr *recv_mr;		/* MR associated with this buffer */

	struct ibv_send_wr sq_wr;	/* send work request record */
	struct ibv_sge send_sgl;
	struct rping_rdma_info send_buf;/* single send buf */
	struct ibv_mr *send_mr;

	struct ibv_send_wr rdma_sq_wr;	/* rdma work request record */
	struct ibv_sge rdma_sgl;	/* rdma single SGE */
	char *rdma_buf;			/* used as rdma sink */
	struct ibv_mr *rdma_mr;

	uint32_t remote_rkey;		/* remote guys RKEY for data buf */
	uint64_t remote_addr;		/* remote guys TO for data buf */
	uint32_t remote_len;		/* remote guys LEN for data buf */

	uint64_t remote_recv_addr;	/* 对端 recv_buf 的物理/虚拟地址 */
	uint32_t remote_recv_rkey;	/* 对端 recv_buf 的 RKEY */

	char *start_buf;		/* rdma read src */
	struct ibv_mr *start_mr;

	enum test_state state;		/* used for cond/signalling */
	sem_t sem;
	sem_t accept_ready;		/* Ready for another conn req */

	struct sockaddr_storage sin;
	struct sockaddr_storage ssource;
	__be16 port;			/* dst port in NBO */
	int verbose;			/* verbose logging */
	int self_create_qp;		/* Create QP not via cma */
	int count;			/* ping count */
	int size;			/* ping data size */
	int validate;			/* validate ping data */

	/* CM stuff */
	int eventfd;
	pthread_t cmthread;
	struct rdma_event_channel *cm_channel;
	struct rdma_cm_id *cm_id;	/* connection on client side,*/
					/* listener on service side. */
	struct rdma_cm_id *child_cm_id;	/* connection on server side */
};

static int rping_cma_event_handler(struct rdma_cm_id *cma_id,
				    struct rdma_cm_event *event)
{
	int ret = 0;
	struct rping_cb *cb = cma_id->context;

	DEBUG_LOG("cma_event type %s cma_id %p (%s)\n",
		  rdma_event_str(event->event), cma_id,
		  (cma_id == cb->cm_id) ? "parent" : "child");

	switch (event->event) {
	case RDMA_CM_EVENT_ADDR_RESOLVED:
		cb->state = ADDR_RESOLVED;
		ret = rdma_resolve_route(cma_id, 2000);
		if (ret) {
			cb->state = ERROR;
			perror("rdma_resolve_route");
			sem_post(&cb->sem);
		}
		break;

	case RDMA_CM_EVENT_ROUTE_RESOLVED:
		cb->state = ROUTE_RESOLVED;
		sem_post(&cb->sem);
		break;

	case RDMA_CM_EVENT_CONNECT_REQUEST:
		sem_wait(&cb->accept_ready);
		cb->state = CONNECT_REQUEST;
		cb->child_cm_id = cma_id;
		DEBUG_LOG("child cma %p\n", cb->child_cm_id);

		/* 提取 Client 在 private_data 中带过来的 recv_buf 信息 */
		if (event->param.conn.private_data &&
		    event->param.conn.private_data_len >= sizeof(struct rping_rdma_info)) {
			struct rping_rdma_info *info = (struct rping_rdma_info *)event->param.conn.private_data;
			cb->remote_recv_addr = be64toh(info->buf);
			cb->remote_recv_rkey = be32toh(info->rkey);
			DEBUG_LOG("Server recorded client recv_buf addr %" PRIx64 " rkey %x\n",
				  cb->remote_recv_addr, cb->remote_recv_rkey);
		}
		sem_post(&cb->sem);
		break;

	case RDMA_CM_EVENT_CONNECT_RESPONSE:
		DEBUG_LOG("CONNECT_RESPONSE\n");
		if (event->param.conn.private_data &&
		    event->param.conn.private_data_len >= sizeof(struct rping_rdma_info)) {
			struct rping_rdma_info *info = (struct rping_rdma_info *)event->param.conn.private_data;
			cb->remote_recv_addr = be64toh(info->buf);
			cb->remote_recv_rkey = be32toh(info->rkey);
			DEBUG_LOG("Client recorded server recv_buf addr %" PRIx64 " rkey %x\n",
				  cb->remote_recv_addr, cb->remote_recv_rkey);
		}
		cb->state = CONNECTED;
		sem_post(&cb->sem);
		break;

	case RDMA_CM_EVENT_ESTABLISHED:
		DEBUG_LOG("ESTABLISHED\n");
		if (event->param.conn.private_data &&
		    event->param.conn.private_data_len >= sizeof(struct rping_rdma_info)) {
			struct rping_rdma_info *info = (struct rping_rdma_info *)event->param.conn.private_data;
			cb->remote_recv_addr = be64toh(info->buf);
			cb->remote_recv_rkey = be32toh(info->rkey);
			DEBUG_LOG("Client recorded server recv_buf addr %" PRIx64 " rkey %x\n",
				  cb->remote_recv_addr, cb->remote_recv_rkey);
		}
		if (!cb->server) {
			cb->state = CONNECTED;
		}
		sem_post(&cb->sem);
		break;

	case RDMA_CM_EVENT_ADDR_ERROR:
	case RDMA_CM_EVENT_ROUTE_ERROR:
	case RDMA_CM_EVENT_CONNECT_ERROR:
	case RDMA_CM_EVENT_UNREACHABLE:
	case RDMA_CM_EVENT_REJECTED:
		fprintf(stderr, "cma event %s, error %d\n",
			rdma_event_str(event->event), event->status);
		sem_post(&cb->sem);
		ret = -1;
		break;

	case RDMA_CM_EVENT_DISCONNECTED:
		fprintf(stderr, "%s DISCONNECT EVENT...\n",
			cb->server ? "server" : "client");
		cb->state = DISCONNECTED;
		sem_post(&cb->sem);
		break;

	case RDMA_CM_EVENT_DEVICE_REMOVAL:
		fprintf(stderr, "cma detected device removal!!!!\n");
		cb->state = ERROR;
		sem_post(&cb->sem);
		ret = -1;
		break;

	default:
		fprintf(stderr, "unhandled event: %s, ignoring\n",
			rdma_event_str(event->event));
		break;
	}

	return ret;
}

static int server_recv(struct rping_cb *cb, struct ibv_wc *wc)
{
	uint32_t imm = be32toh(wc->imm_data);

	/* 如果带过来的立即数通知表明包含了内存描述信息 */
	if (imm == RDMA_READ_ADV || imm == RDMA_WRITE_ADV) {
		cb->remote_rkey = be32toh(cb->recv_buf.rkey);
		cb->remote_addr = be64toh(cb->recv_buf.buf);
		cb->remote_len  = be32toh(cb->recv_buf.size);
		DEBUG_LOG("Received rkey %x addr %" PRIx64 " len %d from peer via RDMA_WRITE_WITH_IMM\n",
			  cb->remote_rkey, cb->remote_addr, cb->remote_len);
	}

	cb->state = (enum test_state)imm;
	return 0;
}

static int client_recv(struct rping_cb *cb, struct ibv_wc *wc)
{
	uint32_t imm = be32toh(wc->imm_data);
	cb->state = (enum test_state)imm;
	return 0;
}

static int rping_cq_event_handler(struct rping_cb *cb)
{
	struct ibv_wc wc;
	struct ibv_recv_wr *bad_wr;
	int ret;
	int flushed = 0;

	while ((ret = ibv_poll_cq(cb->cq, 1, &wc)) == 1) {
		ret = 0;

		if (wc.status) {
			if (wc.status == IBV_WC_WR_FLUSH_ERR) {
				flushed = 1;
				continue;
			}
			fprintf(stderr,
				"cq completion failed status %d\n",
				wc.status);
			ret = -1;
			goto error;
		}

		switch (wc.opcode) {
		case IBV_WC_SEND:
			DEBUG_LOG("send completion\n");
			break;

		case IBV_WC_RDMA_WRITE:
			DEBUG_LOG("rdma write completion, wr_id 0x%" PRIx64 "\n", wc.wr_id);
			/* 只有数据 RDMA Write 完成时才更新状态并唤醒主线程 */
			if (wc.wr_id == RPING_WR_ID_DATA) {
				cb->state = RDMA_WRITE_COMPLETE;
				sem_post(&cb->sem);
			}
			break;

		case IBV_WC_RDMA_READ:
			DEBUG_LOG("rdma read completion\n");
			cb->state = RDMA_READ_COMPLETE;
			sem_post(&cb->sem);
			break;

		case IBV_WC_RECV_RDMA_WITH_IMM:
			DEBUG_LOG("recv rdma with imm completion, imm = %u\n", be32toh(wc.imm_data));
			ret = cb->server ? server_recv(cb, &wc) :
					   client_recv(cb, &wc);
			if (ret) {
				fprintf(stderr, "recv wc error: %d\n", ret);
				goto error;
			}

			/* 重新挂载 Recv WR 以准备接收下一次通知 */
			ret = ibv_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
			if (ret) {
				fprintf(stderr, "post recv error: %d\n", ret);
				goto error;
			}
			sem_post(&cb->sem);
			break;

		default:
			DEBUG_LOG("unknown completion opcode %d\n", wc.opcode);
			ret = -1;
			goto error;
		}
	}
	if (ret) {
		fprintf(stderr, "poll error %d\n", ret);
		goto error;
	}
	return flushed;

error:
	cb->state = ERROR;
	sem_post(&cb->sem);
	return ret;
}

static void rping_init_conn_param(struct rping_cb *cb,
				  struct rdma_conn_param *conn_param)
{
	memset(conn_param, 0, sizeof(*conn_param));
	conn_param->responder_resources = 1;
	conn_param->initiator_depth = 1;
	conn_param->retry_count = 7;
	conn_param->rnr_retry_count = 7;
	if (cb->self_create_qp)
		conn_param->qp_num = cb->qp->qp_num;
}

static int rping_self_modify_qp(struct rping_cb *cb, struct rdma_cm_id *id)
{
	struct ibv_qp_attr qp_attr;
	int qp_attr_mask, ret;

	qp_attr.qp_state = IBV_QPS_INIT;
	ret = rdma_init_qp_attr(id, &qp_attr, &qp_attr_mask);
	if (ret)
		return ret;

	ret = ibv_modify_qp(cb->qp, &qp_attr, qp_attr_mask);
	if (ret)
		return ret;

	qp_attr.qp_state = IBV_QPS_RTR;
	ret = rdma_init_qp_attr(id, &qp_attr, &qp_attr_mask);
	if (ret)
		return ret;

	ret = ibv_modify_qp(cb->qp, &qp_attr, qp_attr_mask);
	if (ret)
		return ret;

	qp_attr.qp_state = IBV_QPS_RTS;
	ret = rdma_init_qp_attr(id, &qp_attr, &qp_attr_mask);
	if (ret)
		return ret;

	return ibv_modify_qp(cb->qp, &qp_attr, qp_attr_mask);
}

static int rping_accept(struct rping_cb *cb)
{
	struct rdma_conn_param conn_param;
	struct rping_rdma_info send_info;
	int ret;

	DEBUG_LOG("accepting client connection request\n");

	/* 打包本地 recv_buf 区域信息传递给 Client */
	memset(&send_info, 0, sizeof(send_info));
	send_info.buf = htobe64((uint64_t)(uintptr_t)&cb->recv_buf);
	send_info.rkey = htobe32(cb->recv_mr->rkey);
	send_info.size = htobe32(sizeof(cb->recv_buf));

	rping_init_conn_param(cb, &conn_param);
	conn_param.private_data = &send_info;
	conn_param.private_data_len = sizeof(send_info);

	if (cb->self_create_qp) {
		ret = rping_self_modify_qp(cb, cb->child_cm_id);
		if (ret)
			return ret;
	}

	ret = rdma_accept(cb->child_cm_id, &conn_param);
	if (ret) {
		perror("rdma_accept");
		return ret;
	}

	sem_wait(&cb->sem);
	if (cb->state == ERROR) {
		fprintf(stderr, "wait for CONNECTED state %d\n", cb->state);
		return -1;
	}
	return 0;
}

static int rping_disconnect(struct rping_cb *cb, struct rdma_cm_id *id)
{
	struct ibv_qp_attr qp_attr = {};
	int err = 0;

	if (cb->self_create_qp) {
		qp_attr.qp_state = IBV_QPS_ERR;
		err = ibv_modify_qp(cb->qp, &qp_attr, IBV_QP_STATE);
		if (err)
			return err;
	}

	return rdma_disconnect(id);
}

static void rping_setup_wr(struct rping_cb *cb)
{
	cb->recv_sgl.addr = (uint64_t) (unsigned long) &cb->recv_buf;
	cb->recv_sgl.length = sizeof cb->recv_buf;
	cb->recv_sgl.lkey = cb->recv_mr->lkey;
	cb->rq_wr.sg_list = &cb->recv_sgl;
	cb->rq_wr.num_sge = 1;

	cb->send_sgl.addr = (uint64_t) (unsigned long) &cb->send_buf;
	cb->send_sgl.length = sizeof cb->send_buf;
	cb->send_sgl.lkey = cb->send_mr->lkey;

	cb->sq_wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
	cb->sq_wr.send_flags = IBV_SEND_SIGNALED;
	cb->sq_wr.sg_list = &cb->send_sgl;
	cb->sq_wr.num_sge = 1;
	cb->sq_wr.wr_id = RPING_WR_ID_CONTROL; /* 标注为控制通知 WR */

	cb->rdma_sgl.addr = (uint64_t) (unsigned long) cb->rdma_buf;
	cb->rdma_sgl.lkey = cb->rdma_mr->lkey;
	cb->rdma_sq_wr.send_flags = IBV_SEND_SIGNALED;
	cb->rdma_sq_wr.sg_list = &cb->rdma_sgl;
	cb->rdma_sq_wr.num_sge = 1;
	cb->rdma_sq_wr.wr_id = RPING_WR_ID_DATA; /* 标注为数据 Write/Read WR */
}

static int rping_setup_buffers(struct rping_cb *cb)
{
	int ret;

	DEBUG_LOG("rping_setup_buffers called on cb %p\n", cb);
	/* 关键修改：因为 recv_buf 会被远端执行 RDMA Write，因此必须赋予 IBV_ACCESS_REMOTE_WRITE 权限 */
	cb->recv_mr = ibv_reg_mr(cb->pd, &cb->recv_buf, sizeof cb->recv_buf,
				 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
	if (!cb->recv_mr) {
		fprintf(stderr, "recv_buf reg_mr failed\n");
		return errno;
	}

	cb->send_mr = ibv_reg_mr(cb->pd, &cb->send_buf, sizeof cb->send_buf, 0);
	if (!cb->send_mr) {
		fprintf(stderr, "send_buf reg_mr failed\n");
		ret = errno;
		goto err1;
	}

	cb->rdma_buf = malloc(cb->size);
	if (!cb->rdma_buf) {
		fprintf(stderr, "rdma_buf malloc failed\n");
		ret = -ENOMEM;
		goto err2;
	}

	cb->rdma_mr = ibv_reg_mr(cb->pd, cb->rdma_buf, cb->size,
				 IBV_ACCESS_LOCAL_WRITE |
				 IBV_ACCESS_REMOTE_READ |
				 IBV_ACCESS_REMOTE_WRITE);
	if (!cb->rdma_mr) {
		fprintf(stderr, "rdma_buf reg_mr failed\n");
		ret = errno;
		goto err3;
	}

	if (!cb->server) {
		cb->start_buf = malloc(cb->size);
		if (!cb->start_buf) {
			fprintf(stderr, "start_buf malloc failed\n");
			ret = -ENOMEM;
			goto err4;
		}

		cb->start_mr = ibv_reg_mr(cb->pd, cb->start_buf, cb->size,
					  IBV_ACCESS_LOCAL_WRITE | 
					  IBV_ACCESS_REMOTE_READ |
					  IBV_ACCESS_REMOTE_WRITE);
		if (!cb->start_mr) {
			fprintf(stderr, "start_buf reg_mr failed\n");
			ret = errno;
			goto err5;
		}
	}

	rping_setup_wr(cb);
	DEBUG_LOG("allocated & registered buffers...\n");
	return 0;

err5:
	free(cb->start_buf);
err4:
	ibv_dereg_mr(cb->rdma_mr);
err3:
	free(cb->rdma_buf);
err2:
	ibv_dereg_mr(cb->send_mr);
err1:
	ibv_dereg_mr(cb->recv_mr);
	return ret;
}

static void rping_free_buffers(struct rping_cb *cb)
{
	DEBUG_LOG("rping_free_buffers called on cb %p\n", cb);
	ibv_dereg_mr(cb->recv_mr);
	ibv_dereg_mr(cb->send_mr);
	ibv_dereg_mr(cb->rdma_mr);
	free(cb->rdma_buf);
	if (!cb->server) {
		ibv_dereg_mr(cb->start_mr);
		free(cb->start_buf);
	}
}

static int rping_create_qp(struct rping_cb *cb)
{
	struct ibv_qp_init_attr init_attr;
	struct rdma_cm_id *id;
	int ret;

	memset(&init_attr, 0, sizeof(init_attr));
	init_attr.cap.max_send_wr = RPING_SQ_DEPTH;
	init_attr.cap.max_recv_wr = 2;
	init_attr.cap.max_recv_sge = 1;
	init_attr.cap.max_send_sge = 1;
	init_attr.qp_type = IBV_QPT_RC;
	init_attr.send_cq = cb->cq;
	init_attr.recv_cq = cb->cq;
	id = cb->server ? cb->child_cm_id : cb->cm_id;

	if (cb->self_create_qp) {
		cb->qp = ibv_create_qp(cb->pd, &init_attr);
		if (!cb->qp) {
			perror("ibv_create_qp");
			return -1;
		}

		struct ibv_qp_attr attr = {
			.qp_state = IBV_QPS_INIT,
			.pkey_index = 0,
			.port_num = id->port_num,
			.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE,
		};

		ret = ibv_modify_qp(cb->qp, &attr,
				    IBV_QP_STATE | IBV_QP_PKEY_INDEX |
				    IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);

		if (ret) {
			perror("ibv_modify_qp");
			ibv_destroy_qp(cb->qp);
		}
		return ret ? -1 : 0;
	}

	ret = rdma_create_qp(id, cb->pd, &init_attr);
	if (!ret)
		cb->qp = id->qp;
	else
		perror("rdma_create_qp");
	return ret;
}

static void rping_free_qp(struct rping_cb *cb)
{
	ibv_destroy_qp(cb->qp);
	ibv_destroy_cq(cb->cq);
	ibv_destroy_comp_channel(cb->channel);
	ibv_dealloc_pd(cb->pd);
}

static int rping_setup_qp(struct rping_cb *cb, struct rdma_cm_id *cm_id)
{
	int ret;

	cb->pd = ibv_alloc_pd(cm_id->verbs);
	if (!cb->pd) {
		fprintf(stderr, "ibv_alloc_pd failed\n");
		return errno;
	}
	DEBUG_LOG("created pd %p\n", cb->pd);

	cb->channel = ibv_create_comp_channel(cm_id->verbs);
	if (!cb->channel) {
		fprintf(stderr, "ibv_create_comp_channel failed\n");
		ret = errno;
		goto err1;
	}
	DEBUG_LOG("created channel %p\n", cb->channel);

	cb->cq = ibv_create_cq(cm_id->verbs, RPING_SQ_DEPTH * 2, cb,
				cb->channel, 0);
	if (!cb->cq) {
		fprintf(stderr, "ibv_create_cq failed\n");
		ret = errno;
		goto err2;
	}
	DEBUG_LOG("created cq %p\n", cb->cq);

	ret = ibv_req_notify_cq(cb->cq, 0);
	if (ret) {
		fprintf(stderr, "ibv_create_cq failed\n");
		ret = errno;
		goto err3;
	}

	ret = rping_create_qp(cb);
	if (ret) {
		goto err3;
	}
	DEBUG_LOG("created qp %p\n", cb->qp);
	return 0;

err3:
	ibv_destroy_cq(cb->cq);
err2:
	ibv_destroy_comp_channel(cb->channel);
err1:
	ibv_dealloc_pd(cb->pd);
	return ret;
}

static void *cm_thread(void *arg)
{
	struct rping_cb *cb = arg;
	struct rdma_cm_event *event;
	struct pollfd pfds[2];
	int ret;

	pfds[0].fd = cb->eventfd;
	pfds[0].events = POLLIN;
	pfds[1].fd = cb->cm_channel->fd;
	pfds[1].events = POLLIN;

	while (1) {
		ret = poll(pfds, 2, -1);
		if (ret == -1 && errno != EINTR) {
			perror("poll failed");
			exit(ret);
		} else if (ret < 1)
			continue;

		if (pfds[0].revents & POLLIN)
			return NULL;

		if (pfds[1].revents & POLLIN) {
			ret = rdma_get_cm_event(cb->cm_channel, &event);
			if (ret) {
				perror("rdma_get_cm_event");
				exit(ret);
			}
			ret = rping_cma_event_handler(event->id, event);
			rdma_ack_cm_event(event);
			if (ret)
				exit(ret);
		}
	}
}

static void *cq_thread(void *arg)
{
	struct rping_cb *cb = arg;
	struct ibv_cq *ev_cq;
	void *ev_ctx;
	int ret;
	
	DEBUG_LOG("cq_thread started.\n");

	while (1) {	
		pthread_testcancel();

		ret = ibv_get_cq_event(cb->channel, &ev_cq, &ev_ctx);
		if (ret) {
			fprintf(stderr, "Failed to get cq event!\n");
			pthread_exit(NULL);
		}
		if (ev_cq != cb->cq) {
			fprintf(stderr, "Unknown CQ!\n");
			pthread_exit(NULL);
		}
		ret = ibv_req_notify_cq(cb->cq, 0);
		if (ret) {
			fprintf(stderr, "Failed to set notify!\n");
			pthread_exit(NULL);
		}
		ret = rping_cq_event_handler(cb);
		ibv_ack_cq_events(cb->cq, 1);
		if (ret)
			pthread_exit(NULL);
	}
}

static void rping_format_send(struct rping_cb *cb, char *buf, struct ibv_mr *mr)
{
	struct rping_rdma_info *info = &cb->send_buf;

	info->buf = htobe64((uint64_t) (unsigned long) buf);
	info->rkey = htobe32(mr->rkey);
	info->size = htobe32(cb->size);

	DEBUG_LOG("RDMA addr %" PRIx64" rkey %x len %d\n",
		  be64toh(info->buf), be32toh(info->rkey), be32toh(info->size));
}

static int rping_test_server(struct rping_cb *cb)
{
	struct ibv_send_wr *bad_wr;
	int ret;

	while (1) {
		/* 等待 Client 通过 RDMA_WRITE_WITH_IMM 发送 Start STAG/TO/Len */
		sem_wait(&cb->sem);
		if (cb->state != RDMA_READ_ADV) {
			fprintf(stderr, "wait for RDMA_READ_ADV state %d\n",
				cb->state);
			ret = -1;
			break;
		}

		DEBUG_LOG("server received sink adv\n");

		/* 发起 RDMA Read */
		cb->rdma_sq_wr.opcode = IBV_WR_RDMA_READ;
		cb->rdma_sq_wr.wr.rdma.rkey = cb->remote_rkey;
		cb->rdma_sq_wr.wr.rdma.remote_addr = cb->remote_addr;
		cb->rdma_sq_wr.sg_list->length = cb->remote_len;

		ret = ibv_post_send(cb->qp, &cb->rdma_sq_wr, &bad_wr);
		if (ret) {
			fprintf(stderr, "post send error %d\n", ret);
			break;
		}
		DEBUG_LOG("server posted rdma read req \n");

		/* 等待 RDMA Read 完成 */
		sem_wait(&cb->sem);
		if (cb->state != RDMA_READ_COMPLETE) {
			fprintf(stderr, "wait for RDMA_READ_COMPLETE state %d\n",
				cb->state);
			ret = -1;
			break;
		}
		DEBUG_LOG("server received read complete\n");

		if (cb->verbose)
			printf("server ping data: %s\n", cb->rdma_buf);

		/* 使用 RDMA_WRITE_WITH_IMM 通知 Client 继续 (带入 imm_data = RDMA_WRITE_ADV) */
		cb->sq_wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
		cb->sq_wr.wr.rdma.remote_addr = cb->remote_recv_addr;
		cb->sq_wr.wr.rdma.rkey = cb->remote_recv_rkey;
		cb->sq_wr.imm_data = htobe32(RDMA_WRITE_ADV);
		cb->sq_wr.sg_list->length = 0;

		ret = ibv_post_send(cb->qp, &cb->sq_wr, &bad_wr);
		if (ret) {
			fprintf(stderr, "post send error %d\n", ret);
			break;
		}
		DEBUG_LOG("server posted go ahead via RDMA_WRITE_WITH_IMM\n");

		/* 等待 Client 通过 RDMA_WRITE_WITH_IMM 发送 Sink STAG/TO/Len */
		sem_wait(&cb->sem);
		if (cb->state != RDMA_WRITE_ADV) {
			fprintf(stderr, "wait for RDMA_WRITE_ADV state %d\n",
				cb->state);
			ret = -1;
			break;
		}
		DEBUG_LOG("server received sink adv\n");

		/* 执行 RDMA Write 回传 echo 数据 */
		cb->rdma_sq_wr.opcode = IBV_WR_RDMA_WRITE;
		cb->rdma_sq_wr.wr.rdma.rkey = cb->remote_rkey;
		cb->rdma_sq_wr.wr.rdma.remote_addr = cb->remote_addr;
		cb->rdma_sq_wr.sg_list->length = strlen(cb->rdma_buf) + 1;
		DEBUG_LOG("rdma write from lkey %x laddr %" PRIx64 " len %d\n",
			  cb->rdma_sq_wr.sg_list->lkey,
			  cb->rdma_sq_wr.sg_list->addr,
			  cb->rdma_sq_wr.sg_list->length);

		ret = ibv_post_send(cb->qp, &cb->rdma_sq_wr, &bad_wr);
		if (ret) {
			fprintf(stderr, "post send error %d\n", ret);
			break;
		}

		/* 等待 RDMA Write 本端完成 */
		ret = sem_wait(&cb->sem);
		if (cb->state != RDMA_WRITE_COMPLETE) {
			fprintf(stderr, "wait for RDMA_WRITE_COMPLETE state %d\n",
				cb->state);
			ret = -1;
			break;
		}
		DEBUG_LOG("server rdma write complete \n");

		/* 使用 RDMA_WRITE_WITH_IMM 通知 Client 本轮 ping 完成 (带入 imm_data = RDMA_WRITE_COMPLETE) */
		cb->sq_wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
		cb->sq_wr.wr.rdma.remote_addr = cb->remote_recv_addr;
		cb->sq_wr.wr.rdma.rkey = cb->remote_recv_rkey;
		cb->sq_wr.imm_data = htobe32(RDMA_WRITE_COMPLETE);
		cb->sq_wr.sg_list->length = 0;

		ret = ibv_post_send(cb->qp, &cb->sq_wr, &bad_wr);
		if (ret) {
			fprintf(stderr, "post send error %d\n", ret);
			break;
		}
		DEBUG_LOG("server posted go ahead via RDMA_WRITE_WITH_IMM\n");
	}

	return (cb->state == DISCONNECTED) ? 0 : ret;
}

static int rping_bind_server(struct rping_cb *cb)
{
	int ret;

	if (cb->sin.ss_family == AF_INET)
		((struct sockaddr_in *) &cb->sin)->sin_port = cb->port;
	else
		((struct sockaddr_in6 *) &cb->sin)->sin6_port = cb->port;

	ret = rdma_bind_addr(cb->cm_id, (struct sockaddr *) &cb->sin);
	if (ret) {
		perror("rdma_bind_addr");
		return ret;
	}
	DEBUG_LOG("rdma_bind_addr successful\n");

	DEBUG_LOG("rdma_listen\n");
	ret = rdma_listen(cb->cm_id, 3);
	if (ret) {
		perror("rdma_listen");
		return ret;
	}

	return 0;
}

static struct rping_cb *clone_cb(struct rping_cb *listening_cb)
{
	struct rping_cb *cb = malloc(sizeof *cb);
	if (!cb)
		return NULL;
	memset(cb, 0, sizeof *cb);
	*cb = *listening_cb;
	cb->child_cm_id->context = cb;
	return cb;
}

static void free_cb(struct rping_cb *cb)
{
	free(cb);
}

static void *rping_persistent_server_thread(void *arg)
{
	struct rping_cb *cb = arg;
	struct ibv_recv_wr *bad_wr;
	int ret;

	ret = rping_setup_qp(cb, cb->child_cm_id);
	if (ret) {
		fprintf(stderr, "setup_qp failed: %d\n", ret);
		goto err0;
	}

	ret = rping_setup_buffers(cb);
	if (ret) {
		fprintf(stderr, "rping_setup_buffers failed: %d\n", ret);
		goto err1;
	}

	ret = ibv_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
	if (ret) {
		fprintf(stderr, "ibv_post_recv failed: %d\n", ret);
		goto err2;
	}

	ret = pthread_create(&cb->cqthread, NULL, cq_thread, cb);
	if (ret) {
		perror("pthread_create");
		goto err2;
	}

	ret = rping_accept(cb);
	if (ret) {
		fprintf(stderr, "connect error %d\n", ret);
		goto err3;
	}

	rping_test_server(cb);
	rping_disconnect(cb, cb->child_cm_id);
	pthread_join(cb->cqthread, NULL);
	rping_free_buffers(cb);
	rping_free_qp(cb);
	rdma_destroy_id(cb->child_cm_id);
	free_cb(cb);
	return NULL;
err3:
	pthread_cancel(cb->cqthread);
	pthread_join(cb->cqthread, NULL);
err2:
	rping_free_buffers(cb);
err1:
	rping_free_qp(cb);
err0:
	free_cb(cb);
	return NULL;
}

static int rping_run_persistent_server(struct rping_cb *listening_cb)
{
	int ret;
	struct rping_cb *cb;
	pthread_attr_t attr;

	ret = rping_bind_server(listening_cb);
	if (ret)
		return ret;

	ret = pthread_attr_init(&attr);
	if (ret) {
		perror("pthread_attr_init");
		return ret;
	}
	ret = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	if (ret) {
		perror("pthread_attr_setdetachstate");
		return ret;
	}

	while (1) {
		sem_wait(&listening_cb->sem);
		if (listening_cb->state != CONNECT_REQUEST) {
			fprintf(stderr, "wait for CONNECT_REQUEST state %d\n",
				listening_cb->state);
			return -1;
		}

		cb = clone_cb(listening_cb);
		if (!cb)
			return -1;

		sem_post(&listening_cb->accept_ready);

		ret = pthread_create(&cb->persistent_server_thread, &attr, rping_persistent_server_thread, cb);
		if (ret) {
			perror("pthread_create");
			return ret;
		}
	}
	return 0;
}

static int rping_run_server(struct rping_cb *cb)
{
	struct ibv_recv_wr *bad_wr;
	int ret;

	ret = rping_bind_server(cb);
	if (ret)
		return ret;

	sem_wait(&cb->sem);
	if (cb->state != CONNECT_REQUEST) {
		fprintf(stderr, "wait for CONNECT_REQUEST state %d\n",
			cb->state);
		return -1;
	}

	ret = rping_setup_qp(cb, cb->child_cm_id);
	if (ret) {
		fprintf(stderr, "setup_qp failed: %d\n", ret);
		return ret;
	}

	ret = rping_setup_buffers(cb);
	if (ret) {
		fprintf(stderr, "rping_setup_buffers failed: %d\n", ret);
		goto err1;
	}

	ret = ibv_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
	if (ret) {
		fprintf(stderr, "ibv_post_recv failed: %d\n", ret);
		goto err2;
	}

	ret = pthread_create(&cb->cqthread, NULL, cq_thread, cb);
	if (ret) {
		perror("pthread_create");
		goto err2;
	}

	ret = rping_accept(cb);
	if (ret) {
		fprintf(stderr, "connect error %d\n", ret);
		goto err2;
	}

	ret = rping_test_server(cb);
	if (ret) {
		fprintf(stderr, "rping server failed: %d\n", ret);
		goto err3;
	}

	ret = 0;
err3:
	rping_disconnect(cb, cb->child_cm_id);
	pthread_join(cb->cqthread, NULL);
	rdma_destroy_id(cb->child_cm_id);
err2:
	rping_free_buffers(cb);
err1:
	rping_free_qp(cb);

	return ret;
}

static int rping_test_client(struct rping_cb *cb)
{
	int ping, start, cc, i, ret = 0;
	struct ibv_send_wr *bad_wr;
	unsigned char c;

	start = 65;
	for (ping = 0; !cb->count || ping < cb->count; ping++) {
		cb->state = RDMA_READ_ADV;

		cc = snprintf(cb->start_buf, cb->size, RPING_MSG_FMT, ping);
		for (i = cc, c = start; i < cb->size; i++) {
			cb->start_buf[i] = c;
			c++;
			if (c > 122)
				c = 65;
		}
		start++;
		if (start > 122)
			start = 65;
		cb->start_buf[cb->size - 1] = 0;

		/* 填充待传送的内存地址信息 */
		rping_format_send(cb, cb->start_buf, cb->start_mr);

		/* 使用 RDMA_WRITE_WITH_IMM 替代 IBV_WR_SEND 写入 Server 的 recv_buf，并附带 RDMA_READ_ADV 标志 */
		cb->sq_wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
		cb->sq_wr.wr.rdma.remote_addr = cb->remote_recv_addr;
		cb->sq_wr.wr.rdma.rkey = cb->remote_recv_rkey;
		cb->sq_wr.imm_data = htobe32(RDMA_READ_ADV);
		cb->sq_wr.sg_list->length = sizeof(cb->send_buf);

		ret = ibv_post_send(cb->qp, &cb->sq_wr, &bad_wr);
		if (ret) {
			fprintf(stderr, "post send error %d\n", ret);
			break;
		}

		/* 等待 Server 的 RDMA_WRITE_WITH_IMM 应答 */
		sem_wait(&cb->sem);
		if (cb->state != RDMA_WRITE_ADV) {
			fprintf(stderr, "wait for RDMA_WRITE_ADV state %d\n",
				cb->state);
			ret = -1;
			break;
		}

		/* 填充 sink 内存信息 */
		rping_format_send(cb, cb->rdma_buf, cb->rdma_mr);

		/* 使用 RDMA_WRITE_WITH_IMM 替代 IBV_WR_SEND 告知 Server 允许写入 sink */
		cb->sq_wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
		cb->sq_wr.wr.rdma.remote_addr = cb->remote_recv_addr;
		cb->sq_wr.wr.rdma.rkey = cb->remote_recv_rkey;
		cb->sq_wr.imm_data = htobe32(RDMA_WRITE_ADV);
		cb->sq_wr.sg_list->length = sizeof(cb->send_buf);

		ret = ibv_post_send(cb->qp, &cb->sq_wr, &bad_wr);
		if (ret) {
			fprintf(stderr, "post send error %d\n", ret);
			break;
		}

		/* 等待 Server 的 RDMA Write 完成通知 */
		sem_wait(&cb->sem);
		if (cb->state != RDMA_WRITE_COMPLETE) {
			fprintf(stderr, "wait for RDMA_WRITE_COMPLETE state %d\n",
				cb->state);
			ret = -1;
			break;
		}

		if (cb->validate)
			if (memcmp(cb->start_buf, cb->rdma_buf, cb->size)) {
				fprintf(stderr, "data mismatch!\n");
				ret = -1;
				break;
			}

		if (cb->verbose)
			printf("ping data: %s\n", cb->rdma_buf);
	}

	return (cb->state == DISCONNECTED) ? 0 : ret;
}

static int rping_connect_client(struct rping_cb *cb)
{
	struct rdma_conn_param conn_param;
	struct rping_rdma_info send_info;
	int ret;

	/* 将 Client 本地的 recv_buf 内存地址及 RKEY 打包进 private_data 传给 Server */
	memset(&send_info, 0, sizeof(send_info));
	send_info.buf = htobe64((uint64_t)(uintptr_t)&cb->recv_buf);
	send_info.rkey = htobe32(cb->recv_mr->rkey);
	send_info.size = htobe32(sizeof(cb->recv_buf));

	rping_init_conn_param(cb, &conn_param);
	conn_param.private_data = &send_info;
	conn_param.private_data_len = sizeof(send_info);

	ret = rdma_connect(cb->cm_id, &conn_param);
	if (ret) {
		perror("rdma_connect");
		return ret;
	}

	sem_wait(&cb->sem);
	if (cb->state != CONNECTED) {
		fprintf(stderr, "wait for CONNECTED state %d\n", cb->state);
		return -1;
	}

	if (cb->self_create_qp) {
		ret = rping_self_modify_qp(cb, cb->cm_id);
		if (ret) {
			perror("rping_modify_qp");
			return ret;
		}

		ret = rdma_establish(cb->cm_id);
		if (ret) {
			perror("rdma_establish");
			return ret;
		}
	}

	DEBUG_LOG("rdma_connect successful\n");
	return 0;
}

static int rping_bind_client(struct rping_cb *cb)
{
	int ret;

	if (cb->sin.ss_family == AF_INET)
		((struct sockaddr_in *) &cb->sin)->sin_port = cb->port;
	else
		((struct sockaddr_in6 *) &cb->sin)->sin6_port = cb->port;

	if (cb->ssource.ss_family) 
		ret = rdma_resolve_addr(cb->cm_id, (struct sockaddr *) &cb->ssource,
					(struct sockaddr *) &cb->sin, 2000);
	else
		ret = rdma_resolve_addr(cb->cm_id, NULL, (struct sockaddr *) &cb->sin, 2000);

	if (ret) {
		perror("rdma_resolve_addr");
		return ret;
	}

	sem_wait(&cb->sem);
	if (cb->state != ROUTE_RESOLVED) {
		fprintf(stderr, "waiting for addr/route resolution state %d\n",
			cb->state);
		return -1;
	}

	DEBUG_LOG("rdma_resolve_addr - rdma_resolve_route successful\n");
	return 0;
}

static int rping_run_client(struct rping_cb *cb)
{
	struct ibv_recv_wr *bad_wr;
	int ret;

	ret = rping_bind_client(cb);
	if (ret)
		return ret;

	ret = rping_setup_qp(cb, cb->cm_id);
	if (ret) {
		fprintf(stderr, "setup_qp failed: %d\n", ret);
		return ret;
	}

	ret = rping_setup_buffers(cb);
	if (ret) {
		fprintf(stderr, "rping_setup_buffers failed: %d\n", ret);
		goto err1;
	}

	ret = ibv_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
	if (ret) {
		fprintf(stderr, "ibv_post_recv failed: %d\n", ret);
		goto err2;
	}

	ret = pthread_create(&cb->cqthread, NULL, cq_thread, cb);
	if (ret) {
		perror("pthread_create");
		goto err2;
	}

	ret = rping_connect_client(cb);
	if (ret) {
		fprintf(stderr, "connect error %d\n", ret);
		goto err3;
	}

	ret = rping_test_client(cb);
	if (ret) {
		fprintf(stderr, "rping client failed: %d\n", ret);
		goto err4;
	}

	ret = 0;
err4:
	rping_disconnect(cb, cb->cm_id);
err3:
	pthread_join(cb->cqthread, NULL);
err2:
	rping_free_buffers(cb);
err1:
	rping_free_qp(cb);

	return ret;
}

static int get_addr(char *dst, struct sockaddr *addr)
{
	struct addrinfo *res;
	int ret;

	ret = getaddrinfo(dst, NULL, NULL, &res);
	if (ret) {
		printf("getaddrinfo failed (%s) - invalid hostname or IP address\n", gai_strerror(ret));
		return ret;
	}

	if (res->ai_family == PF_INET)
		memcpy(addr, res->ai_addr, sizeof(struct sockaddr_in));
	else if (res->ai_family == PF_INET6)
		memcpy(addr, res->ai_addr, sizeof(struct sockaddr_in6));
	else
		ret = -1;
	
	freeaddrinfo(res);
	return ret;
}

static void usage(const char *name)
{
	printf("%s -s [-vVd] [-S size] [-C count] [-a addr] [-p port]\n", 
	       name);
	printf("%s -c [-vVd] [-S size] [-C count] [-I addr] -a addr [-p port]\n", 
	       name);
	printf("\t-c\t\tclient side\n");
	printf("\t-I\t\tSource address to bind to for client.\n");
	printf("\t-s\t\tserver side.  To bind to any address with IPv6 use -a ::0\n");
	printf("\t-v\t\tdisplay ping data to stdout\n");
	printf("\t-V\t\tvalidate ping data\n");
	printf("\t-d\t\tdebug printfs\n");
	printf("\t-S size \tping data size\n");
	printf("\t-C count\tping count times\n");
	printf("\t-a addr\t\taddress\n");
	printf("\t-p port\t\tport\n");
	printf("\t-P\t\tpersistent server mode allowing multiple connections\n");
	printf("\t-q\t\tuse self-created, self-modified QP\n");
}

int main(int argc, char *argv[])
{
	struct rping_cb *cb;
	int op;
	int ret = 0;
	int persistent_server = 0;
	const uint64_t efdw = 1;

	cb = malloc(sizeof(*cb));
	if (!cb)
		return -ENOMEM;

	memset(cb, 0, sizeof(*cb));
	cb->server = -1;
	cb->state = IDLE;
	cb->size = 64;
	cb->sin.ss_family = PF_INET;
	cb->port = htobe16(7174);
	sem_init(&cb->sem, 0, 0);
	sem_init(&cb->accept_ready, 0, 1);

	opterr = 0;
	while ((op = getopt(argc, argv, "a:I:Pp:C:S:t:scvVdq")) != -1) {
		switch (op) {
		case 'a':
			ret = get_addr(optarg, (struct sockaddr *) &cb->sin);
			break;
		case 'I':
			ret = get_addr(optarg, (struct sockaddr *) &cb->ssource);
			break;
		case 'P':
			persistent_server = 1;
			break;
		case 'p':
			cb->port = htobe16(atoi(optarg));
			DEBUG_LOG("port %d\n", (int) atoi(optarg));
			break;
		case 's':
			cb->server = 1;
			DEBUG_LOG("server\n");
			break;
		case 'c':
			cb->server = 0;
			DEBUG_LOG("client\n");
			break;
		case 'S':
			cb->size = atoi(optarg);
			if ((cb->size < RPING_MIN_BUFSIZE) ||
			    (cb->size > (RPING_BUFSIZE - 1))) {
				fprintf(stderr, "Invalid size %d "
				       "(valid range is %zd to %d)\n",
				       cb->size, RPING_MIN_BUFSIZE, RPING_BUFSIZE);
				ret = EINVAL;
			} else
				DEBUG_LOG("size %d\n", (int) atoi(optarg));
			break;
		case 'C':
			cb->count = atoi(optarg);
			if (cb->count < 0) {
				fprintf(stderr, "Invalid count %d\n",
					cb->count);
				ret = EINVAL;
			} else
				DEBUG_LOG("count %d\n", (int) cb->count);
			break;
		case 'v':
			cb->verbose++;
			DEBUG_LOG("verbose\n");
			break;
		case 'V':
			cb->validate++;
			DEBUG_LOG("validate data\n");
			break;
		case 'd':
			debug++;
			break;
		case 'q':
			cb->self_create_qp = 1;
			break;
		default:
			usage("rping");
			ret = EINVAL;
			goto out;
		}
	}
	if (ret)
		goto out;

	if (cb->server == -1) {
		usage("rping");
		ret = EINVAL;
		goto out;
	}

	cb->eventfd = eventfd(0, EFD_NONBLOCK);
	if (cb->eventfd == -1) {
		perror("Could not create event FD");
		ret = errno;
		goto out;
	}

	cb->cm_channel = create_event_channel();
	if (!cb->cm_channel) {
		ret = errno;
		goto out;
	}

	ret = rdma_create_id(cb->cm_channel, &cb->cm_id, cb, RDMA_PS_TCP);
	if (ret) {
		perror("rdma_create_id");
		goto out2;
	}
	DEBUG_LOG("created cm_id %p\n", cb->cm_id);

	ret = pthread_create(&cb->cmthread, NULL, cm_thread, cb);
	if (ret) {
		perror("pthread_create");
		goto out2;
	}

	if (cb->server) {
		if (persistent_server)
			ret = rping_run_persistent_server(cb);
		else
			ret = rping_run_server(cb);
	} else {
		ret = rping_run_client(cb);
	}

	DEBUG_LOG("destroy cm_id %p\n", cb->cm_id);
	rdma_destroy_id(cb->cm_id);
out2:
	if (write(cb->eventfd, &efdw, sizeof(efdw)) != sizeof(efdw))
		fprintf(stderr, "Failed to signal CM thread\n");

	pthread_join(cb->cmthread, NULL);
	rdma_destroy_event_channel(cb->cm_channel);
out:
	free(cb);
	return ret;
}
