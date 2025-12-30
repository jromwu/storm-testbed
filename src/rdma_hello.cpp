#include <arpa/inet.h>
#include <errno.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

namespace {

struct Options {
  std::string rdma_device = "mlx5_0";
  uint8_t rdma_port = 1;
  uint8_t gid_index = 0;
  std::string peer_host;
  uint16_t oob_port = 18517;
  bool is_sender = false;
  std::string op = "write_imm";
};

struct WireInfo {
  uint32_t qp_num = 0;
  uint32_t psn = 0;
  uint32_t rkey = 0;
  uint64_t addr = 0;
  uint16_t lid = 0;
  uint8_t gid[16] = {};
};

struct RdmaResources {
  ibv_context *ctx = nullptr;
  ibv_pd *pd = nullptr;
  ibv_cq *cq = nullptr;
  ibv_qp *qp = nullptr;
  ibv_mr *mr = nullptr;
  void *buf = nullptr;
  size_t buf_len = 0;
};

int Listen(uint16_t port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) throw std::runtime_error("socket failed");
  int opt = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(port);
  if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    ::close(fd);
    throw std::runtime_error("bind failed");
  }
  if (::listen(fd, 1) < 0) {
    ::close(fd);
    throw std::runtime_error("listen failed");
  }
  return fd;
}

int AcceptOne(int listen_fd) {
  sockaddr_in peer{};
  socklen_t len = sizeof(peer);
  int fd = ::accept(listen_fd, reinterpret_cast<sockaddr *>(&peer), &len);
  if (fd < 0) throw std::runtime_error("accept failed");
  return fd;
}

int ConnectWithRetry(const std::string &host, uint16_t port, int attempts, int backoff_ms) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo *res = nullptr;
  const std::string port_str = std::to_string(port);
  if (::getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res) != 0) {
    throw std::runtime_error("getaddrinfo failed");
  }
  for (int i = 0; i < attempts; ++i) {
    for (addrinfo *p = res; p != nullptr; p = p->ai_next) {
      int fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
      if (fd < 0) continue;
      if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) {
        ::freeaddrinfo(res);
        return fd;
      }
      ::close(fd);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
  }
  ::freeaddrinfo(res);
  throw std::runtime_error("connect retries exhausted");
}

bool SendAll(int fd, const void *buf, size_t len) {
  const uint8_t *p = static_cast<const uint8_t *>(buf);
  size_t sent = 0;
  while (sent < len) {
    ssize_t rc = ::send(fd, p + sent, len - sent, 0);
    if (rc <= 0) {
      if (errno == EINTR) continue;
      return false;
    }
    sent += static_cast<size_t>(rc);
  }
  return true;
}

bool RecvAll(int fd, void *buf, size_t len) {
  uint8_t *p = static_cast<uint8_t *>(buf);
  size_t recvd = 0;
  while (recvd < len) {
    ssize_t rc = ::recv(fd, p + recvd, len - recvd, MSG_WAITALL);
    if (rc <= 0) {
      if (errno == EINTR) continue;
      return false;
    }
    recvd += static_cast<size_t>(rc);
  }
  return true;
}

ibv_device *FindDevice(const std::string &name) {
  int num = 0;
  ibv_device **list = ibv_get_device_list(&num);
  if (!list) throw std::runtime_error("ibv_get_device_list failed");
  ibv_device *found = nullptr;
  for (int i = 0; i < num; ++i) {
    if (name == ibv_get_device_name(list[i])) {
      found = list[i];
      break;
    }
  }
  ibv_free_device_list(list);
  if (!found) throw std::runtime_error("rdma device not found");
  return found;
}

RdmaResources InitRdma(const Options &opts) {
  RdmaResources res{};
  ibv_device *dev = FindDevice(opts.rdma_device);
  res.ctx = ibv_open_device(dev);
  if (!res.ctx) throw std::runtime_error("ibv_open_device failed");
  res.pd = ibv_alloc_pd(res.ctx);
  if (!res.pd) throw std::runtime_error("ibv_alloc_pd failed");

  res.buf_len = 256;
  if (posix_memalign(&res.buf, 4096, res.buf_len) != 0) {
    throw std::runtime_error("posix_memalign failed");
  }
  std::memset(res.buf, 0, res.buf_len);
  res.mr = ibv_reg_mr(res.pd, res.buf, res.buf_len,
                      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  if (!res.mr) throw std::runtime_error("ibv_reg_mr failed");

  res.cq = ibv_create_cq(res.ctx, 32, nullptr, nullptr, 0);
  if (!res.cq) throw std::runtime_error("ibv_create_cq failed");

  ibv_qp_init_attr init{};
  init.qp_type = IBV_QPT_RC;
  init.send_cq = res.cq;
  init.recv_cq = res.cq;
  init.cap.max_send_wr = 16;
  init.cap.max_recv_wr = 16;
  init.cap.max_send_sge = 1;
  init.cap.max_recv_sge = 1;
  res.qp = ibv_create_qp(res.pd, &init);
  if (!res.qp) throw std::runtime_error("ibv_create_qp failed");

  ibv_qp_attr attr{};
  attr.qp_state = IBV_QPS_INIT;
  attr.pkey_index = 0;
  attr.port_num = opts.rdma_port;
  attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE;
  int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
  if (ibv_modify_qp(res.qp, &attr, flags) != 0) {
    throw std::runtime_error("ibv_modify_qp INIT failed");
  }

  return res;
}

void MoveToRtr(ibv_qp *qp, const WireInfo &remote, const Options &opts, const ibv_port_attr &port_attr) {
  ibv_qp_attr attr{};
  attr.qp_state = IBV_QPS_RTR;
  attr.path_mtu = static_cast<ibv_mtu>(port_attr.active_mtu);
  attr.dest_qp_num = remote.qp_num;
  attr.rq_psn = remote.psn;
  attr.max_dest_rd_atomic = 1;
  attr.min_rnr_timer = 12;
  const bool use_gid = port_attr.link_layer == IBV_LINK_LAYER_ETHERNET;
  attr.ah_attr.is_global = use_gid ? 1 : 0;
  attr.ah_attr.dlid = use_gid ? 0 : remote.lid;
  attr.ah_attr.sl = 0;
  attr.ah_attr.port_num = opts.rdma_port;
  if (use_gid) {
    attr.ah_attr.grh.dgid = *reinterpret_cast<const ibv_gid *>(remote.gid);
    attr.ah_attr.grh.sgid_index = opts.gid_index;
    attr.ah_attr.grh.hop_limit = 64;
  }
  int flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
              IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
  if (ibv_modify_qp(qp, &attr, flags) != 0) {
    throw std::runtime_error("ibv_modify_qp RTR failed");
  }
}

void MoveToRts(ibv_qp *qp, uint32_t psn) {
  ibv_qp_attr attr{};
  attr.qp_state = IBV_QPS_RTS;
  attr.timeout = 14;
  attr.retry_cnt = 7;
  attr.rnr_retry = 7;
  attr.sq_psn = psn;
  attr.max_rd_atomic = 1;
  int flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
              IBV_QP_MAX_QP_RD_ATOMIC;
  if (ibv_modify_qp(qp, &attr, flags) != 0) {
    throw std::runtime_error("ibv_modify_qp RTS failed");
  }
}

void ParseArgs(int argc, char **argv, Options *opts) {
  for (int i = 1; i < argc; ++i) {
    std::string arg(argv[i]);
    auto next_val = [&](const char *flag) -> std::string {
      if (i + 1 >= argc) throw std::runtime_error(std::string("missing value for ") + flag);
      return std::string(argv[++i]);
    };
    if (arg == "--mode") {
      std::string mode = next_val("--mode");
      if (mode == "send") {
        opts->is_sender = true;
      } else if (mode == "recv") {
        opts->is_sender = false;
      } else {
        throw std::runtime_error("mode must be send or recv");
      }
    } else if (arg == "--op") {
      opts->op = next_val("--op");
      if (opts->op != "write" && opts->op != "write_imm") {
        throw std::runtime_error("op must be write or write_imm");
      }
    } else if (arg == "--peer") {
      opts->peer_host = next_val("--peer");
    } else if (arg == "--rdma-device") {
      opts->rdma_device = next_val("--rdma-device");
    } else if (arg == "--rdma-port") {
      opts->rdma_port = static_cast<uint8_t>(std::stoi(next_val("--rdma-port")));
    } else if (arg == "--gid-index") {
      opts->gid_index = static_cast<uint8_t>(std::stoi(next_val("--gid-index")));
    } else if (arg == "--oob-port") {
      opts->oob_port = static_cast<uint16_t>(std::stoi(next_val("--oob-port")));
    } else if (arg == "--help") {
      throw std::runtime_error("help");
    } else {
      throw std::runtime_error("unknown arg: " + arg);
    }
  }
  if (opts->peer_host.empty()) throw std::runtime_error("missing --peer");
}

}  // namespace

int main(int argc, char **argv) {
  try {
    Options opts;
    try {
      ParseArgs(argc, argv, &opts);
    } catch (const std::runtime_error &e) {
      std::cerr << "Usage: " << argv[0]
                << " --mode <send|recv> --peer <host> [--op write|write_imm] [--rdma-device mlx5_0] "
                   "[--rdma-port 1] [--gid-index 0] [--oob-port 18517]\n";
      return e.what() == std::string("help") ? 0 : 1;
    }

    RdmaResources res = InitRdma(opts);
    ibv_port_attr port_attr{};
    if (ibv_query_port(res.ctx, opts.rdma_port, &port_attr) != 0) {
      throw std::runtime_error("ibv_query_port failed");
    }
    ibv_gid gid{};
    if (ibv_query_gid(res.ctx, opts.rdma_port, opts.gid_index, &gid) != 0) {
      throw std::runtime_error("ibv_query_gid failed");
    }

    const uint32_t psn = 0x12345;
    WireInfo local{};
    local.qp_num = res.qp->qp_num;
    local.psn = psn;
    local.rkey = res.mr->rkey;
    local.addr = reinterpret_cast<uint64_t>(res.buf);
    local.lid = port_attr.lid;
    std::memcpy(local.gid, gid.raw, sizeof(local.gid));

    WireInfo remote{};
    int ctrl_fd = -1;
    if (opts.is_sender) {
      ctrl_fd = ConnectWithRetry(opts.peer_host, opts.oob_port, 50, 100);
      if (!SendAll(ctrl_fd, &local, sizeof(local)) || !RecvAll(ctrl_fd, &remote, sizeof(remote))) {
        ::close(ctrl_fd);
        throw std::runtime_error("handshake failed");
      }
    } else {
      int listen_fd = Listen(opts.oob_port);
      ctrl_fd = AcceptOne(listen_fd);
      if (!RecvAll(ctrl_fd, &remote, sizeof(remote)) || !SendAll(ctrl_fd, &local, sizeof(local))) {
        ::close(ctrl_fd);
        ::close(listen_fd);
        throw std::runtime_error("handshake failed");
      }
      ::close(listen_fd);
    }

    MoveToRtr(res.qp, remote, opts, port_attr);
    MoveToRts(res.qp, psn);

    const std::string payload =
        (opts.op == "write") ? "hello from rdma write\n" : "hello from rdma write_imm\n";
    if (!opts.is_sender) {
      if (opts.op == "write_imm") {
        ibv_sge sge{};
        sge.addr = reinterpret_cast<uint64_t>(res.buf);
        sge.length = 4;
        sge.lkey = res.mr->lkey;
        ibv_recv_wr wr{};
        wr.wr_id = 0x1;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        ibv_recv_wr *bad = nullptr;
        if (ibv_post_recv(res.qp, &wr, &bad) != 0) {
          throw std::runtime_error("ibv_post_recv failed");
        }
      }

      if (!SendAll(ctrl_fd, "RDY!", 4)) {
        throw std::runtime_error("failed to send ready");
      }

      char done[4];
      if (!RecvAll(ctrl_fd, done, sizeof(done))) {
        throw std::runtime_error("failed to receive done");
      }
      if (std::memcmp(done, "DONE", 4) != 0) {
        throw std::runtime_error("unexpected control message");
      }

      if (opts.op == "write_imm") {
        ibv_wc wc{};
        while (ibv_poll_cq(res.cq, 1, &wc) == 0) {
          // spin
        }
        if (wc.status != IBV_WC_SUCCESS) {
          throw std::runtime_error("recv completion failed");
        }
      }

      if (std::strncmp(static_cast<char *>(res.buf), payload.c_str(), payload.size()) != 0) {
        std::cerr << "Payload mismatch.\nExpected: " << payload << "\nActual: " << static_cast<char *>(res.buf)
                  << "\n";
        throw std::runtime_error("payload mismatch");
      }
      std::cout << "Received: " << static_cast<char *>(res.buf) << "\n";
      ::close(ctrl_fd);
      return 0;
    }

    char ready[4];
    if (!RecvAll(ctrl_fd, ready, sizeof(ready))) {
      throw std::runtime_error("failed to receive ready");
    }
    if (std::memcmp(ready, "RDY!", 4) != 0) {
      throw std::runtime_error("unexpected control message");
    }

    std::memcpy(res.buf, payload.data(), payload.size() + 1);
    ibv_sge sge{};
    sge.addr = reinterpret_cast<uint64_t>(res.buf);
    sge.length = static_cast<uint32_t>(payload.size() + 1);
    sge.lkey = res.mr->lkey;
    ibv_send_wr wr{};
    wr.wr_id = 0x2;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    if (opts.op == "write") {
      wr.opcode = IBV_WR_RDMA_WRITE;
    } else {
      wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
      wr.imm_data = htonl(0x1234);
    }
    wr.wr.rdma.remote_addr = remote.addr;
    wr.wr.rdma.rkey = remote.rkey;
    ibv_send_wr *bad = nullptr;
    if (ibv_post_send(res.qp, &wr, &bad) != 0) {
      throw std::runtime_error("ibv_post_send failed");
    }
    ibv_wc wc{};
    while (ibv_poll_cq(res.cq, 1, &wc) == 0) {
      // spin
    }
    if (wc.status != IBV_WC_SUCCESS) {
      throw std::runtime_error("send completion failed");
    }
    if (!SendAll(ctrl_fd, "DONE", 4)) {
      throw std::runtime_error("failed to send done");
    }
    ::close(ctrl_fd);
    std::cout << "Sent hello with " << opts.op << ".\n";
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << "\n";
    return 1;
  }
}
