#include <arpa/inet.h>
#include <errno.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sched.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cinttypes>
#include <cstdlib>
#include <cstring>
#include <emmintrin.h>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

std::string GetHostname() {
  char buf[256];
  if (::gethostname(buf, sizeof(buf)) != 0) {
    return "unknown-host";
  }
  buf[sizeof(buf) - 1] = '\0';
  return std::string(buf);
}

std::string FormatSockaddr(const sockaddr *addr, socklen_t len) {
  char host[NI_MAXHOST];
  char serv[NI_MAXSERV];
  if (getnameinfo(addr, len, host, sizeof(host), serv, sizeof(serv),
                  NI_NUMERICHOST | NI_NUMERICSERV) != 0) {
    return "unknown";
  }
  return std::string(host) + ":" + std::string(serv);
}

class LinePrefixBuf : public std::streambuf {
 public:
  LinePrefixBuf(std::streambuf *dest, std::string prefix)
      : dest_(dest), prefix_(std::move(prefix)), at_line_start_(true) {}

 protected:
  int overflow(int ch) override {
    if (ch == traits_type::eof()) {
      return traits_type::not_eof(ch);
    }
    if (at_line_start_) {
      dest_->sputn(prefix_.data(), static_cast<std::streamsize>(prefix_.size()));
      at_line_start_ = false;
    }
    if (dest_->sputc(static_cast<char>(ch)) == traits_type::eof()) {
      return traits_type::eof();
    }
    if (ch == '\n') {
      at_line_start_ = true;
    }
    return ch;
  }

  std::streamsize xsputn(const char *s, std::streamsize n) override {
    std::streamsize total = 0;
    for (std::streamsize i = 0; i < n; ++i) {
      if (overflow(static_cast<unsigned char>(s[i])) == traits_type::eof()) {
        break;
      }
      ++total;
    }
    return total;
  }

 private:
  std::streambuf *dest_;
  std::string prefix_;
  bool at_line_start_;
};

struct PrefixedLogs {
  explicit PrefixedLogs(const std::string &prefix)
      : cout_buf(std::cout.rdbuf(), prefix), cerr_buf(std::cerr.rdbuf(), prefix) {
    old_cout = std::cout.rdbuf(&cout_buf);
    old_cerr = std::cerr.rdbuf(&cerr_buf);
  }
  ~PrefixedLogs() {
    std::cout.rdbuf(old_cout);
    std::cerr.rdbuf(old_cerr);
  }

  LinePrefixBuf cout_buf;
  LinePrefixBuf cerr_buf;
  std::streambuf *old_cout = nullptr;
  std::streambuf *old_cerr = nullptr;
};

constexpr int kPriorityCount = 8;

enum class TimingSource { kCycles, kSteady };

struct Options {
  std::string rdma_device = "mlx5_0";
  uint8_t rdma_port = 1;
  uint8_t gid_index = 0;
  std::string next_host;
  uint16_t oob_port = 18515;
  int rank = 0;
  int ranks = 1;
  size_t chunk_bytes = 128ull * 1024ull * 1024ull;
  int chunk_count = 7;
  int compute_delay_us = 1;
  bool uniform_priority = false;  // when true, use default priority for all QPs
  int cpu_affinity = -1;
  bool verbose = false;
  TimingSource timing = TimingSource::kCycles;
  double cpu_mhz = 0.0;
};

struct WireInfo {
  uint32_t qp_num[kPriorityCount];
  uint32_t psn[kPriorityCount];
  uint32_t rkey;
  uint64_t addr;
  uint16_t lid;
  uint8_t gid[16];
};

struct RemoteQp {
  uint32_t qp_num = 0;
  uint32_t psn = 0;
  uint16_t lid = 0;
  uint8_t gid[16] = {};
  uint32_t rkey = 0;
  uint64_t addr = 0;
};

struct RdmaResources {
  ibv_context *ctx = nullptr;
  ibv_pd *pd = nullptr;
  ibv_cq *cq = nullptr;
  ibv_mr *mr = nullptr;
  void *buf = nullptr;
  void *recv_buf = nullptr;
  void *send_buf = nullptr;
  size_t buf_len = 0;
  size_t region_len = 0;
  uint64_t max_mr_size = 0;
  uint32_t max_qp_wr = 0;
  uint32_t max_cqe = 0;
};

std::string FormatGid(const ibv_gid &gid) {
  char buf[40];
  std::snprintf(buf, sizeof(buf), "%08x%08x%08x%08x", ntohl(gid.global.subnet_prefix >> 32),
                ntohl(static_cast<uint32_t>(gid.global.subnet_prefix & 0xffffffff)),
                ntohl(gid.global.interface_id >> 32), ntohl(static_cast<uint32_t>(gid.global.interface_id & 0xffffffff)));
  return std::string(buf);
}

#if defined(__x86_64__) || defined(__i386__)
using cycles_t = unsigned long long;
inline cycles_t GetCycles() {
  unsigned low, high;
  asm volatile("rdtsc" : "=a"(low), "=d"(high));
  return (static_cast<cycles_t>(high) << 32) | low;
}
constexpr bool kHasCycles = true;
#else
using cycles_t = unsigned long long;
inline cycles_t GetCycles() { return 0; }
constexpr bool kHasCycles = false;
#endif

inline void CpuRelax() {
#if defined(__x86_64__) || defined(__i386__)
  asm volatile("pause");
#endif
}

inline void FlushCacheLine(const void *ptr) {
#if defined(__x86_64__) || defined(__i386__)
  _mm_clflush(ptr);
#else
  (void)ptr;
#endif
}

inline void FlushMarkerLines(const void *base, size_t offset, size_t chunk_bytes, size_t marker_size) {
#if defined(__x86_64__) || defined(__i386__)
  const uint8_t *ptr = static_cast<const uint8_t *>(base) + offset;
  FlushCacheLine(ptr);
  if (chunk_bytes > marker_size) {
    const uint8_t *tail = ptr + chunk_bytes - marker_size;
    FlushCacheLine(tail);
  }
  _mm_mfence();
#else
  (void)base;
  (void)offset;
  (void)chunk_bytes;
  (void)marker_size;
#endif
}

std::optional<double> ReadCpuMhzFromCpuinfo() {
  std::ifstream file("/proc/cpuinfo");
  if (!file) return std::nullopt;
  std::string line;
  while (std::getline(file, line)) {
    if (line.find("cpu MHz") == std::string::npos) continue;
    const auto pos = line.find(':');
    if (pos == std::string::npos) continue;
    const char *start = line.c_str() + pos + 1;
    char *end = nullptr;
    double val = std::strtod(start, &end);
    if (end != start) {
      return val;
    }
  }
  return std::nullopt;
}

std::optional<double> ReadCpuMhzFromSysfs(const char *path) {
  std::ifstream file(path);
  if (!file) return std::nullopt;
  double khz = 0.0;
  if (!(file >> khz)) return std::nullopt;
  return khz / 1000.0;
}

double DetectCpuMhz() {
  if (auto mhz = ReadCpuMhzFromSysfs("/sys/devices/system/cpu/cpu0/tsc_freq_khz")) return *mhz;
  if (auto mhz = ReadCpuMhzFromSysfs("/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq")) return *mhz;
  if (auto mhz = ReadCpuMhzFromCpuinfo()) return *mhz;
  return 0.0;
}

struct Timer {
  TimingSource source = TimingSource::kSteady;
  double ticks_per_ms = 1e6;
  cycles_t start_cycles = 0;
  std::chrono::steady_clock::time_point start_time{};
  double cpu_mhz = 0.0;

  uint64_t Now() const {
    if (source == TimingSource::kCycles) {
      return static_cast<uint64_t>(GetCycles() - start_cycles);
    }
    auto delta = std::chrono::steady_clock::now() - start_time;
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(delta).count());
  }
};

Timer InitTimer(const Options &opts) {
  Timer timer;
  timer.source = opts.timing;
  if (opts.timing == TimingSource::kCycles) {
    if (!kHasCycles) {
      throw std::runtime_error("cycle counter timing not supported on this architecture");
    }
    timer.cpu_mhz = opts.cpu_mhz > 0.0 ? opts.cpu_mhz : DetectCpuMhz();
    if (timer.cpu_mhz <= 0.0) {
      throw std::runtime_error("failed to detect CPU MHz; pass --cpu-mhz");
    }
    timer.ticks_per_ms = timer.cpu_mhz * 1000.0;
    timer.start_cycles = GetCycles();
  } else {
    timer.ticks_per_ms = 1e6;
    timer.start_time = std::chrono::steady_clock::now();
  }
  return timer;
}

size_t MarkerSizeForChunk(size_t chunk_bytes) {
  if (chunk_bytes >= sizeof(uint64_t)) return sizeof(uint64_t);
  if (chunk_bytes >= sizeof(uint32_t)) return sizeof(uint32_t);
  return sizeof(uint8_t);
}

uint64_t ChunkMarkerValue(uint64_t seed, int chunk_idx) {
  return (seed & 0xffffffff00000000ull) | static_cast<uint32_t>(chunk_idx);
}

uint64_t Fnv1a64(const void *data, size_t len, uint64_t seed = 1469598103934665603ull) {
  const uint8_t *bytes = static_cast<const uint8_t *>(data);
  uint64_t hash = seed;
  for (size_t i = 0; i < len; ++i) {
    hash ^= bytes[i];
    hash *= 1099511628211ull;
  }
  return hash;
}

uint64_t MarkerSeed(const WireInfo &info) {
  uint64_t hash = Fnv1a64(info.gid, sizeof(info.gid));
  hash = Fnv1a64(&info.lid, sizeof(info.lid), hash);
  hash = Fnv1a64(&info.rkey, sizeof(info.rkey), hash);
  hash = Fnv1a64(&info.addr, sizeof(info.addr), hash);
  return hash;
}

std::array<uint8_t, 8> MarkerBytes(uint64_t marker) {
  std::array<uint8_t, 8> bytes{};
  std::memcpy(bytes.data(), &marker, bytes.size());
  return bytes;
}

uint64_t ReadMarkerValue(const void *base, size_t offset, size_t marker_size) {
  const volatile uint8_t *ptr = static_cast<const volatile uint8_t *>(base) + offset;
  uint64_t val = 0;
  for (size_t i = 0; i < marker_size; ++i) {
    val |= static_cast<uint64_t>(ptr[i]) << (i * 8);
  }
  return val;
}

void WriteChunkMarker(uint8_t *base, size_t offset, size_t chunk_bytes, const std::array<uint8_t, 8> &marker_bytes,
                      size_t marker_size) {
  std::memcpy(base + offset, marker_bytes.data(), marker_size);
  if (chunk_bytes > marker_size) {
    const size_t tail_offset = offset + chunk_bytes - marker_size;
    std::memcpy(base + tail_offset, marker_bytes.data(), marker_size);
  }
}

bool ChunkHasMarker(const void *base, size_t offset, size_t chunk_bytes, const std::array<uint8_t, 8> &marker_bytes,
                    size_t marker_size) {
  FlushMarkerLines(base, offset, chunk_bytes, marker_size);
  const volatile uint8_t *ptr = static_cast<const volatile uint8_t *>(base) + offset;
  for (size_t i = 0; i < marker_size; ++i) {
    if (ptr[i] != marker_bytes[i]) return false;
  }
  if (chunk_bytes > marker_size) {
    const volatile uint8_t *tail = static_cast<const volatile uint8_t *>(base) + offset + chunk_bytes - marker_size;
    for (size_t i = 0; i < marker_size; ++i) {
      if (tail[i] != marker_bytes[i]) return false;
    }
  }
  return true;
}

void InitBufferPattern(uint8_t *base, size_t region_len, size_t chunk_bytes, int chunk_count, uint64_t seed) {
  const uint8_t fill = static_cast<uint8_t>(0x10 + (seed & 0x7f));
  std::memset(base, fill, region_len);
  const size_t marker_size = MarkerSizeForChunk(chunk_bytes);
  for (int chunk_idx = 0; chunk_idx < chunk_count; ++chunk_idx) {
    const size_t offset = static_cast<size_t>(chunk_bytes) * static_cast<size_t>(chunk_idx);
    const auto marker_bytes = MarkerBytes(ChunkMarkerValue(seed, chunk_idx));
    WriteChunkMarker(base, offset, chunk_bytes, marker_bytes, marker_size);
  }
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

void RingBarrier(int prev_fd, int next_fd, const Options &opts) {
  const uint32_t kMagic = 0xB17B00B5;
  uint32_t token = kMagic;
  if (opts.rank == 0) {
    if (!SendAll(next_fd, &token, sizeof(token))) {
      throw std::runtime_error("barrier send to next failed");
    }
  }
  if (!RecvAll(prev_fd, &token, sizeof(token))) {
    throw std::runtime_error("barrier recv from prev failed");
  }
  if (token != kMagic) {
    throw std::runtime_error("barrier token mismatch");
  }
  if (opts.rank != 0) {
    if (!SendAll(next_fd, &token, sizeof(token))) {
      throw std::runtime_error("barrier forward to next failed");
    }
  }
}

int Listen(uint16_t port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) throw std::runtime_error("socket() failed");
  int opt = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(port);
  if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    ::close(fd);
    throw std::runtime_error("bind() failed");
  }
  if (::listen(fd, 1) < 0) {
    ::close(fd);
    throw std::runtime_error("listen() failed");
  }
  return fd;
}

int AcceptOne(int listen_fd) {
  sockaddr_in peer{};
  socklen_t len = sizeof(peer);
  int fd = ::accept(listen_fd, reinterpret_cast<sockaddr *>(&peer), &len);
  if (fd < 0) throw std::runtime_error("accept() failed");
  std::cout << "Accepted control connection from " << FormatSockaddr(reinterpret_cast<sockaddr *>(&peer), len) << "\n";
  return fd;
}

int ConnectWithRetry(const std::string &host, uint16_t port, int attempts, int backoff_ms) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo *res = nullptr;
  const std::string port_str = std::to_string(port);
  int rc = ::getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res);
  if (rc != 0) throw std::runtime_error("getaddrinfo failed for " + host);

  for (int i = 0; i < attempts; ++i) {
    for (addrinfo *p = res; p != nullptr; p = p->ai_next) {
      int fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
      if (fd < 0) continue;
      if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) {
        std::cout << "Connected control to " << host << " at "
                  << FormatSockaddr(reinterpret_cast<const sockaddr *>(p->ai_addr), p->ai_addrlen) << "\n";
        ::freeaddrinfo(res);
        return fd;
      }
      ::close(fd);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
  }
  ::freeaddrinfo(res);
  throw std::runtime_error("connect() retries exhausted for " + host);
}

uint32_t FixedPsn(int priority, int salt) {
  return static_cast<uint32_t>(0x12345 + (priority * 17) + salt);
}

uint8_t PriorityForIndex(int idx, const Options &opts) {
  return opts.uniform_priority ? 7 : static_cast<uint8_t>(7 - (idx % kPriorityCount));
}

uint8_t DscpForPriority(uint8_t prio) {
  // Default DSCP mapping aligned to the provided switch dscp->tc table.
  // prio 0..7 -> DSCP {10,0,16,24,32,40,48,56} -> TC {0,1,2,3,4,5,6,7}.
  static const uint8_t kMap[kPriorityCount] = {10, 0, 16, 24, 32, 40, 48, 56};
  return kMap[prio % kPriorityCount];
}

bool PinThreadToCpu(int cpu, const char *label) {
  if (cpu < 0) return true;
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);
  int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
  if (rc != 0) {
    std::cerr << "Failed to set CPU affinity for " << label << " to CPU " << cpu << ": "
              << std::strerror(rc) << "\n";
    return false;
  }
  return true;
}

void ParseArgs(int argc, char **argv, Options *opts) {
  for (int i = 1; i < argc; ++i) {
    std::string arg(argv[i]);
    auto next_val = [&](const char *flag) -> std::string {
      if (i + 1 >= argc) throw std::runtime_error(std::string("missing value for ") + flag);
      return std::string(argv[++i]);
    };
    if (arg == "--rdma-device") {
      opts->rdma_device = next_val("--rdma-device");
    } else if (arg == "--rdma-port") {
      opts->rdma_port = static_cast<uint8_t>(std::stoi(next_val("--rdma-port")));
    } else if (arg == "--gid-index") {
      opts->gid_index = static_cast<uint8_t>(std::stoi(next_val("--gid-index")));
    } else if (arg == "--next-host") {
      opts->next_host = next_val("--next-host");
    } else if (arg == "--oob-port") {
      opts->oob_port = static_cast<uint16_t>(std::stoi(next_val("--oob-port")));
    } else if (arg == "--rank") {
      opts->rank = std::stoi(next_val("--rank"));
    } else if (arg == "--ranks") {
      opts->ranks = std::stoi(next_val("--ranks"));
    } else if (arg == "--chunk-bytes") {
      opts->chunk_bytes = static_cast<size_t>(std::stoull(next_val("--chunk-bytes")));
    } else if (arg == "--chunk-count") {
      opts->chunk_count = std::stoi(next_val("--chunk-count"));
    } else if (arg == "--compute-delay-us") {
      opts->compute_delay_us = std::stoi(next_val("--compute-delay-us"));
    } else if (arg == "--uniform-priority") {
      opts->uniform_priority = true;
    } else if (arg == "--cpu-affinity") {
      opts->cpu_affinity = std::stoi(next_val("--cpu-affinity"));
    } else if (arg == "--verbose") {
      opts->verbose = true;
    } else if (arg == "--timing") {
      const std::string mode = next_val("--timing");
      if (mode == "cycles") {
        opts->timing = TimingSource::kCycles;
      } else if (mode == "steady") {
        opts->timing = TimingSource::kSteady;
      } else {
        throw std::runtime_error("timing must be cycles or steady");
      }
    } else if (arg == "--cpu-mhz") {
      opts->cpu_mhz = std::stod(next_val("--cpu-mhz"));
    } else if (arg == "--help") {
      throw std::runtime_error("help");
    } else {
      throw std::runtime_error("unknown arg: " + arg);
    }
  }
  if (opts->next_host.empty()) throw std::runtime_error("missing --next-host");
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
  if (!found) throw std::runtime_error("rdma device not found: " + name);
  return found;
}

RdmaResources InitRdma(const Options &opts, ibv_port_attr *port_attr) {
  RdmaResources res{};
  ibv_device *dev = FindDevice(opts.rdma_device);
  res.ctx = ibv_open_device(dev);
  if (!res.ctx) throw std::runtime_error("ibv_open_device failed");

  ibv_device_attr dev_attr{};
  if (ibv_query_device(res.ctx, &dev_attr) != 0) {
    throw std::runtime_error("ibv_query_device failed");
  }
  res.max_mr_size = dev_attr.max_mr_size;
  res.max_qp_wr = dev_attr.max_qp_wr;
  res.max_cqe = static_cast<uint32_t>(dev_attr.max_cqe);

  if (!port_attr) {
    throw std::runtime_error("port_attr output is null");
  }
  if (ibv_query_port(res.ctx, opts.rdma_port, port_attr) != 0) {
    throw std::runtime_error("ibv_query_port failed");
  }
  res.pd = ibv_alloc_pd(res.ctx);
  if (!res.pd) throw std::runtime_error("ibv_alloc_pd failed");

  if (opts.chunk_bytes == 0 || opts.chunk_count <= 0) {
    throw std::runtime_error("chunk_bytes and chunk_count must be positive");
  }
  const size_t chunk_count = static_cast<size_t>(opts.chunk_count);
  if (opts.chunk_bytes > std::numeric_limits<size_t>::max() / chunk_count) {
    throw std::runtime_error("buffer size overflow for chunk_bytes * chunk_count");
  }
  const size_t buf_len = opts.chunk_bytes * chunk_count;
  if (buf_len > std::numeric_limits<size_t>::max() / 2) {
    throw std::runtime_error("buffer size overflow for double buffer");
  }
  const size_t total_len = buf_len * 2;
  if (res.max_mr_size != 0 && total_len > res.max_mr_size) {
    throw std::runtime_error("buffer size exceeds device max_mr_size");
  }
  constexpr size_t kPageAlign = 4096;
  if (posix_memalign(&res.buf, kPageAlign, total_len) != 0) {
    throw std::runtime_error("posix_memalign failed");
  }
  res.buf_len = total_len;
  res.region_len = buf_len;
  res.recv_buf = res.buf;
  res.send_buf = static_cast<uint8_t *>(res.buf) + buf_len;
  res.mr = ibv_reg_mr(res.pd, res.buf, total_len,
                      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
  if (!res.mr) throw std::runtime_error("ibv_reg_mr failed");

  return res;
}

ibv_qp *CreatePriorityQp(ibv_pd *pd, ibv_cq *cq, uint8_t port, uint8_t priority, int min_send_wr,
                         int min_recv_wr, uint32_t max_qp_wr) {
  ibv_qp_init_attr init{};
  init.qp_type = IBV_QPT_RC;
  init.send_cq = cq;
  init.recv_cq = cq;
  int desired_send_wr = std::max(128, min_send_wr);
  int desired_recv_wr = std::max(64, min_recv_wr);  // recv WRs unused for write-only data path
  if (max_qp_wr != 0) {
    if (static_cast<uint32_t>(desired_send_wr) > max_qp_wr) {
      throw std::runtime_error("chunk_bytes requires max_send_wr " + std::to_string(desired_send_wr) +
                               " above device limit " + std::to_string(max_qp_wr));
    }
    if (static_cast<uint32_t>(desired_recv_wr) > max_qp_wr) {
      throw std::runtime_error("chunk_count requires max_recv_wr " + std::to_string(desired_recv_wr) +
                               " above device limit " + std::to_string(max_qp_wr));
    }
  }
  init.cap.max_send_wr = desired_send_wr;
  init.cap.max_recv_wr = desired_recv_wr;
  init.cap.max_send_sge = 1;
  init.cap.max_recv_sge = 1;
  init.sq_sig_all = 0;

  ibv_qp *qp = ibv_create_qp(pd, &init);
  if (!qp) throw std::runtime_error("ibv_create_qp failed");

  ibv_qp_attr attr{};
  attr.qp_state = IBV_QPS_INIT;
  attr.pkey_index = 0;
  attr.port_num = port;
  attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_LOCAL_WRITE;
  attr.ah_attr.is_global = 0;
  attr.ah_attr.sl = priority;
  int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
  if (ibv_modify_qp(qp, &attr, flags) != 0) {
    throw std::runtime_error("ibv_modify_qp INIT failed");
  }
  return qp;
}

bool MoveToRtr(ibv_qp *qp, const RemoteQp &remote, uint8_t port, uint8_t priority, bool use_gid, uint8_t path_mtu,
               uint8_t sgid_index) {
  ibv_qp_attr attr{};
  attr.qp_state = IBV_QPS_RTR;
  attr.path_mtu = static_cast<ibv_mtu>(path_mtu);
  attr.dest_qp_num = remote.qp_num;
  attr.rq_psn = remote.psn;
  attr.max_dest_rd_atomic = 1;
  attr.min_rnr_timer = 12;
  attr.ah_attr.is_global = use_gid ? 1 : 0;
  attr.ah_attr.dlid = use_gid ? 0 : remote.lid;
  attr.ah_attr.sl = priority;
  attr.ah_attr.src_path_bits = 0;
  attr.ah_attr.port_num = port;
  if (use_gid) {
    attr.ah_attr.grh.dgid = *reinterpret_cast<const ibv_gid *>(remote.gid);
    attr.ah_attr.grh.sgid_index = sgid_index;
    attr.ah_attr.grh.hop_limit = 64;
    const uint8_t dscp = DscpForPriority(priority);
    attr.ah_attr.grh.traffic_class = static_cast<uint8_t>(dscp << 2);  // DSCP in upper 6 bits.
  }
  int flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
              IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
  int rc = ibv_modify_qp(qp, &attr, flags);
  if (rc != 0) {
    std::cerr << "ibv_modify_qp RTR failed: errno=" << errno << " (" << std::strerror(errno) << ")\n";
  }
  return rc == 0;
}

bool MoveToRts(ibv_qp *qp, uint32_t psn) {
  ibv_qp_attr attr{};
  attr.qp_state = IBV_QPS_RTS;
  attr.timeout = 14;
  attr.retry_cnt = 7;
  attr.rnr_retry = 7;
  attr.sq_psn = psn;
  attr.max_rd_atomic = 1;
  int flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
              IBV_QP_MAX_QP_RD_ATOMIC;
  int rc = ibv_modify_qp(qp, &attr, flags);
  if (rc != 0) {
    std::cerr << "ibv_modify_qp RTS failed: errno=" << errno << " (" << std::strerror(errno) << ")\n";
  }
  return rc == 0;
}

WireInfo BuildWire(const std::array<ibv_qp *, kPriorityCount> &qps, const RdmaResources &res, uint64_t base_addr,
                   const std::array<uint32_t, kPriorityCount> &psn, uint16_t lid, const ibv_gid &gid) {
  WireInfo info{};
  for (int i = 0; i < kPriorityCount; ++i) {
    info.qp_num[i] = qps[i]->qp_num;
    info.psn[i] = psn[i];
  }
  info.rkey = res.mr->rkey;
  info.addr = base_addr;
  info.lid = lid;
  std::memcpy(info.gid, gid.raw, sizeof(info.gid));
  return info;
}

void BringUpSide(const std::array<ibv_qp *, kPriorityCount> &local_qps, const WireInfo &remote_info,
                 const std::array<uint32_t, kPriorityCount> &local_psn, bool use_gid, uint8_t port,
                 uint8_t path_mtu, const Options &opts) {
  for (int i = 0; i < kPriorityCount; ++i) {
    RemoteQp remote{};
    remote.qp_num = remote_info.qp_num[i];
    remote.psn = remote_info.psn[i];
    remote.lid = remote_info.lid;
    std::memcpy(remote.gid, remote_info.gid, sizeof(remote.gid));
    const uint8_t prio = PriorityForIndex(i, opts);
    if (!MoveToRtr(local_qps[i], remote, port, prio, use_gid, path_mtu, opts.gid_index)) {
      throw std::runtime_error("MoveToRtr failed for priority " + std::to_string(i));
    }
    if (!MoveToRts(local_qps[i], local_psn[i])) {
      throw std::runtime_error("MoveToRts failed for priority " + std::to_string(i));
    }
  }
}

}  // namespace

int main(int argc, char **argv) {
  try {
    const std::string hostname = GetHostname();
    const std::string prefix = "[" + hostname + "] ";
    PrefixedLogs prefixed(prefix);
    std::cout << "log starts here\n";

    Options opts;
    try {
      ParseArgs(argc, argv, &opts);
    } catch (const std::runtime_error &e) {
      std::cerr << "Usage: " << argv[0]
                << " --next-host <host> [--oob-port 18515] [--rdma-device mlx5_0] [--rdma-port 1] "
                   "[--gid-index 0] [--rank 0] [--ranks 2] [--chunk-bytes 134217728] [--chunk-count 7] "
                   "[--compute-delay-us 1] [--uniform-priority] [--cpu-affinity N] [--verbose] "
                   "[--timing cycles|steady] [--cpu-mhz MHz]\n";
      return e.what() == std::string("help") ? 0 : 1;
    }

    PinThreadToCpu(opts.cpu_affinity, "main");
    Timer timer = InitTimer(opts);
    ibv_port_attr port_attr{};
    RdmaResources res = InitRdma(opts, &port_attr);
    ibv_gid gid{};
    if (ibv_query_gid(res.ctx, opts.rdma_port, opts.gid_index, &gid) != 0) {
      throw std::runtime_error("ibv_query_gid failed");
    }
    const bool use_gid = port_attr.link_layer == IBV_LINK_LAYER_ETHERNET;
    const uint8_t path_mtu = static_cast<uint8_t>(port_attr.active_mtu);
    const uint64_t max_seg_bytes =
        std::min<uint64_t>(port_attr.max_msg_sz, static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()));
    if (max_seg_bytes == 0) {
      throw std::runtime_error("port max_msg_sz is zero");
    }
    const uint64_t segments_per_chunk64 = (opts.chunk_bytes + max_seg_bytes - 1) / max_seg_bytes;
    if (segments_per_chunk64 > static_cast<uint64_t>(std::numeric_limits<int>::max() - 1)) {
      throw std::runtime_error("chunk_bytes too large for segment calculation");
    }
    const int segments_per_chunk = static_cast<int>(segments_per_chunk64);
    const int wrs_per_chunk = segments_per_chunk;
    const int min_recv_wr = 1;

    std::cout << "Device limits: max_mr_size=" << res.max_mr_size << " max_qp_wr=" << res.max_qp_wr
              << " max_cqe=" << res.max_cqe << " port_max_msg_sz=" << port_attr.max_msg_sz << "\n";
    std::cout << "Chunk sizing: chunk_bytes=" << opts.chunk_bytes << " segments_per_chunk=" << segments_per_chunk
              << " wrs_per_chunk=" << wrs_per_chunk << " buf_bytes="
              << (opts.chunk_bytes * static_cast<uint64_t>(opts.chunk_count)) << "\n";

    int cq_depth = kPriorityCount * (wrs_per_chunk + min_recv_wr) + 32;
    if (res.max_cqe != 0 && cq_depth > static_cast<int>(res.max_cqe)) {
      if (opts.verbose) {
        std::cerr << "Clamping CQ depth " << cq_depth << " to device max_cqe " << res.max_cqe << "\n";
      }
      cq_depth = static_cast<int>(res.max_cqe);
    }
    res.cq = ibv_create_cq(res.ctx, cq_depth, nullptr, nullptr, 0);
    if (!res.cq) throw std::runtime_error("ibv_create_cq failed");

    std::array<ibv_qp *, kPriorityCount> qps_rx{};
    std::array<ibv_qp *, kPriorityCount> qps_tx{};
    std::array<uint32_t, kPriorityCount> psn_rx{};
    std::array<uint32_t, kPriorityCount> psn_tx{};
    for (int i = 0; i < kPriorityCount; ++i) {
      const uint8_t prio = PriorityForIndex(i, opts);
      qps_rx[i] = CreatePriorityQp(res.pd, res.cq, opts.rdma_port, prio, wrs_per_chunk, min_recv_wr, res.max_qp_wr);
      qps_tx[i] = CreatePriorityQp(res.pd, res.cq, opts.rdma_port, prio, wrs_per_chunk, min_recv_wr, res.max_qp_wr);
      psn_rx[i] = FixedPsn(i, 0);
      psn_tx[i] = FixedPsn(i, 100);
    }

    WireInfo local_rx = BuildWire(qps_rx, res, reinterpret_cast<uint64_t>(res.recv_buf), psn_rx, port_attr.lid, gid);
    WireInfo local_tx = BuildWire(qps_tx, res, reinterpret_cast<uint64_t>(res.recv_buf), psn_tx, port_attr.lid, gid);
    const uint64_t local_seed = MarkerSeed(local_tx);
    InitBufferPattern(static_cast<uint8_t *>(res.send_buf), res.region_len, opts.chunk_bytes, opts.chunk_count,
                      local_seed);
    InitBufferPattern(static_cast<uint8_t *>(res.recv_buf), res.region_len, opts.chunk_bytes, opts.chunk_count,
                      local_seed);

    // Accept control channel from previous rank.
    int listen_fd = Listen(opts.oob_port);
    std::optional<WireInfo> from_prev;
    int prev_fd = -1;
    std::thread accept_thread([&]() {
      PinThreadToCpu(opts.cpu_affinity, "accept");
      int fd = AcceptOne(listen_fd);
      WireInfo incoming{};
      if (!RecvAll(fd, &incoming, sizeof(incoming))) {
        std::cerr << "Failed to receive from previous\n";
        ::close(fd);
        return;
      }
      if (!SendAll(fd, &local_rx, sizeof(local_rx))) {
        std::cerr << "Failed to send to previous\n";
        ::close(fd);
        return;
      }
      from_prev = incoming;
      prev_fd = fd;
    });

    // Connect to next rank.
    WireInfo from_next{};
    int next_fd = -1;
    {
      int fd = ConnectWithRetry(opts.next_host, opts.oob_port, 50, 200);
      if (!SendAll(fd, &local_tx, sizeof(local_tx))) {
        throw std::runtime_error("send to next failed");
      }
      if (!RecvAll(fd, &from_next, sizeof(from_next))) {
        throw std::runtime_error("recv from next failed");
      }
      next_fd = fd;
    }

    accept_thread.join();
    if (!from_prev || prev_fd < 0 || next_fd < 0) {
      throw std::runtime_error("did not receive handshake from previous rank");
    }
    ::close(listen_fd);

    BringUpSide(qps_tx, from_next, psn_tx, use_gid, opts.rdma_port, path_mtu, opts);
    BringUpSide(qps_rx, *from_prev, psn_rx, use_gid, opts.rdma_port, path_mtu, opts);

    const uint64_t prev_seed = MarkerSeed(*from_prev);
    std::cout << "Marker seeds: local=0x" << std::hex << local_seed << " prev=0x" << prev_seed << std::dec << "\n";

    std::cout << "RDMA control plane ready.\n";
    std::cout << "Device=" << opts.rdma_device << " port=" << static_cast<int>(opts.rdma_port)
              << " gid_index=" << static_cast<int>(opts.gid_index) << " gid=" << FormatGid(gid)
              << " active_mtu=" << static_cast<int>(port_attr.active_mtu) << "\n";
    std::cout << "chunk_bytes=" << opts.chunk_bytes << " chunk_count=" << opts.chunk_count
              << " compute_delay_us=" << opts.compute_delay_us << " uniform_priority=" << opts.uniform_priority
              << "\n";
    std::cout << "Next host=" << opts.next_host << " OOB port=" << opts.oob_port << "\n";
    std::cout << "Timing=" << (opts.timing == TimingSource::kCycles ? "cycles" : "steady");
    if (opts.timing == TimingSource::kCycles) {
      std::cout << " cpu_mhz=" << timer.cpu_mhz;
    }
    std::cout << "\n";
    std::cout << "QPs up for priorities 0-7 (tx to next and rx from prev).\n";
    if (use_gid) {
      std::cout << "DSCP mapping (prio->dscp): ";
      for (int i = 0; i < kPriorityCount; ++i) {
        const uint8_t prio = PriorityForIndex(i, opts);
        std::cout << i << "->" << static_cast<int>(DscpForPriority(prio))
                  << (i == kPriorityCount - 1 ? "" : " ");
      }
      std::cout << "\n";
    }

    std::cout << "Waiting for start barrier...\n";
    RingBarrier(prev_fd, next_fd, opts);
    std::cout << "Start barrier complete.\n";
    ::close(prev_fd);
    ::close(next_fd);

    timer = InitTimer(opts);
    const int total_chunks = opts.chunk_count;
    std::vector<uint64_t> send_ticks(total_chunks, 0);
    std::vector<uint64_t> recv_ticks(total_chunks, 0);
    std::vector<bool> send_set(total_chunks, false);
    std::vector<bool> recv_set(total_chunks, false);
    uint8_t *send_base = static_cast<uint8_t *>(res.send_buf);
    uint8_t *recv_base = static_cast<uint8_t *>(res.recv_buf);

    auto post_chunk = [&](int chunk_idx) {
      const int prio_idx = chunk_idx % kPriorityCount;
      ibv_qp *qp = qps_tx[prio_idx];
      const uint64_t offset = static_cast<uint64_t>(opts.chunk_bytes) * static_cast<uint64_t>(chunk_idx);
      uint64_t remaining = opts.chunk_bytes;
      uint64_t seg_offset = 0;
      std::vector<ibv_sge> sges(segments_per_chunk);
      std::vector<ibv_send_wr> wrs(wrs_per_chunk);

      for (int seg_idx = 0; seg_idx < segments_per_chunk; ++seg_idx) {
        const uint32_t len = static_cast<uint32_t>(std::min<uint64_t>(remaining, max_seg_bytes));
        sges[seg_idx].addr = reinterpret_cast<uint64_t>(send_base) + offset + seg_offset;
        sges[seg_idx].length = len;
        sges[seg_idx].lkey = res.mr->lkey;

        wrs[seg_idx].wr_id = (static_cast<uint64_t>(chunk_idx) << 32) | static_cast<uint32_t>(seg_idx);
        wrs[seg_idx].next = (seg_idx + 1 < segments_per_chunk) ? &wrs[seg_idx + 1] : nullptr;
        wrs[seg_idx].sg_list = &sges[seg_idx];
        wrs[seg_idx].num_sge = 1;
        wrs[seg_idx].opcode = IBV_WR_RDMA_WRITE;
        wrs[seg_idx].send_flags = 0;
        wrs[seg_idx].wr.rdma.remote_addr = from_next.addr + offset + seg_offset;
        wrs[seg_idx].wr.rdma.rkey = from_next.rkey;

        seg_offset += len;
        remaining -= len;
      }

      ibv_send_wr *bad = nullptr;
      if (ibv_post_send(qp, &wrs[0], &bad) != 0) {
        throw std::runtime_error("ibv_post_send failed for chunk " + std::to_string(chunk_idx));
      }
      const uint64_t post_ts = timer.Now();
      send_ticks[chunk_idx] = post_ts;
      send_set[chunk_idx] = true;
      const double post_ms = static_cast<double>(post_ts) / timer.ticks_per_ms;
      std::cout << "Send chunk " << chunk_idx << " posted at t=" << post_ms << "ms ticks=" << post_ts << "\n";
      if (opts.verbose) {
        std::cout << "Posted chunk " << chunk_idx << " on prio_idx=" << prio_idx << " offset=" << offset
                  << " size=" << opts.chunk_bytes << " segments=" << segments_per_chunk << "\n";
      }
    };

    // Kick off the first chunk immediately.
    post_chunk(0);
    const size_t marker_size = MarkerSizeForChunk(opts.chunk_bytes);
    for (int chunk_idx = 0; chunk_idx < total_chunks; ++chunk_idx) {
      const size_t offset = static_cast<size_t>(opts.chunk_bytes) * static_cast<size_t>(chunk_idx);
      const auto marker_bytes = MarkerBytes(ChunkMarkerValue(prev_seed, chunk_idx));
      std::cout << "Waiting for chunk " << chunk_idx << " marker seed=0x" << std::hex << prev_seed << std::dec
                << "\n"
                << std::flush;
      uint64_t last_log = timer.Now();
      uint64_t wait_start = last_log;
      uint64_t spins = 0;
      while (!ChunkHasMarker(recv_base, offset, opts.chunk_bytes, marker_bytes, marker_size)) {
        ++spins;
        // CpuRelax();
        const uint64_t now = timer.Now();
        if (now - last_log >= static_cast<uint64_t>(timer.ticks_per_ms * 1000.0)) {
          last_log = now;
          FlushMarkerLines(recv_base, offset, opts.chunk_bytes, marker_size);
          const uint64_t head = ReadMarkerValue(recv_base, offset, marker_size);
          const uint64_t tail = ReadMarkerValue(recv_base, offset + opts.chunk_bytes - marker_size, marker_size);
          const uint64_t expected = ChunkMarkerValue(prev_seed, chunk_idx);
          const uint64_t observed_seed = head & 0xffffffff00000000ull;
          const uint64_t local_masked = local_seed & 0xffffffff00000000ull;
          const uint64_t prev_masked = prev_seed & 0xffffffff00000000ull;
          const char *observed_label = "unknown";
          if (observed_seed == local_masked) {
            observed_label = "local";
          } else if (observed_seed == prev_masked) {
            observed_label = "prev";
          }
          const double elapsed_ms = static_cast<double>(now - wait_start) / timer.ticks_per_ms;
          std::cout << "Still waiting for chunk " << chunk_idx << " expected=0x" << std::hex << expected
                    << " head=0x" << head << " tail=0x" << tail << " observed=" << observed_label << std::dec
                    << " spins=" << spins << " elapsed_ms=" << elapsed_ms << "\n";
        }
      }
      recv_ticks[chunk_idx] = timer.Now();
      recv_set[chunk_idx] = true;
      if (opts.verbose) {
        std::cout << "Chunk " << chunk_idx << " data observed in memory\n";
      }
      std::this_thread::sleep_for(std::chrono::microseconds(opts.compute_delay_us));
      if (chunk_idx + 1 < total_chunks) {
        post_chunk(chunk_idx + 1);
      }
    }

    double total_ms = static_cast<double>(timer.Now()) / timer.ticks_per_ms;
    std::cout << "Completed " << total_chunks << " chunks. Total time (ms): " << total_ms << "\n";
    for (int i = 0; i < total_chunks; ++i) {
      double send_ms = send_set[i] ? static_cast<double>(send_ticks[i]) / timer.ticks_per_ms : -1.0;
      double recv_ms = recv_set[i] ? static_cast<double>(recv_ticks[i]) / timer.ticks_per_ms : -1.0;
      std::cout << "Chunk " << i << " send_ms=" << send_ms << " recv_ms=" << recv_ms << "\n";
    }
    std::cout << "Waiting 5s for remaining traffic to drain...\n";
    std::this_thread::sleep_for(std::chrono::seconds(5));
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << "\n";
    return 1;
  }
}
