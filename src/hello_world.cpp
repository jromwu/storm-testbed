#include <arpa/inet.h>
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

void SendAll(int fd, const std::string &msg) {
  const char *p = msg.data();
  size_t left = msg.size();
  while (left > 0) {
    ssize_t rc = ::send(fd, p, left, 0);
    if (rc <= 0) throw std::runtime_error("send failed");
    p += rc;
    left -= static_cast<size_t>(rc);
  }
}

std::string RecvOnce(int fd) {
  char buf[1024];
  ssize_t rc = ::recv(fd, buf, sizeof(buf) - 1, 0);
  if (rc < 0) throw std::runtime_error("recv failed");
  buf[rc] = '\0';
  return std::string(buf);
}

std::string Hostname() {
  char buf[256];
  if (::gethostname(buf, sizeof(buf)) != 0) {
    return "unknown";
  }
  buf[sizeof(buf) - 1] = '\0';
  return std::string(buf);
}

}  // namespace

int main(int argc, char **argv) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <peer-host> [port]\n";
    return 1;
  }
  const std::string peer = argv[1];
  const uint16_t port = (argc >= 3) ? static_cast<uint16_t>(std::stoi(argv[2])) : 18516;

  int listen_fd = Listen(port);
  std::thread accept_thread([&]() {
    int fd = AcceptOne(listen_fd);
    std::string msg = RecvOnce(fd);
    std::cout << "Received: " << msg << "\n";
    ::close(fd);
  });

  int out_fd = ConnectWithRetry(peer, port, 50, 100);
  const std::string msg = "hello from " + Hostname() + "\n";
  SendAll(out_fd, msg);
  ::shutdown(out_fd, SHUT_WR);
  ::close(out_fd);

  accept_thread.join();
  ::close(listen_fd);
  std::cout << "Done.\n";
  return 0;
}
