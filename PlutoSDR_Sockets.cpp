#include <cstring>
#include <stdexcept>
#include <unistd.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>


#include <SoapySDR/Logger.hpp>

#include "sdr_ip_gadget_types.h"

int create_data_socket(const struct sockaddr_in &peer_addr, bool use_tcp = false)
{
	int sock_data = -1;

	sock_data = socket(AF_INET, use_tcp ? SOCK_STREAM : SOCK_DGRAM, 0);
	if (-1 == sock_data) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "failed to open data socket");
		throw std::runtime_error("failed to open data socket");
	}

	// Bind data socket, such that we can tell the server where to direct rx data to
	struct sockaddr_in addr;

	memset(&addr, 0x00, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY;
	addr.sin_port = 0;  // Let the system choose an available port
	if (bind(sock_data, (struct sockaddr*)&addr, sizeof(addr))) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "failed to bind data socket");
		throw std::runtime_error("failed to bind data socket");
	}

	if (!use_tcp) {
		addr.sin_family = AF_INET;
		addr.sin_addr.s_addr = peer_addr.sin_addr.s_addr;
		addr.sin_port = htons(DIRECT_IP_PORT_DATA);
		if (connect(sock_data, (struct sockaddr*)&addr, sizeof(addr))) {
			SoapySDR_logf(SOAPY_SDR_ERROR, "failed to connect UDP data socket");
			throw std::runtime_error("failed to connect UDP data socket");
		}
	}

	// Set receive timeout on data socket
	struct timeval timeout;
	timeout.tv_sec = 1;
	timeout.tv_usec = 0;
	if (setsockopt(sock_data, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "failed to set data socket receive timeout");
		throw std::runtime_error("failed to set data socket receive timeout");
	}
	timeout.tv_sec = 1;
	timeout.tv_usec = 0;
	if (setsockopt(sock_data, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) < 0) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "failed to set data socket send timeout");
		throw std::runtime_error("failed to set data socket send timeout");
	}

	// Get the current receive buffer size
	int recv_size, send_size;
	socklen_t size_len;

	size_len = sizeof(recv_size);
	if (getsockopt(sock_data, SOL_SOCKET, SO_RCVBUF, &recv_size, &size_len) < 0) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "failed to get data socket receive buffer size");
		throw std::runtime_error("failed to get data socket receive buffer size");
	}
	size_len = sizeof(send_size);
	if (getsockopt(sock_data, SOL_SOCKET, SO_SNDBUF, &send_size, &size_len) < 0) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "failed to get data socket send buffer size");
		throw std::runtime_error("failed to get data socket send buffer size");
	}
	SoapySDR_logf(SOAPY_SDR_INFO, "Currect socket buffer size for receive = %d send = %d", recv_size, send_size);

	// Set the receive buffer size
	recv_size = 8*1024*1024;
	if (setsockopt(sock_data, SOL_SOCKET, SO_RCVBUF, &recv_size, sizeof(recv_size)) < 0) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "failed to set data socket receive buffer size");
		throw std::runtime_error("failed to set data socket receive buffer size");
	}
	size_len = sizeof(recv_size);
	if (getsockopt(sock_data, SOL_SOCKET, SO_RCVBUF, &recv_size, &size_len) < 0) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "failed to get data socket receive buffer size");
		throw std::runtime_error("failed to get data socket receive buffer size");
	}

	// Set the send buffer size
	send_size = 8*1024*1024;
	if (setsockopt(sock_data, SOL_SOCKET, SO_SNDBUF, &send_size, sizeof(send_size)) < 0) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "failed to set data socket send buffer size");
		throw std::runtime_error("failed to set data socket send buffer size");
	}
	size_len = sizeof(send_size);
	if (getsockopt(sock_data, SOL_SOCKET, SO_SNDBUF, &send_size, &size_len) < 0) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "failed to get data socket send buffer size");
		throw std::runtime_error("failed to get data socket send buffer size");
	}

	SoapySDR_logf(SOAPY_SDR_INFO, "Updated socket buffer size for receive = %d send = %d", recv_size, send_size);

	fcntl(sock_data, F_SETFL, fcntl(sock_data, F_GETFL, 0) & ~O_NONBLOCK);

	return sock_data;
}

int tcp_wait_for_connection(int listen_sock)
{
	int client_sock = -1;

	SoapySDR_logf(SOAPY_SDR_DEBUG, "Waiting for TCP data connection");
	if (listen(listen_sock, 10) < 0) {
		if ((EWOULDBLOCK == errno) || (EAGAIN == errno))
			return 0;
		SoapySDR_logf(SOAPY_SDR_ERROR, "listen() failed %s(%d)", strerror(errno), errno);
		return -1;
	}

	struct sockaddr_in client_addr;
	socklen_t client_addr_len = sizeof(client_addr);

	client_sock = accept(listen_sock, (struct sockaddr *)&client_addr, &client_addr_len);
	if (client_sock < 0) {
		if ((EWOULDBLOCK == errno) || (EAGAIN == errno))
			return 0;
		SoapySDR_logf(SOAPY_SDR_ERROR, "accept() failed %s(%d)", strerror(errno), errno);
		return -1;
	}

	SoapySDR_logf(SOAPY_SDR_DEBUG, "Accepted TCP data connection");

	int recv_size = 8*1024*1024;
	if (setsockopt(client_sock, SOL_SOCKET, SO_RCVBUF, &recv_size, sizeof(recv_size)) < 0) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "failed to set data socket (client) receive buffer size");
		return -1;
	}
	int send_size = 8*1024*1024;
	if (setsockopt(client_sock, SOL_SOCKET, SO_SNDBUF, &send_size, sizeof(send_size)) < 0) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "failed to set data socket (client) send buffer size");
		return -1;
	}

	// Set receive timeout on data socket
	struct timeval timeout;
	timeout.tv_sec = 1;
	timeout.tv_usec = 0;
	if (setsockopt(client_sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "failed to set data socket (client) receive timeout");
		return -1;
	}
	timeout.tv_sec = 1;
	timeout.tv_usec = 0;
	if (setsockopt(client_sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) < 0) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "failed to set data socket (client) send timeout");
		return -1;
	}

	int optval = 1;
	setsockopt(client_sock, SOL_SOCKET, SO_KEEPALIVE, &optval, sizeof(optval));
	optval = 1;
	setsockopt(client_sock, IPPROTO_TCP, TCP_KEEPIDLE, &optval, sizeof(optval));
	optval = 1;
	setsockopt(client_sock, IPPROTO_TCP, TCP_KEEPINTVL, &optval, sizeof(optval));
	optval = 5;
	setsockopt(client_sock, IPPROTO_TCP, TCP_KEEPCNT, &optval, sizeof(optval));
	//optval = 1;
	//setsockopt(client_sock, IPPROTO_TCP, TCP_QUICKACK, &optval, sizeof(optval));

	fcntl(client_sock, F_SETFL, fcntl(client_sock, F_GETFL, 0) & ~O_NONBLOCK);

	return client_sock;
}
