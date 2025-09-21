#pragma once


int create_data_socket(const struct sockaddr_in &peer_addr, bool use_tcp = false);
int tcp_wait_for_connection(int listen_sock);
