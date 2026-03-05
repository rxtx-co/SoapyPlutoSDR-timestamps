#pragma once

#include <vector>
#include <thread>
#include <atomic>

#include <iio.h>

#include <sys/socket.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include <libusb.h>
#pragma GCC diagnostic pop

#include <SoapySDR/Types.hpp>

#include "PlutoSDR_StreamFormat.hpp"
#include "PlutoSDR_RXStreamer.hpp"

#include "FIFO.hpp"

#include "sdr_ip_gadget_types.h"
#include "sdr_ip_gadget_private_types.h"

class rx_streamer_ip_gadget : public rx_streamer {
	public:
		rx_streamer_ip_gadget(const iio_context *iio_ctx, const iio_device *dev,
				int sock_control, size_t udp_packet_size,
				const plutosdrStreamFormat format, const std::vector<size_t> &channels,
				const SoapySDR::Kwargs &args, uint32_t timestamp_every);
		~rx_streamer_ip_gadget();

		size_t recv(void * const *buffs,
					const size_t numElems,
					int &flags,
					long long &timeNs,
					const long timeoutUs=100000);

		int start(const int flags,
				  const long long timeNs,
				  const size_t numElems);

		int stop(const int flags,
				 const long long timeNs=100000);

		void set_buffer_size_by_samplerate(const size_t _samplerate);

		size_t get_mtu_size();

	private:
		// IIO device
		const iio_context *iio_ctx;
		const iio_device *dev;

		// IP gadget
		int sock_control;
		int sock_data;
		size_t udp_packet_size;

		// report stream failure
		bool _failed;

		// Sample format
		const plutosdrStreamFormat format;

		// How often to expect a timestamp in the stream
		uint32_t timestamp_every;
		uint32_t timestamp_clock_rate;
		uint32_t timestamp_increment;

		// Read thread
		std::thread thread;

		// Stop flag
		std::atomic<bool> thread_stop;

		// Queue
		TSQueue<std::shared_ptr<seq_payload_t>> queue;

		// IIO channel list
		std::vector<iio_channel*> channel_list;

		// Channel bitmask
		uint32_t enabled_channels;

		// Sample rate
		long long sample_rate;

		// Expected sample size (bytes)
		uint32_t sample_size_bytes;

		// Fixed buffer size (user supplies or set due to timestamp)
		bool fixed_buffer_size;

		// Buffer size (samples)
		uint32_t buffer_size_samples;

		// Direct copy supported
		bool direct_copy;

		// Current buffer being processed by receive
		std::shared_ptr<seq_payload_t> curr_buffer;
		size_t curr_buffer_samples_remaining;
		size_t curr_buffer_offset;
		uint64_t curr_buffer_timestamp;

		// Update buffer size, restarting stream if required
		void set_buffer_size(const size_t new_buffer_size_samples);

		//
		bool timestamp_insert_extra;
		uint32_t encoder_uart_baud_rate;
		void sdr_set_timestamp_insert_extra(uint32_t timestamp_insert_extra);
		void sdr_set_uart_baud_rate(uint32_t baud_rate);

		// Read thread - fetches data from USB device and places into fifo
		void thread_func(uint32_t curr_enabled_channels, uint32_t curr_buffer_size_samples);

		// Private start / stop functions
		void _start(void);
		void _stop(void);

		int udp_recv(data_ip_hdr_t *hdr, uint8_t *payload, size_t buffer_offset);
		int tcp_recv(data_ip_hdr_t *hdr, uint8_t *payload, size_t buffer_offset);
		int tcp_recv_data(int sock, uint8_t *buffer, size_t size);
		int tcp_stream_resync(int sock, data_ip_hdr_t *hdr, uint8_t *buffer, size_t buffer_size, size_t search_limit);

		int check_state(data_ip_hdr_t *hdr);

		int _data_fd;
		bool _transport_tcp;
		size_t _buffer_size;

		struct {
			struct msghdr msg;
			struct iovec iov[2];
		} _udp;

		struct {
			uint32_t buffer_size_samples;
			size_t buffer_size;

			size_t pkt_count;

			// Track used buffer space
			size_t buffer_used;

			// Track buffer index and count
			uint16_t block_index;
			uint16_t block_count;

			// Track timestamps
			uint64_t last_seqno;
		} _state;
};
