#include <cstring>
#include <cinttypes>

#include <sched.h>
#include <pthread.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <netinet/tcp.h>

#include "PlutoSDR_RXStreamerIPGadget.hpp"

#include <SoapySDR/Device.hpp>
#include <SoapySDR/Logger.hpp>
#include <SoapySDR/Types.hpp>
#include <SoapySDR/Formats.hpp>
#include <SoapySDR/Time.hpp>

#include "PlutoSDR_TimestampEvery.hpp"
#include "PlutoSDR_Sockets.hpp"

#include "sdr_ip_gadget_types.h"

rx_streamer_ip_gadget::rx_streamer_ip_gadget(const iio_device *_dev, int _sock_control, size_t _udp_packet_size, const plutosdrStreamFormat _format, const std::vector<size_t> &channels, const SoapySDR::Kwargs &args, uint32_t _timestamp_every):
	dev(_dev), sock_control(_sock_control), udp_packet_size(_udp_packet_size), format(_format), timestamp_every(_timestamp_every), thread_stop(false), queue(16, true)

{
	_failed = false;
	sock_data = -1;
	_data_fd = -1;

	//default to channel 0, if none were specified
	const std::vector<size_t> &channelIDs = channels.empty() ? std::vector<size_t>{0} : channels;

	// This doesn't look right in general....but works for the purposes of enabling a single channel here
	enabled_channels = 0;
	for (unsigned int i = 0; i < channelIDs.size() * 2; i++) {
		struct iio_channel *chn = iio_device_get_channel(dev, i);

		// Store channel for sample conversions later
		channel_list.push_back(chn);

		// Add bit to enabled channels
		enabled_channels |= (1 << i);
	}

	// Retrieve sample rate
	iio_channel_attr_read_longlong(iio_device_find_channel(dev, "voltage0", false),"sampling_frequency", &sample_rate);

	// Calculate expected sample size
	sample_size_bytes = channel_list.size() * sizeof(uint16_t);

	// If timestamp is enable, what is the clock rate
	timestamp_clock_rate = 0;
	if ((timestamp_every > 0)
		&& args.count("timestamp_clock_rate"))
	{
		uint32_t val;
		try {
			val = std::stoi(args.at("timestamp_clock_rate"));
		} catch (const std::invalid_argument &) {
			SoapySDR_logf(SOAPY_SDR_ERROR, "bad timestamp_clock_rate provided");
			throw std::runtime_error("bad timestamp_clock_rate provided\n");
		}
		if (val > 0)
			timestamp_clock_rate = val;
		SoapySDR_logf(SOAPY_SDR_INFO, "Timestamp clock rate: %d", timestamp_clock_rate);
	}

	// Assume buffer size will be supplied by user
	fixed_buffer_size = true;

	// Calculate buffer length
	if (args.count("bufflen") != 0) {
		// Buffer length provided
		size_t buffer_length;

		// Convert argument from string to integer
		try {
			buffer_length = std::stoi(args.at("bufflen"));

		} catch (const std::invalid_argument &) {
			SoapySDR_logf(SOAPY_SDR_ERROR, "bad bufflen provided");
			throw std::runtime_error("bad bufflen provided\n");
		}

		// If buffer length provided, while timestamping enabled, check that two values are equal
		// for now every buffer is expected to have a timestamp
		if (    (buffer_length > 0)
			 && (timestamp_every > 0)
		   ) {
			SoapySDR_logf(SOAPY_SDR_ERROR, "bufflen provided incompatible with timestamp_every");
			throw std::runtime_error("bufflen provided incompatible with timestamp_every\n");
		}

		// Set length
		set_buffer_size(buffer_length);

	} else if (timestamp_every > 0) {
		// Buffer length set based on timestamping
		set_buffer_size(timestamp_every);

	} else {
		// Buffer length not provided and timestamping disabled, calculate based on sample rate
		fixed_buffer_size = false;
		set_buffer_size_by_samplerate(sample_rate);
	}

	// Setup timestamping
	SoapyPlutoSDR_TimestampEvery::update_device_timestamp_every(dev, timestamp_every, channel_list.size());

	// Assume direct copying is supported
	direct_copy = true;

	// Transport TCP or UDP
	_transport_tcp = false;
	if (args.count("transport") && args.at("transport") == "tcp")
		_transport_tcp = true;

	// Direct copy only supported for a single channel (of I + Q samples)
	if (channel_list.size() != 2) direct_copy = false;

	// Check endianess
	int16_t test_dst, test_src = 0x1234;
	iio_channel_convert(channel_list[0], &test_dst, (const void *)&test_src);
	if (test_src != test_dst) direct_copy = false;

	// Report status
	SoapySDR_logf(SOAPY_SDR_INFO, "Has direct RX copy: %d", (int)direct_copy);

	// Create data socket
	struct sockaddr_in peer_addr;
	socklen_t peer_addr_len = sizeof(peer_addr);
	if (getpeername(sock_control, (struct sockaddr *)&peer_addr, &peer_addr_len)) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "failed to get control socket peer address");
		throw std::runtime_error("failed to get control socket peer address");
	}

	sock_data = create_data_socket(peer_addr, _transport_tcp);
}

rx_streamer_ip_gadget::~rx_streamer_ip_gadget()
{
	if (thread.joinable()) {
		_stop();
	}
}

size_t rx_streamer_ip_gadget::recv(void * const *buffs,
									const size_t numElems,
									int &flags,
									long long &timeNs,
									const long timeoutUs)
{
	if (!curr_buffer) {
		// Need to dequeue a new buffer
		if (!queue.pop(curr_buffer, (timeoutUs > 0), timeoutUs)) {
			if (_failed)
				return SOAPY_SDR_STREAM_ERROR;
			// Failed to dequeue buffer within timeout
			return SOAPY_SDR_TIMEOUT;
		}

		// Calculate items in buffer
		curr_buffer_samples_remaining = curr_buffer->payload.size() / sample_size_bytes;

		// Reset offset
		curr_buffer_offset = 0;

		// Grab timestamp or sequence number
		curr_buffer_timestamp = curr_buffer->seqno;
	}

	// Work out how many items to copy
	size_t items = std::min(curr_buffer_samples_remaining, numElems);

	if (direct_copy) {
		// optimize for single RX, 2 channel (I/Q), same endianess direct copy
		// note that RX is 12 bits LSB aligned, i.e. fullscale 2048
		uint8_t *src = curr_buffer->payload.data() + curr_buffer_offset;
		int16_t const *src_ptr = (int16_t *)src;

		if (format == PLUTO_SDR_CS16) {

			::memcpy(buffs[0], src_ptr, 2 * sizeof(int16_t) * items);

		}
		else if (format == PLUTO_SDR_CF32) {

			float *dst_cf32 = (float *)buffs[0];

			for (size_t index = 0; index < items * 2; ++index) {
				*dst_cf32 = float(*src_ptr) / 2048.0f;
				src_ptr++;
				dst_cf32++;
			}

		}
		else if (format == PLUTO_SDR_CS12) {

			int8_t *dst_cs12 = (int8_t *)buffs[0];

			for (size_t index = 0; index < items; ++index) {
				int16_t i = *src_ptr++;
				int16_t q = *src_ptr++;
				// produce 24 bit (iiqIQQ), note the input is LSB aligned, scale=2048
				// note: byte0 = i[7:0]; byte1 = {q[3:0], i[11:8]}; byte2 = q[11:4];
				*dst_cs12++ = uint8_t(i);
				*dst_cs12++ = uint8_t((q << 4) | ((i >> 8) & 0x0f));
				*dst_cs12++ = uint8_t(q >> 4);
			}
		}
		else if (format == PLUTO_SDR_CS8) {

			int8_t *dst_cs8 = (int8_t *)buffs[0];

			for (size_t index = 0; index < items * 2; index++) {
				*dst_cs8 = int8_t(*src_ptr >> 4);
				src_ptr++;
				dst_cs8++;
			}
		}
	} else {
		int16_t conv = 0, *conv_ptr = &conv;

		for (unsigned int i = 0; i < channel_list.size(); i++) {
			iio_channel *chn = channel_list[i];
			unsigned int index = i / 2;

			uint8_t *src = curr_buffer->payload.data() + curr_buffer_offset + (sizeof(uint16_t) * i);

			if (format == PLUTO_SDR_CS16) {

				int16_t *dst_cs16 = (int16_t *)buffs[index];

				for (size_t j = 0; j < items; ++j) {
					iio_channel_convert(chn, conv_ptr, src);
					src += sample_size_bytes;
					dst_cs16[j * 2 + (i % 2)] = conv;
				}
			}
			else if (format == PLUTO_SDR_CF32) {

				float *dst_cf32 = (float *)buffs[index];

				for (size_t j = 0; j < items; ++j) {
					iio_channel_convert(chn, conv_ptr, src);
					src += sample_size_bytes;
					dst_cf32[j * 2 + (i % 2)] = float(conv) / 2048.0f;
				}
			}
			else if (format == PLUTO_SDR_CS8) {

				int8_t *dst_cs8 = (int8_t *)buffs[index];

				for (size_t j = 0; j < items; ++j) {
					iio_channel_convert(chn, conv_ptr, src);
					src += sample_size_bytes;
					dst_cs8[j * 2 + (i % 2)] = int8_t(conv >> 4);
				}
			}

		}
	}

	// Decrement items in buffer and advance byte offset
	curr_buffer_samples_remaining -= items;
	curr_buffer_offset += items * sample_size_bytes;

	// Free buffer when all items used
	if (0 == curr_buffer_samples_remaining) curr_buffer.reset();

	// Set flags
	flags = 0;
	if (timestamp_every > 0)
	{
		// Timestamp present
		flags |= SOAPY_SDR_HAS_TIME;
		timeNs = SoapySDR::ticksToTimeNs(curr_buffer_timestamp, sample_rate);
	}

	// Increment timestamp ticks or sequence number
	curr_buffer_timestamp += items;

	// Return number of samples copied
	return items;
}

int rx_streamer_ip_gadget::start(const int flags,
								  const long long timeNs,
								  const size_t numElems)
{
	// Issue start command
	_start();

	return 0;
}

int rx_streamer_ip_gadget::stop(const int flags,
								 const long long timeNs)
{
	// Issue stop command
	_stop();

	return 0;
}

void rx_streamer_ip_gadget::set_buffer_size_by_samplerate(const size_t samplerate)
{
	// Store new sample rate
	sample_rate = samplerate;

	// Skip update if user has supplied a fixed buffer size
	if (fixed_buffer_size) return;

	//Adapt buffer size (= MTU) as a tradeoff to minimize readStream overhead but at
	//the same time allow realtime applications. Keep it a power of 2 which seems to be better.
	//so try to target very roughly 60fps [30 .. 100] readStream calls / s for realtime applications.
	int rounded_nb_samples_per_call = (int)::round(samplerate / 60.0);

	int power_of_2_nb_samples = 0;

	while (rounded_nb_samples_per_call > (1 << power_of_2_nb_samples)) {
		power_of_2_nb_samples++;
	}

	set_buffer_size(1 << power_of_2_nb_samples);

	SoapySDR_logf(SOAPY_SDR_INFO, "Auto setting Buffer Size: %lu", (unsigned long)buffer_size_samples);
}

size_t rx_streamer_ip_gadget::get_mtu_size()
{
	// Return size of buffer data area
	return buffer_size_samples;
}

void rx_streamer_ip_gadget::set_buffer_size(const size_t new_buffer_size_samples)
{
	if (buffer_size_samples != new_buffer_size_samples) {
		// Is thread currently running?
		bool was_running = thread.joinable();

		// Buffer size changing
		if (was_running) {
			// Stop stream
			_stop();
		}

		// Save new buffer size
		buffer_size_samples = new_buffer_size_samples;

		if (was_running) {
			// Start stream
			_start();
		}
	}
}

void rx_streamer_ip_gadget::thread_func(uint32_t curr_enabled_channels, uint32_t curr_buffer_size_samples)
{
	std::memset(&_state, 0x00, sizeof(_state));

	// Calculate buffer size in bytes
	_state.buffer_size_samples = curr_buffer_size_samples;
	_state.buffer_size = curr_buffer_size_samples * sample_size_bytes;

	/* Update timestamp increment */
	if (timestamp_every > 0) {
		timestamp_increment = timestamp_clock_rate > 0
				? (__int128_t(timestamp_every) * timestamp_clock_rate) / sample_rate
				: timestamp_every;
	} else {
		timestamp_increment = curr_buffer_size_samples;
	}

	// Allocate temporary header
	data_ip_hdr_t hdr;

	// Allocate buffer
	std::shared_ptr<seq_payload_t> buffer = std::make_shared<seq_payload_t>();
	buffer->payload.resize(_state.buffer_size);

	// Create shorthand buffer pointer
	uint8_t *payload = buffer->payload.data();


	/* Prepare network socket */
	if (_transport_tcp) {
		_data_fd = -1;
	} else {
		std::memset(&_udp.msg, 0x00, sizeof(_udp.msg));
		_udp.msg.msg_iov = _udp.iov;
		_udp.msg.msg_iovlen = 2;
		_udp.iov[0].iov_len = sizeof(hdr);
		_udp.iov[0].iov_base = &hdr;

		_data_fd = sock_data;
	}


	/* ----------------------------------------------------
	** Start stream
	*/
	// Retrieve data socket port
	struct sockaddr_in addr;
	socklen_t addr_len = sizeof(addr);
	if (getsockname(sock_data, (struct sockaddr*)&addr, &addr_len) == -1) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "Error getting socket name");
		return;
	}

	// send start command
	cmd_ip_t cmd;
	cmd.hdr.magic = SDR_IP_GADGET_MAGIC;
	cmd.hdr.cmd = SDR_IP_GADGET_COMMAND_START_RX;
	cmd.start_rx.data_port = ntohs(addr.sin_port);
	cmd.start_rx.enabled_channels = curr_enabled_channels;
	cmd.start_rx.timestamping_enabled = (timestamp_every > 0);
	cmd.start_rx.timestamp_increment = timestamp_increment;
	cmd.start_rx.transport_tcp = _transport_tcp;
	cmd.start_rx.buffer_size_samples = curr_buffer_size_samples;
	cmd.start_rx.packet_size = udp_packet_size;

	int rc = sendto(sock_control, &cmd, sizeof(cmd.start_rx), 0, NULL, 0);
	if (rc < 0) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "Failed to send start RX stream cmd (%d)", rc);
		return;
	}
	// ----------------------------------------------------


	/* Main RX loop */ 
	// Keep running until told to stop
	while (!thread_stop.load()) {
		/* TCP: wait for remote peer fo connect */
		if (_data_fd < 0) {
			if ((_data_fd = tcp_wait_for_connection(sock_data)) < 0) {
				_failed = true;
				break;
			}
			continue;
		}

		/* Receive new block of data */
		int rc;
		if (_transport_tcp) {
			rc = tcp_recv(&hdr, &payload[_state.buffer_used], _state.buffer_size - _state.buffer_used);
		} else {
			rc = udp_recv(&hdr, &payload[_state.buffer_used], udp_packet_size - sizeof(data_ip_hdr_t));
		}
		if (rc <= 0) {
			if (rc == 0)
				continue;
			_failed = true;
			break;
		}

		/* Check data integrity */
		if (check_state(&hdr) < 0)
			continue;

		/* Update buffer used */
		_state.buffer_used += (size_t)rc;

		/* Is buffer full? */
		if (_state.buffer_size == _state.buffer_used)
		{
			// Push buffer into fifo without blocking
			buffer->seqno = hdr.seqno;
			queue.push(buffer, false, 0);

			// Create new buffer
			buffer = std::make_shared<seq_payload_t>();
			buffer->payload.resize(_state.buffer_size);

			// Reset buffer used
			_state.buffer_used = 0;

			// Update pointer
			payload = buffer->payload.data();
		}
	}

	close(sock_data);
	if (_data_fd != sock_data)
		close(_data_fd);
	sock_data = _data_fd = -1;

	// Stop stream
	cmd.hdr.magic = SDR_IP_GADGET_MAGIC;
	cmd.hdr.cmd = SDR_IP_GADGET_COMMAND_STOP_RX;
	rc = send(sock_control, &cmd, sizeof(cmd.stop), 0);
	if (rc < 0) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "Failed to send stop RX stream cmd (%d)", rc);
	}
}

int rx_streamer_ip_gadget::check_state(data_ip_hdr_t *hdr)
{
	_state.pkt_count++;
	if (0) {
		SoapySDR_logf(SOAPY_SDR_DEBUG, "packet pkt=%" PRIu64
				" hdr.seqno=%" PRIu64
				" hdr.block_index=%" PRIu64
				" hdr.block_count=%" PRIu64,
				_state.pkt_count,
				hdr->seqno, 
				hdr->block_index,
				hdr->block_count);
	}

	// Are we starting a new buffer
	if (0 == _state.buffer_used)
	{
		/* Check packet starts sequence */
		if (0 != hdr->block_index)
		{
			/* Drop packet, waiting for sequence start */
			SoapySDR_logf(SOAPY_SDR_WARNING, "Drop packet: waiting for sequency start:"
						" pkt=%" PRIu64
						" hdr.block_index=%" PRIu64
						" hdr.block_count=%" PRIu64
						" hdr.seqno=%" PRIu64,
						_state.pkt_count,
						hdr->block_index,
						hdr->block_count,
						hdr->seqno);
			return -1;
		}

		if (hdr->seqno < _state.last_seqno)
		{
			SoapySDR_logf(SOAPY_SDR_WARNING, "Time wrap :: "
							" pkt=%" PRIu64
							" hdr.seqno=%" PRIu64
							" - last_seqno=%" PRIu64
							" = %" PRIu64,
							_state.pkt_count,
							hdr->seqno, _state.last_seqno,
							_state.last_seqno - hdr->seqno);
		} else
		if (_state.pkt_count > 1 &&
			(
				((timestamp_every  > 0) && (abs(int64_t(hdr->seqno - _state.last_seqno) - timestamp_increment) > 1))
				||
				((timestamp_every == 0) && (hdr->seqno - _state.last_seqno) != _state.buffer_size_samples)
			))
		{
			SoapySDR_logf(SOAPY_SDR_DEBUG, "Timestamp gap:"
						" pkt=%" PRIu64
						" last_seqno=%" PRIu64
						" hdr.seqno=%" PRIu64
						" delta=%" PRId64
						" timestamp_increment=%" PRIu64,
						_state.pkt_count,
						_state.last_seqno,
						hdr->seqno,
						hdr->seqno - _state.last_seqno,
						timestamp_increment);
		}

		/* Reset index and store total */
		_state.block_index = 0;
		_state.block_count = hdr->block_count;

		/* timestamp from header to working data */
		_state.last_seqno = hdr->seqno;

		if (0) {
			SoapySDR_logf(SOAPY_SDR_DEBUG, "NEW BLOCK pkt=%" PRIu64
					" hdr.seqno=%" PRIu64
					" hdr.block_count=%" PRIu64,
					_state.pkt_count,
					hdr->seqno, hdr->block_count);
		}
	}
	else
	{
		/* Check index, total and timestamp match */
		if ((_state.block_index != hdr->block_index) ||
			(_state.block_count != hdr->block_count) ||
			(_state.last_seqno  != hdr->seqno))
		{
			/* Either an out of order, or duplicate block, reset buffer */
			_state.buffer_used = 0;

			/* Drop packet */
			SoapySDR_logf(SOAPY_SDR_WARNING, "Drop packet: mismatch:"
						" pkt=%" PRIu64
						" block_index=%" PRIu64
						" block_count=%" PRIu64
						" last_seqno=%" PRIu64
						" hdr.block_index=%" PRIu64
						" hdr.block_count=%" PRIu64
						" hdr.seqno=%" PRIu64,
						_state.pkt_count,
						_state.block_index,
						_state.block_count,
						_state.last_seqno,
						hdr->block_index,
						hdr->block_count,
						hdr->seqno);
			return -1;
		}
	}

	/* Increment index */
	_state.block_index++;

	return 0;
}

void rx_streamer_ip_gadget::_start(void)
{
	if (!thread.joinable()) {
		// Reset signal
		thread_stop = false;

		// Start thread, passing it current settings
		thread = std::thread(&rx_streamer_ip_gadget::thread_func, this, enabled_channels, buffer_size_samples);

		// Attempt to increase thread priority
		int max_prio = sched_get_priority_max(SCHED_RR);
		if (max_prio >= 0) {
			sched_param sch;
			sch.sched_priority = max_prio;
			if (int rc = pthread_setschedparam(thread.native_handle(), SCHED_RR, &sch)) {
				SoapySDR_logf(SOAPY_SDR_WARNING, "Failed to set RX thread priority (%d)", rc);
			}
		} else {
			SoapySDR_logf(SOAPY_SDR_WARNING, "Failed to query thread schedular priorities");
		}
	}
}

void rx_streamer_ip_gadget::_stop(void)
{
	if (thread.joinable()) {
		// Signal thread to stop
		thread_stop = true;

		// Wait for thread to stop
		thread.join();
	}
}

/*
** Handle network traffic UDP/TCP
*/
int rx_streamer_ip_gadget::udp_recv(data_ip_hdr_t *hdr, uint8_t *payload, size_t payload_size)
{
	_udp.iov[1].iov_base = payload;
	_udp.iov[1].iov_len  = payload_size;

	// Fetch next datagram from socket
	int rc = recvmsg(_data_fd, &_udp.msg, MSG_WAITALL);
	if (-1 == rc) {
		if ((EWOULDBLOCK == errno) || (EAGAIN == errno))
			return 0;
		/* Receive failed and error was not a timeout */
		SoapySDR_logf(SOAPY_SDR_ERROR, "Failed to receive on data socket %s(%d)", strerror(errno), errno);
		return -1;
	}

	/* Receive succeeded, what did we win? Check magic */
	if (((size_t)rc < sizeof(data_ip_hdr_t)) || (SDR_IP_GADGET_MAGIC != hdr->magic)) {
		/* Wrong header size or bad magic, possibly a naughty network application or an honest mistake */
		SoapySDR_logf(SOAPY_SDR_DEBUG, "Dropped wrong header udp datagram "
						" hdr_size=%" PRIu64
						" recv_size=%" PRIu64
						" magic=%" PRIu64
						" recv_magic=%" PRIu64,
						sizeof(data_ip_hdr_t), rc, SDR_IP_GADGET_MAGIC, hdr->magic);
		return -1;
	}

	return rc - sizeof(data_ip_hdr_t);
}

static int tcp_recv_data(int sock, uint8_t *buffer, size_t size)
{
	size_t offset = 0;
	while (offset < size) {
		int rc = recv(sock, &buffer[offset], size - offset, MSG_WAITALL);
		if (rc <= 0) {
			if (rc == 0) {
				SoapySDR_logf(SOAPY_SDR_ERROR, "tcp_recv_data :: Connection is closed by the peer");
				return -1;
			}
			if ((EWOULDBLOCK == errno) || (EAGAIN == errno))
				continue;
			SoapySDR_logf(SOAPY_SDR_ERROR, "tcp_recv_data ::"
				" Failed to receive buffer on tcp data socket %s(%d)", strerror(errno), errno);
			return -1;
		}
		offset += rc;
	}

	return offset;
}

static int tcp_stream_resync(int sock, data_ip_hdr_t *hdr, uint8_t *buffer, size_t buffer_size, size_t search_limit)
{
	size_t processed_bytes = 0;
	int buf_offset = 0;

	while(processed_bytes < search_limit) {
		int recv_bytes = recv(sock, &buffer[buf_offset], buffer_size - buf_offset, 0);
		if (recv_bytes <= 0) {
			if ((EWOULDBLOCK == errno) || (EAGAIN == errno))
				continue;
			if (recv_bytes == 0) {
				SoapySDR_logf(SOAPY_SDR_ERROR, "tcp_stream_resync :: Connection is closed by the peer");
				return -1;
			}
			SoapySDR_logf(SOAPY_SDR_ERROR, "tcp_stream_resync ::"
				" Failed to receive buffer on tcp data socket %s(%d)", strerror(errno), errno);
			return -1;
		}

		uint8_t *p = buffer;
		uint8_t *p_end = p + recv_bytes + buf_offset;
		for(; p < p_end; p++) {
			data_ip_hdr_t *h = (data_ip_hdr_t *)p;
			if (h->magic != SDR_IP_GADGET_MAGIC)
				continue;
			break;
		}
		if (p == p_end) {
			/* SDR_IP_GADGET_MAGIC not found */
			processed_bytes += recv_bytes;
			memcpy(buffer, p_end - sizeof(data_ip_hdr_t), sizeof(data_ip_hdr_t));
			buf_offset = sizeof(data_ip_hdr_t);
			continue;
		}

		processed_bytes += buffer - p;
		size_t left_bytes = p_end - p;

		if (left_bytes < sizeof(data_ip_hdr_t)) {
			if (tcp_recv_data(sock, p_end, sizeof(data_ip_hdr_t) - left_bytes) < 0)
				return -1;
			left_bytes = sizeof(data_ip_hdr_t);
		}

		// copy header from buffer into hdr struct
		memcpy((uint8_t *)hdr, p, sizeof(data_ip_hdr_t));
		left_bytes -= sizeof(data_ip_hdr_t);
		p += sizeof(data_ip_hdr_t);

		// move what is left into the begining of the buffer
		if (left_bytes > 0) {
			memcpy((uint8_t *)buffer, p, left_bytes);
		}
		return left_bytes;
	}

	SoapySDR_logf(SOAPY_SDR_ERROR, "Failed to resynchronize :: search limit reached");
	return -1;
}

int rx_streamer_ip_gadget::tcp_recv(data_ip_hdr_t *hdr, uint8_t *payload, size_t payload_size)
{
	int rc = tcp_recv_data(_data_fd, (uint8_t *)hdr, sizeof(data_ip_hdr_t));
	if (rc < 0) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "Failed to receive header on tcp data socket");
		return -1;
	}

	size_t payload_offset = 0;

	/* Receive succeeded, what did we win? Check magic */
	if ((SDR_IP_GADGET_MAGIC != hdr->magic)) {
		/* Wrong header size or bad magic, possibly a naughty network application or an honest mistake */
		SoapySDR_logf(SOAPY_SDR_WARNING, "Dropped wrong header tcp datagram. Try to resynchronize :: "
						" hdr_size=%" PRIu64
						" recv_size=%" PRIu64
						" magic=%" PRIu64
						" recv_magic=%" PRIu64,
						sizeof(data_ip_hdr_t), rc, SDR_IP_GADGET_MAGIC, hdr->magic);

		/* Try to resynchronize */
		int search_buf_size = payload_size;
		if (search_buf_size > 4096)
			search_buf_size = 4096;

		int rc = tcp_stream_resync(_data_fd, hdr, payload, search_buf_size, _state.buffer_size * 2);
		if (rc < 0) {
			SoapySDR_logf(SOAPY_SDR_ERROR, "Failed to resynchronize");
			return -1;
		}
		payload_offset += rc;
		if (payload_offset == payload_size)
			return sizeof(data_ip_hdr_t);
	}

	rc = tcp_recv_data(_data_fd, &payload[payload_offset], (payload_size - payload_offset));
	if (rc < 0) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "Failed to receive buffer on tcp data socket");
		return -1;
	}

	return payload_size;
}
