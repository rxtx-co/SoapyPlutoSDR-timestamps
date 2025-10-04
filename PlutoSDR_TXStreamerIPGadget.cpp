#include <cstring>
#include <cinttypes>

#include <sched.h>
#include <pthread.h>

#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "PlutoSDR_TXStreamerIPGadget.hpp"

#include <SoapySDR/Device.hpp>
#include <SoapySDR/Logger.hpp>
#include <SoapySDR/Types.hpp>
#include <SoapySDR/Formats.hpp>
#include <SoapySDR/Time.hpp>

#include "PlutoSDR_TimestampEvery.hpp"
#include "PlutoSDR_Sockets.hpp"

tx_streamer_ip_gadget::tx_streamer_ip_gadget(const iio_context *_iio_ctx, const iio_device *_dev, 
	int _sock_control, size_t _udp_packet_size,
	const plutosdrStreamFormat _format, const std::vector<size_t> &channels,
	const SoapySDR::Kwargs &args, uint32_t _timestamp_every):
	iio_ctx(_iio_ctx), dev(_dev),
	sock_control(_sock_control), udp_packet_size(_udp_packet_size),
	format(_format), timestamp_every(_timestamp_every),
	thread_stop(false), queue(16, false), curr_buffer_timestamp(0)

{
	_failed = false;
	sock_data = _data_fd = -1;

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
	iio_channel_attr_read_longlong(iio_device_find_channel(dev, "voltage0", true),"sampling_frequency", &sample_rate);

	// Calculate expected sample size
	sample_size_bytes = channel_list.size() * sizeof(uint16_t);

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
		// Buffer length not provided and timestamping disabled, assume same default as iio tx streamer
		set_buffer_size(4096);
	}

	// Setup timestamping
	SoapyPlutoSDR_TimestampEvery::update_device_timestamp_every(dev, timestamp_every, channel_list.size());

	// Assume direct copying is supported
	direct_copy = true;

	// Transport TCP/UDP
	_transport_tcp = false;
	if (args.count("transport") && args.at("transport") == "tcp")
		_transport_tcp = true;
	SoapySDR_logf(SOAPY_SDR_INFO, "Transport: %s", _transport_tcp ? "tcp" : "udp");

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

	// Direct copy only supported for a single channel (of I + Q samples)
	if (channel_list.size() != 2) direct_copy = false;

	// Check endianess
	int16_t test_dst, test_src = 0x1234;
	iio_channel_convert(channel_list[0], &test_dst, (const void *)&test_src);
	if (test_src != test_dst) direct_copy = false;

	// Report status
	SoapySDR_logf(SOAPY_SDR_INFO, "Has direct TX copy: %d", (int)direct_copy);

	// Create data socket
	struct sockaddr_in peer_addr;
	socklen_t peer_addr_len = sizeof(peer_addr);
	if (getpeername(sock_control, (struct sockaddr *)&peer_addr, &peer_addr_len)) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "failed to get control socket peer address");
		throw std::runtime_error("failed to get control socket peer address");
	}

	sock_data = create_data_socket(peer_addr, _transport_tcp);

}

tx_streamer_ip_gadget::~tx_streamer_ip_gadget()
{
	if (thread.joinable()) {
		_stop();
	}
}

int tx_streamer_ip_gadget::send(const void * const *buffs,
								 const size_t numElems,
								 int &flags,
								 const long long timeNs,
								 const long timeoutUs)
{
	if (_failed)
		return SOAPY_SDR_STREAM_ERROR;

	// Convert timestamp, in case we need to use it
	uint64_t temp_timestamp = SoapySDR::timeNsToTicks(timeNs, sample_rate);

	// Check if timestamping enabled
	if (	curr_buffer
		 && (timestamp_every > 0)
		 && (flags & SOAPY_SDR_HAS_TIME)
	   ) {
		// Buffer available, timestamping enabled and time provided
		// Calculate timestamp difference and sample count difference
		uint64_t timestamp_diff = (temp_timestamp - curr_buffer_timestamp);
		size_t curr_buffer_free_samples = buffer_size_samples - curr_buffer_samples_stored;

		// Calculate how many samples to add to buffer
		// No need to reset samples as buffer is zero initialized when created
		size_t samples_to_fill = std::min(timestamp_diff, curr_buffer_free_samples);

		// Increment sample count
		curr_buffer_samples_stored += samples_to_fill;

		// Increment timestamp ticks
		curr_buffer_timestamp += samples_to_fill;

		// Flush buffer if full
		if (curr_buffer_samples_stored == buffer_size_samples) {
			// Flush and return error code
			int rc = flush(timeoutUs);
			if (0 != rc)
				return rc;
		}
	}

	if (	(timestamp_every > 0)
		 && (flags & SOAPY_SDR_HAS_TIME)
	   ) {
			// Timestamping enabled and timestamp provided, capture it
			if (curr_buffer_timestamp == 0) {
				// ...
			} else
			if ((temp_timestamp + 1) < curr_buffer_timestamp) {
				// Warn timestamp has jumped backwards
				SoapySDR_logf(SOAPY_SDR_WARNING, "Timestamp backwards step: last=%" PRIu64 " new=%" PRIu64 " delta=%" PRIu64,
						curr_buffer_timestamp,
						temp_timestamp,
						(curr_buffer_timestamp - temp_timestamp));
			} else
			if (abs((int64_t)curr_buffer_timestamp - (int64_t)temp_timestamp) > 1) {
				SoapySDR_logf(SOAPY_SDR_WARNING, "Timestamp gap step: last=%" PRIu64 " new=%" PRIu64 " delta=%" PRIu64,
						curr_buffer_timestamp,
						temp_timestamp,
						(curr_buffer_timestamp - temp_timestamp));
			}

			// Update current timestamp
			curr_buffer_timestamp = temp_timestamp;
	}

	if (!curr_buffer) {
		// Allocate new buffer
		curr_buffer = std::make_shared<seq_payload_t>();
		curr_buffer->seqno = curr_buffer_timestamp;
		curr_buffer->payload.resize(buffer_size_samples * sample_size_bytes);

		// Reset samples stored
		curr_buffer_samples_stored = 0;
	}

	// Work out how many items to copy
	size_t items = std::min(buffer_size_samples - curr_buffer_samples_stored, numElems);

	int16_t src = 0;
	int16_t const *src_ptr = &src;

	if (direct_copy && format == PLUTO_SDR_CS16) {
		// optimize for single TX, 2 channel (I/Q), same endianess direct copy
		int16_t *dst_ptr = (int16_t *)curr_buffer->payload.data() + curr_buffer_samples_stored * 2;
		memcpy(dst_ptr, buffs[0], 2 * sizeof(int16_t) * items);

	} else if (direct_copy && format == PLUTO_SDR_CS12) {
		int16_t *dst_ptr = (int16_t *)curr_buffer->payload.data() + curr_buffer_samples_stored * 2;
		uint8_t const *samples_cs12 = (uint8_t *)buffs[0];

		for (size_t index = 0; index < items; ++index) {
			// consume 24 bit (iiqIQQ)
			uint16_t src0 = uint16_t(*(samples_cs12++));
			uint16_t src1 = uint16_t(*(samples_cs12++));
			uint16_t src2 = uint16_t(*(samples_cs12++));
			// produce 2x 16 bit, note the output is MSB aligned, scale=32768
			// note: byte0 = i[11:4]; byte1 = {q[7:4], i[15:12]}; byte2 = q[15:8];
			*dst_ptr = int16_t((src1 << 12) | (src0 << 4));
			dst_ptr++;
			*dst_ptr = int16_t((src2 << 8) | (src1 & 0xf0));
			dst_ptr++;
		}

	} else if (direct_copy && format == PLUTO_SDR_CS8) {
		int16_t *dst_ptr = (int16_t *)curr_buffer->payload.data() + curr_buffer_samples_stored * 2;
		int8_t const *samples_cs8 = (int8_t *)buffs[0];

		for (size_t index = 0; index < items * 2; ++index) {
			// consume (2x) 8bit (IQ)
			// produce (2x) 16 bit, note the output is MSB aligned, scale=32768
			*dst_ptr = int16_t(*samples_cs8) << 8;
			samples_cs8++;
			dst_ptr++;
		}

	} else if (format == PLUTO_SDR_CS12) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "CS12 not available with this endianess or channel layout");
		throw std::runtime_error("CS12 not available with this endianess or channel layout");

	} else {
		for (unsigned int k = 0; k < channel_list.size(); k++) {
			iio_channel *chn = channel_list[k];
			unsigned int index = k / 2;

			uint8_t *dst_ptr = curr_buffer->payload.data() + (curr_buffer_samples_stored * sample_size_bytes) + (sizeof(uint16_t) * k);

			// note that TX expects samples MSB aligned, unlike RX which is LSB aligned
			// Given two channels, caller provides a single buffer for I/Q sample pairs.
			// Given four channels, caller provides two buffers. Channels 0,1 become I/Q pairs of buffer 0, Channels 2,3 become I/Q pairs of buffer 1.
			if (format == PLUTO_SDR_CS16) {

				int16_t *samples_cs16 = (int16_t *)buffs[index];

				for (size_t j = 0; j < items; ++j) {
					src = samples_cs16[j * 2 + (k % 2)];
					iio_channel_convert_inverse(chn, dst_ptr, src_ptr);
					dst_ptr += sample_size_bytes;
				}
			}
			else if (format == PLUTO_SDR_CF32) {

				float *samples_cf32 = (float *)buffs[index];

				for (size_t j = 0; j < items; ++j) {
					src = (int16_t)(samples_cf32[j * 2 + (k % 2)] * 32767.999f); // 32767.999f (0x46ffffff) will ensure better distribution
					iio_channel_convert_inverse(chn, dst_ptr, src_ptr);
					dst_ptr += sample_size_bytes;
				}
			}
			else if (format == PLUTO_SDR_CS8) {

				int8_t *samples_cs8 = (int8_t *)buffs[index];

				for (size_t j = 0; j < items; ++j) {
					src = (int16_t)(samples_cs8[j * 2 + (k % 2)] << 8);
					iio_channel_convert_inverse(chn, dst_ptr, src_ptr);
					dst_ptr += sample_size_bytes;
				}
			}
		}
	}

	// Increment items in buffer
	curr_buffer_samples_stored += items;

	// Increment timestamp ticks
	curr_buffer_timestamp += items;

	if (curr_buffer_samples_stored == buffer_size_samples || (flags & SOAPY_SDR_END_BURST && numElems == items)) {
		// Buffer is full, or caller has indicated this is the end of the burst, send buffer, returning any errors
		int rc = flush(timeoutUs);
		if (0 != rc) return rc;
	}

	// Return number of samples copied
	return items;
}

int tx_streamer_ip_gadget::flush(const long timeoutUs)
{
	int result = 0;

	long waited_time = 0;
	while (!thread_stop.load()) {
		if (queue.push(curr_buffer, (timeoutUs > 0), 1000))
			break;
		waited_time += 1000;
		if (waited_time >= timeoutUs) {
			// Failed to push entry within timeout
			result = SOAPY_SDR_TIMEOUT;
			break;
		}
	}

	// Reset buffer
	curr_buffer.reset();

	if (_failed)
		return SOAPY_SDR_STREAM_ERROR;
	return result;
}

int tx_streamer_ip_gadget::start(const int flags,
								  const long long timeNs,
								  const size_t numElems)
{
	// Issue start command
	_start();

	return 0;
}

int tx_streamer_ip_gadget::stop(const int flags,
								 const long long timeNs)
{
	// Issue stop command
	_stop();

	return 0;
}

size_t tx_streamer_ip_gadget::get_mtu_size()
{
	// Return size of buffer data area
	return buffer_size_samples;
}

void tx_streamer_ip_gadget::set_samplerate(const size_t _samplerate)
{
	// Store updated sample rate
	sample_rate = _samplerate;
}

void tx_streamer_ip_gadget::set_buffer_size(const size_t _buffer_size)
{
	if (buffer_size_samples != _buffer_size) {
		// Is thread currently running?
		bool was_running = thread.joinable();

		// Buffer size changing
		if (was_running) {
			// Stop stream
			_stop();
		}

		// Save new buffer size
		buffer_size_samples = _buffer_size;

		if (was_running) {
			// Start stream
			_start();
		}
	}
}

void tx_streamer_ip_gadget::thread_func(uint32_t curr_enabled_channels, uint32_t curr_buffer_size_samples)
{
	// Re-calculate how many packets are required to transfer a buffer
	// Calculate how much data can fit in each udp packet once header has been accounted for
	_payload_size = buffer_size_samples * sample_size_bytes;

	// Declare buffer
	std::shared_ptr<seq_payload_t> buffer;

	if (_transport_tcp) {
		tcp_prepare();
		_data_fd = -1;	// wait for incoming connection XXX: ... shit
	} else {
		udp_prepare();
		_data_fd = sock_data;
	}


	/* Update timestamp increment */
	uint32_t timestamp_increment;
	if (timestamp_every > 0) {
		timestamp_increment = timestamp_clock_rate > 0
				? (__int128_t(timestamp_every) * timestamp_clock_rate) / sample_rate
				: timestamp_every;
	} else {
		timestamp_increment = curr_buffer_size_samples;
	}
	sdr_set_timestamp_increment(timestamp_increment);

	/*
	**  Start stream
	*/
	// Retrieve data socket port
	struct sockaddr_in addr;
	socklen_t addr_len = sizeof(addr);
	if (getsockname(sock_data, (struct sockaddr*)&addr, &addr_len) == -1) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "Error getting socket name");
		return;
	}

	cmd_ip_t cmd;
	cmd.hdr.magic = SDR_IP_GADGET_MAGIC;
	cmd.hdr.cmd = SDR_IP_GADGET_COMMAND_START_TX;
	cmd.start_tx.data_port = ntohs(addr.sin_port);
	cmd.start_tx.enabled_channels = curr_enabled_channels;
	cmd.start_tx.timestamping_enabled = (timestamp_every > 0);
	cmd.start_tx.timestamp_increment = timestamp_increment;
	cmd.start_tx.transport_tcp = _transport_tcp;
	cmd.start_tx.buffer_size_samples = curr_buffer_size_samples;

	int rc = sendto(sock_control, &cmd, sizeof(cmd.start_tx), 0, NULL, 0);
	if (rc < 0) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "Failed to send start TX stream cmd (%d)", rc);
		_failed = true;
		return;
	}
	// ------------------------------------------

	/* Main TX loop */
	// Keep running until told to stop
	while (!thread_stop.load()) {
		if (_data_fd < 0) {
			if ((_data_fd = tcp_wait_for_connection(sock_data)) < 0) {
				_failed = true;
				break;
			}
			continue;
		}

		// Read buffer from queue with 1s timeout
		if (!queue.pop(buffer, true, 1000000))
			continue;

		uint64_t seqno = timestamp_clock_rate > 0
			? (__int128(buffer->seqno) * timestamp_clock_rate) / sample_rate
			: buffer->seqno;

		uint8_t *payload = buffer->payload.data();

		SoapySDR_logf(SOAPY_SDR_DEBUG, "TX buffer: seqno=%" PRIu64, seqno);

		int rc;
		if (_transport_tcp) {
			rc = tcp_send(seqno, payload);
		} else {
			rc = udp_send(seqno, payload);
		}
		if (rc < 0)
			break;
	}

	close(sock_data);
	if (_data_fd != sock_data)
		close(_data_fd);
	sock_data = _data_fd = -1;

	// Free buffers
	if (!_transport_tcp) {
		delete[] _udp.arr_mmsg_hdrs;
		delete[] _udp.arr_iovs;
		delete[] _udp.arr_pkt_hdrs;
	}

	// Stop stream
	cmd.hdr.magic = SDR_IP_GADGET_MAGIC;
	cmd.hdr.cmd = SDR_IP_GADGET_COMMAND_STOP_TX;
	rc = sendto(sock_control, &cmd, sizeof(cmd.stop), 0, NULL, 0);
	if (rc < 0) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "Failed to send stop TX stream cmd (%d)", rc);
		return;
	}
}

void tx_streamer_ip_gadget::_start(void)
{
	if (!thread.joinable()) {
		// Reset signal
		thread_stop = false;

		// Start thread, passing it current settings
		thread = std::thread(&tx_streamer_ip_gadget::thread_func, this, enabled_channels, buffer_size_samples);

		// Attempt to increase thread priority
		int max_prio = sched_get_priority_max(SCHED_RR);
		if (max_prio >= 0) {
			sched_param sch;
			sch.sched_priority = max_prio;
			if (int rc = pthread_setschedparam(thread.native_handle(), SCHED_RR, &sch)) {
				SoapySDR_logf(SOAPY_SDR_WARNING, "Failed to set TX thread priority (%d)", rc);
			}
		} else {
			SoapySDR_logf(SOAPY_SDR_WARNING, "Failed to query thread schedular priorities");
		}
	}
}

void tx_streamer_ip_gadget::_stop(void)
{
	if (thread.joinable()) {
		// Signal thread to stop
		thread_stop = true;

		// Wait for thread to stop
		thread.join();
	}
}

/*
** Update timestamp increment into SDR
*/
void tx_streamer_ip_gadget::sdr_set_timestamp_increment(uint32_t timestamp_increment)
{
	iio_device *iio_dev_timestamp_ctrl = iio_context_find_device(iio_ctx, "axi-timestamp-ctrl");
	if (iio_dev_timestamp_ctrl == nullptr) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "IIO: cannot find axi-timestamp-ctrl device");
		throw std::runtime_error("IIO: cannot find axi-timestamp-ctrl device");
	}

	iio_channel *iio_chan_dac = iio_device_find_channel(iio_dev_timestamp_ctrl, "dac", false);
	if (iio_chan_dac == nullptr) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "IIO:axi-timestamp-ctrl: cannot find channel \"dac\"");
		throw std::runtime_error("IIO:axi-timestamp-ctrl: cannot find channel \"dac\"");
	}

	long long v = timestamp_increment;
	int ret = iio_channel_attr_write_longlong(iio_chan_dac, "dac_timestamp_increment", v);
	if (ret != 0) {
		SoapySDR_logf(SOAPY_SDR_ERROR, "IIO: failed to set dac_timestamp_increment=%u", timestamp_increment);
		throw std::runtime_error("IIO: failed to set dac_timestamp_increment");
	}
}

/*
** Handle socket operations UDP/TCP
*/
int tx_streamer_ip_gadget::udp_prepare(void)
{
	_udp.packet_payload_size = udp_packet_size - sizeof(data_ip_hdr_t);
	_udp.packets_per_buffer  = (_payload_size + (_udp.packet_payload_size - 1U)) / _udp.packet_payload_size;

	// Declare scatter gather array
	_udp.arr_mmsg_hdrs = new struct mmsghdr[_udp.packets_per_buffer];
	_udp.arr_iovs = new struct iovec[2 * _udp.packets_per_buffer];
	_udp.arr_pkt_hdrs = new data_ip_hdr_t[_udp.packets_per_buffer];

	// Pre-populate fixed fields
	for (size_t i = 0; i < _udp.packets_per_buffer; i++)
	{
		// Reset objects
		std::memset(&_udp.arr_mmsg_hdrs[i], 0x00, sizeof(_udp.arr_mmsg_hdrs[0]));
		std::memset(&_udp.arr_iovs[2 * i],  0x00, sizeof(_udp.arr_iovs[0]) * 2);
		std::memset(&_udp.arr_pkt_hdrs[i],  0x00, sizeof(_udp.arr_pkt_hdrs[0]));

		// Each message makes use of two IOVs (one for the header and one for the data)
		_udp.arr_mmsg_hdrs[i].msg_hdr.msg_iov = &_udp.arr_iovs[2 * i];
		_udp.arr_mmsg_hdrs[i].msg_hdr.msg_iovlen = 2;

		// First IOV of each pair points at packet header, next will point at payload and be updated just before tranmission
		_udp.arr_iovs[(2 * i) + 0].iov_base = &_udp.arr_pkt_hdrs[i];
		_udp.arr_iovs[(2 * i) + 0].iov_len = sizeof(data_ip_hdr_t);
		_udp.arr_iovs[(2 * i) + 1].iov_base = NULL;

		/* Set data pointer for packet */
		if (i < (_udp.packets_per_buffer - 1)) {
			/* Not the last packet, therefore must be full */
			_udp.arr_iovs[(2 * i) + 1].iov_len = _udp.packet_payload_size;
		} else {
			/* Last packet, work out how many bytes of the payload it will contain */
			_udp.arr_iovs[(2 * i) + 1].iov_len = (_payload_size % _udp.packet_payload_size);
		}

		/* Prepare packet headers, just need to fill in the sequence number at transmission time */
		_udp.arr_pkt_hdrs[i].magic = SDR_IP_GADGET_MAGIC;
		_udp.arr_pkt_hdrs[i].block_index = (uint16_t)i;
		_udp.arr_pkt_hdrs[i].block_count = (uint16_t)_udp.packets_per_buffer;
	}

	return 0;
}

int tx_streamer_ip_gadget::tcp_prepare()
{
	/* prepare packet header */
	_tcp.pkt_hdr.magic = SDR_IP_GADGET_MAGIC;
	_tcp.pkt_hdr.block_index = 0;
	_tcp.pkt_hdr.block_count = 1;

	return 0;
}

int tx_streamer_ip_gadget::udp_send(uint64_t seqno, uint8_t *payload)
{
	// Retrieved valid data block, update scatter/gather to send header and data
	for (size_t i = 0; i < _udp.packets_per_buffer; i++)
	{
		/* Set sequence number for packet */
		_udp.arr_pkt_hdrs[i].seqno = seqno;

		_udp.arr_iovs[(2 * i) + 1].iov_base = payload;

		payload += _udp.packet_payload_size;
	}

	/* Send all datagrams with single system call :-) */
	int rc = 0;
	while (!thread_stop.load()) {
		rc = sendmmsg(_data_fd, _udp.arr_mmsg_hdrs, _udp.packets_per_buffer, 0);
		if ((EWOULDBLOCK == errno) || (EAGAIN == errno))
			continue;
		break;
	}

	if ((int)_udp.packets_per_buffer != rc)
	{
		if (rc < 0) {
			SoapySDR_logf(SOAPY_SDR_ERROR, "Failed to send on data socket: %s(%d)", strerror(errno), errno);
			_failed = true;
			return -1;
		}
		SoapySDR_logf(SOAPY_SDR_DEBUG, "Send overflow : sent packets = %d of %d",
				rc, _udp.packets_per_buffer);
	}

	return 0;
}

int tx_streamer_ip_gadget::tcp_send_data(uint8_t *data, size_t size)
{
	size_t offset = 0;
	while ((offset < size) && !thread_stop.load()) {
		int rc = sendto(_data_fd, (const void *)&data[offset], size - offset, MSG_NOSIGNAL, NULL, 0);
		if (rc < 0) {
			if (EWOULDBLOCK == errno || EAGAIN == errno)
				continue;
			SoapySDR_logf(SOAPY_SDR_ERROR, "TCP: Failed to send on data socket: %s(%d)", strerror(errno), errno);
			_failed = true;
			return -1;
		}
		offset += rc;
	}
	return offset;
}

int tx_streamer_ip_gadget::tcp_send(uint64_t seqno, uint8_t *payload)
{
	_tcp.pkt_hdr.seqno = seqno;

	if (tcp_send_data((uint8_t *)&_tcp.pkt_hdr, sizeof(data_ip_hdr_t)) != sizeof(data_ip_hdr_t))
		return -1;

	if (tcp_send_data(payload, _payload_size) != (int)_payload_size)
		return -1;

	return 0;
}