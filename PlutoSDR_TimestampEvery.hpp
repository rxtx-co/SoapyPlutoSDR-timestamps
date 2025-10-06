#include <cstdint>

#include <SoapySDR/Logger.hpp>
#include <iio.h>

namespace SoapyPlutoSDR_TimestampEvery
{
    inline void update_device_timestamp_every(const iio_context *iio_ctx, bool is_dac, uint32_t value, size_t channel_count)
    {
        // ADC/DAC data bus in the pluto is 64-bit wide. With I/Q channels enabled for one channel
        // each sample is 2 * 16-bit. Therefore 2 samples will be carried in each 64-bit value.
        // Timestamps are applied every x samples on the 64-bit bus. Therefore we divide the
        // requested timestamp every value by 2.
        // If four channels are enabled, each sample is 4 x 16-bit. Therefore one sample will be carried
        // in each 64-bit value. We do not need to divide the requested timestamp value.
        if (channel_count == 2)
        {
            value /= 2;
        }

        const char *device_label = is_dac ? "dac" : "adc";
        const char *dev_attr_label = is_dac ? "dac_timestamp_every" : "adc_timestamp_every";

        iio_device *iio_dev_timestamp_ctrl = iio_context_find_device(iio_ctx, "axi-timestamp-ctrl");
        if (iio_dev_timestamp_ctrl == nullptr) {
            SoapySDR_logf(SOAPY_SDR_ERROR, "IIO: cannot find axi-timestamp-ctrl device");
            throw std::runtime_error("IIO: cannot find axi-timestamp-ctrl device");
        }

        iio_channel *iio_chan_dac = iio_device_find_channel(iio_dev_timestamp_ctrl, device_label, false);
        if (iio_chan_dac == nullptr) {
            SoapySDR_logf(SOAPY_SDR_ERROR, "IIO:axi-timestamp-ctrl: cannot find channel \"%s\"", device_label);
            throw std::runtime_error("IIO:axi-timestamp-ctrl: cannot find [dac|adc] channel");
        }

        long long v = value;
        int ret = iio_channel_attr_write_longlong(iio_chan_dac, dev_attr_label, v);
        if (ret != 0) {
            SoapySDR_logf(SOAPY_SDR_ERROR, "IIO: failed to set %s=%u",  dev_attr_label, value);
            throw std::runtime_error("IIO: failed to set [dac|adc]_timestamp_every");
        }
    }
}
