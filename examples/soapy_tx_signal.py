# Copyright (C) 2024 Analog Devices, Inc.
#
# SPDX short identifier: ADIBSD

import numpy as np
import sys
import time
from datetime import datetime

import SoapySDR
from SoapySDR import *  # SOAPY_SDR_ constants

# ------------------------
def now():
    return datetime.now().strftime("%H:%M:%S+%f")

dt_start = time.perf_counter()
def dt():
    global dt_start
    return time.perf_counter() - dt_start
# ------------------------

SoapySDR.setLogLevel(SoapySDR.SOAPY_SDR_DEBUG)

print("API Version:", SoapySDR.getAPIVersion())
print("Library Version:", SoapySDR.getLibVersion())


# %% Configuration Variables
#sdr_conf = 'driver=plutosdr,uri=ip:192.168.1.10,direct=1,timestamp_every=250000'
sdr_conf = 'driver=plutosdr,uri=ip:192.168.1.10,direct=1'

sample_rate = 15_000_000
center_freq = 5_500_000_000 + 500_000
tx_bandwidth = 0
tx_gain = 0

# %% Setup SDR
sdr = SoapySDR.Device(sdr_conf)
if sdr.hasHardwareTime():
    print("SDR has HardwareTime")

sdr.setFrequency(SOAPY_SDR_TX,  0, int(center_freq))
sdr.setSampleRate(SOAPY_SDR_TX, 0, int(sample_rate))
sdr.setBandwidth(SOAPY_SDR_TX,  0, tx_bandwidth)
sdr.setGain(SOAPY_SDR_TX,       0, tx_gain)

sample_rate = sdr.getSampleRate(SOAPY_SDR_TX, 0)
print(f"Actual Tx Rate {sample_rate=}")

print("Create Tx stream")
#tx_stream = sdr.setupStream(SOAPY_SDR_TX, SOAPY_SDR_CF32, [ 0 ])
#tx_stream = sdr.setupStream(SOAPY_SDR_TX, SOAPY_SDR_CF32, [ 0 ], {"transport":"tcp"})
tx_stream = sdr.setupStream(SOAPY_SDR_TX, SOAPY_SDR_CF32, [ 0 ], {"transport":"tcp", "bufflen":"250000"})


tx_mtu = sdr.getStreamMTU(tx_stream)
print(f"TX stream: {tx_mtu=}")

print("Activate Tx Stream")
sdr.activateStream(tx_stream)

time.sleep(1)

# % Create CW signal
num_samples = tx_mtu
const_value = 1 + 1j
cw_signal = np.full(num_samples, const_value, dtype=np.complex64)
print(f"TX buffer length: {len(cw_signal)}")


# % send data
t_report = t_last = time.perf_counter()
tx_samples = 0


try:
    while True:
        status = sdr.writeStream(tx_stream, [ cw_signal ], cw_signal.size, timeoutUs=2_000_000)
        if status.ret < 0:
            if status.ret == -1:
                print(f"[{dt():.4f}] writeStream() timeout")
                continue;
            print(f"[{dt():.4f}] writeStream() error: {status.ret=}")
            break
        t_now = time.perf_counter()

        if (t_now - t_last) > 1.5:
            print(f"[{dt():.4f}] !!! write delay={t_now - t_last}")
        t_last = t_now

        tx_samples += cw_signal.size
        if (t_now - t_report) > 1:
            sps = tx_samples / (t_now - t_report) / 1e6
            print(f"[{dt():.4f}] TX RATE: {sps:.4f} Msps  {tx_samples=}")
            tx_samples = 0
            t_report = t_now

except Exception as e:
    print("Error: ", e)

finally: 
    print("\nStopping")
    print("Closing TX Stream")
    sdr.deactivateStream(tx_stream)
    sdr.closeStream(tx_stream)

