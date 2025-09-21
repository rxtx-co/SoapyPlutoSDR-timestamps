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
sdr_conf = 'driver=plutosdr,uri=ip:192.168.1.10,direct=1,timestamp_every=250000'
#sdr_conf = 'driver=plutosdr,uri=ip:192.168.1.10,direct=1'

sample_rate = 20_000_000
center_freq = 5_800_000_000
rx_bandwidth = 0
rx_gain = 0

# %% Setup SDR
sdr = SoapySDR.Device(sdr_conf)
if sdr.hasHardwareTime():
    print("SDR has HardwareTime")

sdr.setFrequency(SOAPY_SDR_RX,  0, int(center_freq))
sdr.setSampleRate(SOAPY_SDR_RX, 0, int(sample_rate))
sdr.setBandwidth(SOAPY_SDR_RX,  0, rx_bandwidth)
sdr.setGain(SOAPY_SDR_RX,       0, rx_gain)

sample_rate = sdr.getSampleRate(SOAPY_SDR_RX, 0)
print(f"Actual Rx Rate {sample_rate=}")

print("Create Rx stream")
#rx_stream = sdr.setupStream(SOAPY_SDR_RX, SOAPY_SDR_CF32, [ 0 ])
rx_stream = sdr.setupStream(SOAPY_SDR_RX, SOAPY_SDR_CF32, [ 0 ], {"transport":"tcp"})
#rx_stream = sdr.setupStream(SOAPY_SDR_RX, SOAPY_SDR_CF32, [ 0 ], {"transport":"tcp", "bufflen":"250000"})

rx_mtu = sdr.getStreamMTU(rx_stream)
print(f"RX stream: {rx_mtu=}")

print("Activate Rx Stream")
sdr.activateStream(rx_stream)

t_report = t_last = time.perf_counter()
timestamp_last = 0
rx_samples = 0
rx_count = 0

try:
    buffer = np.zeros(rx_mtu, dtype=np.complex64)
    while True:
        status = sdr.readStream(rx_stream, [ buffer ], rx_mtu, timeoutUs=1_000_000)
        if status.ret < 0:
            print(f"[{dt():.4f}] readStream() error: {status.ret=}")
            if status.ret != -1:
                break
            continue
        t_now = time.perf_counter()

        num_samples = status.ret
        timestamp_new = status.timeNs
        rx_count += num_samples

#        print(f"[{dt():.4f}] SAMPLES: {num_samples=} {timestamp_new=}")

        if (t_now - t_last) > 0.5:
            print(f"[{dt():.4f}] !!! read delay={t_now - t_last}: {timestamp_new=}")
        t_last = t_now

        if (timestamp_last > timestamp_new):
            print(f"[{dt():.4f}] !!! timestamp rewind => new {timestamp_new=} {timestamp_last=} {rx_count=}")
        timestamp_last = timestamp_new

        rx_samples += num_samples
        if (t_now - t_report) > 1:
            sps = rx_samples / (t_now - t_report) / 1e6
            print(f"[{dt():.4f}] RX RATE: {sps:.4f} Msps  {rx_samples=}")
            rx_samples = 0
            t_report = t_now


except Exception as e:
    print("Error: ", e)

finally:
    print("\nStopping")
    print("Closing RX Stream")
    sdr.deactivateStream(rx_stream)
    sdr.closeStream(rx_stream)

