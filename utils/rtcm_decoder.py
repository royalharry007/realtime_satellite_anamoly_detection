"""
RTCM MSM7 decoder tailored for AUSCORS + pyrtcm output where all fields are strings.
Decodes:
- satellite_id (PRN)
- SNR (DF404_xx)
"""

from pyrtcm import RTCMReader
from datetime import datetime
import io


class SocketStream(io.RawIOBase):
    def __init__(self, sock):
        self.sock = sock

    def read(self, n=1):
        try:
            return self.sock.recv(n)
        except Exception:
            return b""


class RTCMDecoder:
    def __init__(self, sock):
        self.stream = SocketStream(sock)
        self.reader = RTCMReader(self.stream)

    def _safe_int(self, value):
        """Convert to int safely."""
        try:
            return int(value)
        except:
            return None

    def _safe_float(self, value):
        """Convert to float safely."""
        try:
            return float(value)
        except:
            return None

    def _msgtype(self, parsed):
        if hasattr(parsed, "msgtype"):
            return parsed.msgtype
        if hasattr(parsed, "identity"):
            try:
                return int(parsed.identity)
            except:
                return None
        return None

    def read_next(self):
        messages = []

        try:
            raw, parsed = self.reader.read()
            if parsed is None:
                return messages

            msgtype = self._msgtype(parsed)
            if msgtype is None:
                return messages

            print("RTCM message received:", msgtype)

            # MSM messages
            if msgtype in (
                1071, 1072, 1073, 1074, 1075, 1076, 1077,
                1081, 1082, 1083, 1084, 1085, 1086, 1087,
                1091, 1092, 1093, 1094, 1095, 1096, 1097,
                1111, 1112, 1113, 1114, 1115, 1116, 1117,
                1121, 1122, 1123, 1124, 1125, 1126, 1127
            ):

                nsat = self._safe_int(getattr(parsed, "NSat", 0))
                ncell = self._safe_int(getattr(parsed, "NCell", 0))
                constellation = getattr(parsed, "identity", "MSM")

                if nsat is None or ncell is None:
                    return messages

                # Iterate over every cell
                for cell_idx in range(1, ncell + 1):

                    # Which satellite index does this cell belong to?
                    cellprn_attr = f"CELLPRN_{cell_idx:02d}"
                    sat_index = self._safe_int(getattr(parsed, cellprn_attr, None))
                    if sat_index is None:
                        continue

                    if sat_index < 1 or sat_index > nsat:
                        continue

                    # Get actual PRN
                    prn_attr = f"PRN_{sat_index:02d}"
                    prn = self._safe_int(getattr(parsed, prn_attr, None))
                    if prn is None:
                        continue

                    # SNR from DF404_cellIdx
                    snr_attr = f"DF404_{cell_idx:02d}"
                    snr = self._safe_float(getattr(parsed, snr_attr, None))
                    if snr is None:
                        continue

                    messages.append({
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                        "constellation": constellation,
                        "satellite_id": prn,
                        "snr": snr,
                        "pseudorange": None,
                        "carrier_phase": None,
                        "doppler": None,
                        "lock_time": None,
                    })

                return messages

        except KeyboardInterrupt:
            raise
        except Exception as e:
            print("RTCM decode error:", e)

        return messages