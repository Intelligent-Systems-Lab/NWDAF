#!/usr/bin/env python3
"""
Fake SMF+UPF Server for NWDAF testing.

Simulates:
- SMF: Receives event subscription requests from NWDAF
- UPF: Sends periodic USER_DATA_USAGE_MEASURES notifications

Traffic Patterns:
- stable:   Steady traffic with small gaussian noise
- periodic: Regular burst cycles on a stable baseline
- random:   Highly variable traffic

Usage:
    uv run fake_smf_upf_server.py [port] --pattern <pattern> [options]

Examples:
    uv run fake_smf_upf_server.py 8081 --pattern stable
    uv run fake_smf_upf_server.py 8081 --pattern periodic --burst-multiplier 10 --burst-period 5
    uv run fake_smf_upf_server.py 8081 --pattern random --random-min 500 --random-max 5000
"""

import argparse
import json
import math
import random
import sys
import threading
import time
import uuid
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler


# ============================================================================
# Constants
# ============================================================================

# Assumed average Ethernet payload size for packet count estimation.
# Real UPF reports actual counts; this is only for fake traffic simulation.
AVG_PACKET_BYTES = 1400

# ============================================================================
# Traffic Generators
# ============================================================================

class TrafficGenerator:
    """Base class for traffic pattern generators."""

    def __init__(self, base_ul: int = 2000, base_dl: int = 2000):
        self.base_ul = base_ul
        self.base_dl = base_dl
        self._tick = 0

    def generate(self) -> tuple[int, int]:
        """Generate (ul_volume, dl_volume) for this tick."""
        self._tick += 1
        return self._generate_impl()

    def _generate_impl(self) -> tuple[int, int]:
        raise NotImplementedError

    @property
    def tick(self) -> int:
        return self._tick

    def describe(self) -> str:
        return "base"


class StableTrafficGenerator(TrafficGenerator):
    """Very steady traffic with small gaussian noise.

    Each tick produces base ± jitter%, where jitter controls the noise level.
    Example: base_ul=100000, jitter_pct=5 → 95000-105000

    Args:
        base_ul:     UL baseline volume (bytes)
        base_dl:     DL baseline volume (bytes)
        jitter_pct:  Max percentage deviation (default: 5 → ±5%)
    """

    def __init__(self, base_ul: int = 2000, base_dl: int = 2000,
                 jitter_pct: float = 5.0):
        super().__init__(base_ul, base_dl)
        self.jitter_pct = jitter_pct

    def _generate_impl(self) -> tuple[int, int]:
        # Gaussian noise: 95% of values within ±jitter_pct
        ul_noise = random.gauss(0, self.jitter_pct / 2) / 100
        dl_noise = random.gauss(0, self.jitter_pct / 2) / 100
        ul = max(0, int(self.base_ul * (1 + ul_noise)))
        dl = max(0, int(self.base_dl * (1 + dl_noise)))
        return ul, dl

    def describe(self) -> str:
        return f"stable (base_ul={self.base_ul}, base_dl={self.base_dl}, jitter=±{self.jitter_pct}%)"


class PeriodicTrafficGenerator(TrafficGenerator):
    """Periodic burst patterns on a stable baseline.

    Every burst_period ticks, traffic spikes by burst_multiplier.
    The burst lasts burst_duration ticks, then returns to baseline.
    Baseline has small jitter for realism.

    Args:
        base_ul:           UL baseline volume (bytes)
        base_dl:           DL baseline volume (bytes)
        burst_period:      Ticks between bursts (default: 5)
        burst_multiplier:  Multiplier during burst (default: 10)
        burst_duration:    How many ticks the burst lasts (default: 1)
        baseline_jitter:   Baseline noise ±% (default: 3)
    """

    def __init__(self, base_ul: int = 2000, base_dl: int = 2000,
                 burst_period: int = 5, burst_multiplier: float = 10.0,
                 burst_duration: int = 1, baseline_jitter: float = 3.0):
        super().__init__(base_ul, base_dl)
        self.burst_period = burst_period
        self.burst_multiplier = burst_multiplier
        self.burst_duration = burst_duration
        self.baseline_jitter = baseline_jitter

    def _generate_impl(self) -> tuple[int, int]:
        # Check if we're in a burst window
        cycle_pos = self.tick % self.burst_period
        in_burst = cycle_pos > 0 and cycle_pos <= self.burst_duration

        if in_burst:
            multiplier = self.burst_multiplier
        else:
            multiplier = 1.0

        # Small baseline jitter
        ul_noise = random.gauss(0, self.baseline_jitter / 2) / 100
        dl_noise = random.gauss(0, self.baseline_jitter / 2) / 100

        ul = max(0, int(self.base_ul * multiplier * (1 + ul_noise)))
        dl = max(0, int(self.base_dl * multiplier * (1 + dl_noise)))
        return ul, dl

    def describe(self) -> str:
        return (f"periodic (base_ul={self.base_ul}, base_dl={self.base_dl}, "
                f"burst every {self.burst_period} ticks ×{self.burst_multiplier}, "
                f"duration={self.burst_duration}, jitter=±{self.baseline_jitter}%)")


class RandomTrafficGenerator(TrafficGenerator):
    """Highly variable random traffic.

    Each tick produces a uniformly distributed volume in [min, max].
    Optional smoothing factor blends with previous value for less chaotic output.

    Args:
        random_min_ul:  Min UL volume (bytes)
        random_max_ul:  Max UL volume (bytes)
        random_min_dl:  Min DL volume (bytes)
        random_max_dl:  Max DL volume (bytes)
        smoothing:      Blend factor with previous: 0=fully random, 0.9=very smooth (default: 0)
    """

    def __init__(self, random_min_ul: int = 500, random_max_ul: int = 5000,
                 random_min_dl: int = 500, random_max_dl: int = 5000,
                 smoothing: float = 0.0):
        super().__init__(0, 0)
        self.random_min_ul = random_min_ul
        self.random_max_ul = random_max_ul
        self.random_min_dl = random_min_dl
        self.random_max_dl = random_max_dl
        self.smoothing = max(0.0, min(smoothing, 0.99))
        self._prev_ul = (random_min_ul + random_max_ul) // 2
        self._prev_dl = (random_min_dl + random_max_dl) // 2

    def _generate_impl(self) -> tuple[int, int]:
        raw_ul = random.randint(self.random_min_ul, self.random_max_ul)
        raw_dl = random.randint(self.random_min_dl, self.random_max_dl)

        if self.smoothing > 0:
            ul = int(self.smoothing * self._prev_ul + (1 - self.smoothing) * raw_ul)
            dl = int(self.smoothing * self._prev_dl + (1 - self.smoothing) * raw_dl)
        else:
            ul, dl = raw_ul, raw_dl

        self._prev_ul = ul
        self._prev_dl = dl
        return ul, dl

    def describe(self) -> str:
        return (f"random (UL=[{self.random_min_ul}, {self.random_max_ul}], "
                f"DL=[{self.random_min_dl}, {self.random_max_dl}], "
                f"smoothing={self.smoothing})")


# ============================================================================
# HTTP Handler
# ============================================================================

class FakeSmfUpfHandler(BaseHTTPRequestHandler):
    """Handler for SMF subscription requests."""
    
    # Class-level storage for subscriptions
    subscriptions: dict = {}
    upf_notify_threads: dict = {}
    traffic_generator: TrafficGenerator = None  # Set from main()
    
    def do_POST(self):
        """Handle SMF subscription creation."""
        if self.path == "/nsmf-event-exposure/v1/subscriptions":
            self._handle_create_subscription()
        else:
            self._send_error(404, "Not Found")
    
    def do_DELETE(self):
        """Handle SMF subscription deletion."""
        if self.path.startswith("/nsmf-event-exposure/v1/subscriptions/"):
            sub_id = self.path.split("/")[-1]
            self._handle_delete_subscription(sub_id)
        else:
            self._send_error(404, "Not Found")
    
    def _handle_create_subscription(self):
        """Process subscription creation request."""
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)
        
        try:
            request = json.loads(body)
        except json.JSONDecodeError:
            self._send_error(400, "Invalid JSON")
            return
        
        # Generate subscription ID
        sub_id = f"fake-smf-sub-{uuid.uuid4().hex[:8]}"
        
        # Extract UPF notification URI and reporting period
        upf_notify_uri = None
        supi = request.get("supi")
        group_id = request.get("groupId")  # External Group ID support
        any_ue_ind = request.get("anyUeInd", False)  # Any UE indication
        rep_period = request.get("repPeriod", 10)  # Default 10s if not specified
        # Per TS 29.508: notifId is used as correlation ID
        notify_correlation_id = request.get("notifId", "")
        
        for event_sub in request.get("eventSubs", []):
            if event_sub.get("event") == "UPF_EVENT":
                upf_notify_uri = event_sub.get("bundledEventNotifyUri")
                break
        
        # Determine subscription target type
        target_type = "unknown"
        target_value = "unknown"
        if supi:
            target_type = "supi"
            target_value = supi
        elif group_id:
            target_type = "groupId"
            target_value = group_id
        elif any_ue_ind:
            target_type = "anyUe"
            target_value = "all"
        
        # Store subscription
        self.subscriptions[sub_id] = {
            "request": request,
            "upf_notify_uri": upf_notify_uri,
            "supi": supi,
            "group_id": group_id,
            "any_ue_ind": any_ue_ind,
            "target_type": target_type,
            "target_value": target_value,
            "rep_period": rep_period,
            "notify_correlation_id": notify_correlation_id,
            "created_at": datetime.now().isoformat(),
        }
        
        self._log(f"📝 Subscription created: {sub_id}")
        self._log(f"   Target Type: {target_type}")
        self._log(f"   Target Value: {target_value}")
        self._log(f"   UPF notify URI: {upf_notify_uri}")
        self._log(f"   Report Period: {rep_period}s")
        self._log(f"   NotifId (correlationId): {notify_correlation_id}")
        self._log(f"   📋 Request JSON:")
        self._log_json(request)

        
        # Start UPF notification thread if URI provided
        if upf_notify_uri:
            self._start_upf_notifier(sub_id, upf_notify_uri, target_type, target_value, rep_period, notify_correlation_id)
        
        # Send 201 Created response
        response = {
            "eventSubs": request.get("eventSubs", []),
            "notifUri": request.get("notifUri"),
            "notifId": request.get("notifId"),
            "subId": sub_id,
        }
        # Include target in response based on type
        if supi:
            response["supi"] = supi
        if group_id:
            response["groupId"] = group_id
        if any_ue_ind:
            response["anyUeInd"] = any_ue_ind
        
        self.send_response(201)
        self.send_header("Content-Type", "application/json")
        self.send_header("Location", f"/nsmf-event-exposure/v1/subscriptions/{sub_id}")
        self.end_headers()
        self.wfile.write(json.dumps(response).encode())
    
    def _handle_delete_subscription(self, sub_id: str):
        """Process subscription deletion request."""
        if sub_id in self.subscriptions:
            # Stop UPF notifier thread
            if sub_id in self.upf_notify_threads:
                self.upf_notify_threads[sub_id]["stop"] = True
                del self.upf_notify_threads[sub_id]
            
            del self.subscriptions[sub_id]
            self._log(f"🗑️ Subscription deleted: {sub_id}")
            
            self.send_response(204)
            self.end_headers()
        else:
            self._send_error(404, "Subscription not found")
    
    def _start_upf_notifier(self, sub_id: str, notify_uri: str, target_type: str, target_value: str, rep_period: int, correlation_id: str):
        """Start background thread to send UPF notifications."""
        thread_data = {"stop": False}
        self.upf_notify_threads[sub_id] = thread_data
        
        thread = threading.Thread(
            target=self._upf_notify_loop,
            args=(sub_id, notify_uri, target_type, target_value, rep_period, correlation_id, thread_data),
            daemon=True
        )
        thread.start()
    
    def _upf_notify_loop(self, sub_id: str, notify_uri: str, target_type: str, target_value: str, rep_period: int, correlation_id: str, control: dict):
        """Send periodic UPF notifications."""
        import urllib.request
        
        interval = rep_period  # Use consumer-specified period
        count = 0
        
        while not control.get("stop", False):
            time.sleep(interval)
            if control.get("stop", False):
                break
            
            count += 1
            # Build notification based on target type
            notification = self._build_upf_notification(correlation_id, target_type, target_value, count, interval)
            
            try:
                data = json.dumps(notification).encode()
                req = urllib.request.Request(
                    notify_uri,
                    data=data,
                    headers={"Content-Type": "application/json"},
                    method="POST"
                )
                with urllib.request.urlopen(req, timeout=5) as resp:
                    num_items = len(notification.get("notificationItems", []))
                    self._log(f"📤 UPF notification #{count} sent (target: {target_type}, items: {num_items}, status: {resp.status})")
                    self._log(f"   📋 Notification JSON:")
                    self._log_json(notification)
            except Exception as e:
                self._log(f"❌ Failed to send UPF notification: {e}")
    
    def _build_upf_notification(self, correlation_id: str, target_type: str, target_value: str, count: int, rep_period: int = 10) -> dict:
        """Build UPF USER_DATA_USAGE_MEASURES notification.

        Per TS 29.564: UPF notifications may NOT include SUPI.
        The correlationId is used by NWDAF to resolve the target.

        For groupId subscriptions, simulate multiple UEs in the group.
        """
        notification_items = []

        # Determine how many UEs to simulate
        if target_type == "groupId":
            # Simulate 3-5 UEs in the group with different IPs
            num_ues = random.randint(3, 5)
            for i in range(num_ues):
                ip_addr = f"10.60.0.{10 + i}"
                item = self._build_notification_item(count, rep_period, ip_address=ip_addr)
                notification_items.append(item)
        elif target_type == "anyUe":
            # Simulate 2-4 random UEs
            num_ues = random.randint(2, 4)
            for i in range(num_ues):
                ip_addr = f"10.60.{random.randint(0, 255)}.{random.randint(1, 254)}"
                item = self._build_notification_item(count, rep_period, ip_address=ip_addr)
                notification_items.append(item)
        else:
            # Single UE (supi-based subscription) - generate IP based on SUPI
            # Use hash of SUPI to generate consistent IP for same UE
            if target_value and target_value.startswith("imsi-"):
                # Extract last 3 digits of IMSI for IP
                try:
                    imsi_suffix = int(target_value[-3:]) % 254 + 1
                except ValueError:
                    imsi_suffix = random.randint(1, 254)
                ip_addr = f"10.60.0.{imsi_suffix}"
            else:
                ip_addr = f"10.60.0.{random.randint(1, 254)}"
            item = self._build_notification_item(count, rep_period, ip_address=ip_addr)
            notification_items.append(item)

        return {
            "correlationId": correlation_id,
            "notificationItems": notification_items,
        }

    def _format_packet_rate(self, pps: float) -> str:
        """Format a packet-per-second value as a TS29571 PacketRate string.

        Pattern: '<number> (pps|kpps|Mpps|Gpps|Tpps)'
        """
        if pps >= 1_000_000_000_000:
            return f"{pps / 1_000_000_000_000:.3f} Tpps"
        elif pps >= 1_000_000_000:
            return f"{pps / 1_000_000_000:.3f} Gpps"
        elif pps >= 1_000_000:
            return f"{pps / 1_000_000:.3f} Mpps"
        elif pps >= 1000:
            return f"{pps / 1000:.3f} kpps"
        else:
            return f"{pps:.3f} pps"

    def _build_notification_item(self, count: int, rep_period: int = 10, ip_address: str = None) -> dict:
        """Build a single notification item with traffic data from the generator.

        Packet counts are estimated from volume using AVG_PACKET_BYTES.
        Packet throughput is computed as packets / rep_period, formatted per TS29571.
        """
        gen = self.__class__.traffic_generator
        ul, dl = gen.generate()

        # Estimate packet counts (integers)
        ul_pkts = max(1, ul // AVG_PACKET_BYTES)
        dl_pkts = max(1, dl // AVG_PACKET_BYTES)
        total_pkts = ul_pkts + dl_pkts

        # Packet throughput = packets per second over the reporting interval
        ul_pps = ul_pkts / rep_period if rep_period > 0 else 0.0
        dl_pps = dl_pkts / rep_period if rep_period > 0 else 0.0

        item = {
            "eventType": "USER_DATA_USAGE_MEASURES",
            "dnn": "internet",
            "timeStamp": datetime.now().astimezone().isoformat(),
            "userDataUsageMeasurements": [
                {
                    "volumeMeasurement": {
                        "totalVolume": ul + dl,
                        "ulVolume": ul,
                        "dlVolume": dl,
                        "totalNbOfPackets": total_pkts,
                        "ulNbOfPackets": ul_pkts,
                        "dlNbOfPackets": dl_pkts,
                    },
                    "throughputMeasurement": {
                        "ulThroughput": f"{ul * 8 // 1000} Kbps",
                        "dlThroughput": f"{dl * 8 // 1000} Kbps",
                        "ulPacketThroughput": self._format_packet_rate(ul_pps),
                        "dlPacketThroughput": self._format_packet_rate(dl_pps),
                    }
                }
            ]
        }
        # Add IP address if provided (per TS 29.564: ueIpv4Addr)
        if ip_address:
            item["ueIpv4Addr"] = ip_address

        return item
    
    def _send_error(self, code: int, message: str):
        """Send error response."""
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        error = {"status": code, "cause": message}
        self.wfile.write(json.dumps(error).encode())
    
    def _log(self, message: str):
        """Print log message with timestamp."""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
    
    def _log_json(self, data: dict):
        """Print formatted JSON data."""
        formatted = json.dumps(data, indent=2, ensure_ascii=False)
        for line in formatted.split('\n'):
            print(f"[{datetime.now().strftime('%H:%M:%S')}]   {line}")
    
    def log_message(self, format, *args):
        """Suppress default HTTP logging."""
        pass


# ============================================================================
# CLI & Main
# ============================================================================

def build_traffic_generator(args) -> TrafficGenerator:
    """Create a traffic generator from CLI arguments."""
    pattern = args.pattern

    if pattern == "stable":
        return StableTrafficGenerator(
            base_ul=args.base_ul,
            base_dl=args.base_dl,
            jitter_pct=args.jitter,
        )
    elif pattern == "periodic":
        return PeriodicTrafficGenerator(
            base_ul=args.base_ul,
            base_dl=args.base_dl,
            burst_period=args.burst_period,
            burst_multiplier=args.burst_multiplier,
            burst_duration=args.burst_duration,
            baseline_jitter=args.jitter,
        )
    elif pattern == "random":
        return RandomTrafficGenerator(
            random_min_ul=args.random_min,
            random_max_ul=args.random_max,
            random_min_dl=args.random_min * 5,
            random_max_dl=args.random_max * 5,
            smoothing=args.smoothing,
        )
    else:
        print(f"Unknown pattern: {pattern}, using stable", file=sys.stderr)
        return StableTrafficGenerator(base_ul=args.base_ul, base_dl=args.base_dl)


def main():
    parser = argparse.ArgumentParser(
        description="Fake SMF+UPF Server for NWDAF testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Traffic Pattern Examples:
  Stable:    --pattern stable --base-ul 100000 --jitter 5
  Periodic:  --pattern periodic --burst-period 5 --burst-multiplier 10
  Random:    --pattern random --random-min 10000 --random-max 2000000
        """,
    )
    parser.add_argument("port", nargs="?", type=int, default=8081,
                        help="Port to listen on (default: 8081)")

    # Traffic pattern selection
    pattern_group = parser.add_argument_group("Traffic Pattern")
    pattern_group.add_argument("--pattern", type=str, default="stable",
                               choices=["stable", "periodic", "random"],
                               help="Traffic pattern (default: stable)")

    # Common parameters
    common_group = parser.add_argument_group("Common Parameters")
    common_group.add_argument("--base-ul", type=int, default=2000,
                              help="Baseline UL volume in bytes (default: 2000)")
    common_group.add_argument("--base-dl", type=int, default=2000,
                              help="Baseline DL volume in bytes (default: 2000)")
    common_group.add_argument("--jitter", type=float, default=5.0,
                              help="Baseline jitter ±%% (default: 5.0)")

    # Periodic-specific
    periodic_group = parser.add_argument_group("Periodic Pattern")
    periodic_group.add_argument("--burst-period", type=int, default=5,
                                help="Ticks between bursts (default: 5)")
    periodic_group.add_argument("--burst-multiplier", type=float, default=10.0,
                                help="Volume multiplier during burst (default: 10.0)")
    periodic_group.add_argument("--burst-duration", type=int, default=1,
                                help="Burst duration in ticks (default: 1)")

    # Random-specific
    random_group = parser.add_argument_group("Random Pattern")
    random_group.add_argument("--random-min", type=int, default=500,
                              help="Min UL volume in bytes (default: 500)")
    random_group.add_argument("--random-max", type=int, default=5000,
                              help="Max UL volume in bytes (default: 5000)")
    random_group.add_argument("--smoothing", type=float, default=0.0,
                              help="Random smoothing 0-0.99: 0=chaotic, 0.9=smooth (default: 0)")

    args = parser.parse_args()

    # Build traffic generator
    generator = build_traffic_generator(args)
    FakeSmfUpfHandler.traffic_generator = generator

    server = HTTPServer(("", args.port), FakeSmfUpfHandler)
    
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║           Fake SMF+UPF Server for NWDAF Testing              ║
╠══════════════════════════════════════════════════════════════╣
║ Listening on: http://127.0.0.1:{args.port:<5}                        ║
║                                                              ║
║ Endpoints:                                                   ║
║   POST   /nsmf-event-exposure/v1/subscriptions               ║
║   DELETE /nsmf-event-exposure/v1/subscriptions/{{subId}}       ║
║                                                              ║
║ Traffic Pattern: {generator.describe():<42}║
╚══════════════════════════════════════════════════════════════╝
Press Ctrl+C to stop
""")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n👋 Server stopped")


if __name__ == "__main__":
    main()
