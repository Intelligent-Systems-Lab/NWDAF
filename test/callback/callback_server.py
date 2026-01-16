#!/usr/bin/env python3
"""
Simple notification callback server for testing NWDAF subscription notifications.

Usage:
    uv run callback_server.py [port]

Example:
    uv run callback_server.py 9090
"""

import json
import sys
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler


class NotificationHandler(BaseHTTPRequestHandler):
    """Handler for NWDAF notification callbacks."""

    def do_POST(self):
        """Handle incoming notification."""
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)

        try:
            notification = json.loads(body)
            self._print_notification(notification)
        except json.JSONDecodeError:
            print(f"[{self._timestamp()}] ❌ Invalid JSON received")
            print(body.decode())

        # Respond with 204 No Content per TS 29.520
        self.send_response(204)
        self.end_headers()

    def _print_notification(self, notification: dict):
        """Pretty print notification."""
        print(f"\n{'=' * 60}")
        print(f"[{self._timestamp()}] 📩 Notification Received")
        print(f"{'=' * 60}")
        
        # Core fields
        print(f"Subscription ID: {notification.get('subscriptionId', 'N/A')}")
        
        # Optional top-level fields
        if corr_id := notification.get("notifCorrId"):
            print(f"Correlation ID: {corr_id}")
        if term_cause := notification.get("termCause"):
            print(f"Termination Cause: {term_cause}")

        event_notifications = notification.get("eventNotifications", [])
        for i, event in enumerate(event_notifications, 1):
            print(f"\n--- Event {i} ---")
            print(f"Event Type: {event.get('event', 'N/A')}")
            
            # Show optional event-level fields
            if start := event.get("start"):
                print(f"Start Time: {start}")
            if expiry := event.get("expiry"):
                print(f"Expiry Time: {expiry}")
            if fail_code := event.get("failNotifyCode"):
                print(f"⚠️ Failure Code: {fail_code}")

            # UE Communication (primary event type)
            ue_comms = event.get("ueComms", [])
            if ue_comms:
                self._print_ue_communications(ue_comms)
            
            # Abnormal Behaviours (legacy/fallback)
            abnor_behavrs = event.get("abnorBehavrs", [])
            if abnor_behavrs:
                self._print_abnormal_behaviours(abnor_behavrs)
            
            # Show any other event fields not explicitly handled
            known_event_keys = {"event", "ueComms", "abnorBehavrs", "start", "expiry", "failNotifyCode"}
            extra_event_keys = set(event.keys()) - known_event_keys
            if extra_event_keys:
                print(f"\n  📋 Other Event Fields:")
                for key in extra_event_keys:
                    val = event[key]
                    if isinstance(val, (dict, list)):
                        print(f"    {key}: {json.dumps(val, indent=6)[:200]}...")
                    else:
                        print(f"    {key}: {val}")

        print(f"\n{'=' * 60}\n")

    def _print_ue_communications(self, ue_comms: list):
        """Print UE Communication analytics."""
        for j, ue_comm in enumerate(ue_comms, 1):
            print(f"\n  📱 UE Communication {j}:")
            
            # Required fields
            if comm_dur := ue_comm.get("commDur"):
                print(f"    Communication Duration: {comm_dur}s")
            if ts := ue_comm.get("ts"):
                print(f"    Timestamp: {ts}")
            
            # Traffic Characterization
            if traf_char := ue_comm.get("trafChar"):
                print(f"    Traffic Info:")
                if dnn := traf_char.get("dnn"):
                    print(f"      DNN: {dnn}")
                if "ulVol" in traf_char:
                    print(f"      Uplink Volume: {self._format_bytes(traf_char['ulVol'])}")
                if "dlVol" in traf_char:
                    print(f"      Downlink Volume: {self._format_bytes(traf_char['dlVol'])}")
                if ul_rate := traf_char.get("ulRate"):
                    print(f"      Uplink Rate: {ul_rate}")
                if dl_rate := traf_char.get("dlRate"):
                    print(f"      Downlink Rate: {dl_rate}")
            
            # Optional fields (always show if present, even if 0)
            if "confidence" in ue_comm:
                print(f"    Confidence: {ue_comm['confidence']}%")
            if ratio := ue_comm.get("ratio"):
                print(f"    Ratio: {ratio}%")

    def _print_abnormal_behaviours(self, abnor_behavrs: list):
        """Print Abnormal Behaviour analytics (legacy)."""
        for j, ab in enumerate(abnor_behavrs, 1):
            print(f"\n  ⚠️ Abnormal Behaviour {j}:")
            excep = ab.get("excep", {})
            print(f"    ExceptionId: {excep.get('excepId', 'N/A')}")
            print(f"    ExceptionLevel: {excep.get('excepLevel', 'N/A')}")
            print(f"    ExceptionTrend: {excep.get('excepTrend', 'N/A')}")
            
            if ratio := ab.get("ratio"):
                print(f"    Ratio: {ratio}%")
            if confidence := ab.get("confidence"):
                print(f"    Confidence: {confidence}%")
            if supis := ab.get("supis"):
                print(f"    SUPIs: {supis}")
            if dnn := ab.get("dnn"):
                print(f"    DNN: {dnn}")

            addtl = ab.get("addtMeasInfo", {})
            if ddos := addtl.get("ddosAttack"):
                print(f"    DDoS Attack IPs: {ddos.get('ipv4Addrs', [])}")

    def _format_bytes(self, bytes_val: int) -> str:
        """Format bytes to human readable string."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if abs(bytes_val) < 1024.0:
                return f"{bytes_val:.1f} {unit}"
            bytes_val /= 1024.0
        return f"{bytes_val:.1f} TB"

    def _timestamp(self) -> str:
        return datetime.now().strftime("%H:%M:%S")

    def log_message(self, format, *args):
        """Suppress default logging."""
        pass


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9090
    server = HTTPServer(("", port), NotificationHandler)
    print(f"🚀 Callback server listening on http://localhost:{port}")
    print("Supported events: UE_COMMUNICATION")
    print("Press Ctrl+C to stop\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n👋 Server stopped")


if __name__ == "__main__":
    main()
