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
        
        # Show any other unknown top-level fields
        known_keys = {"subscriptionId", "notifCorrId", "termCause", "eventNotifications"}
        extra_keys = set(notification.keys()) - known_keys
        if extra_keys:
            print(f"\n📋 Additional Fields:")
            for key in extra_keys:
                print(f"  {key}: {notification[key]}")

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

            abnor_behavrs = event.get("abnorBehavrs", [])
            for j, ab in enumerate(abnor_behavrs, 1):
                print(f"\n  Abnormal Behaviour {j}:")
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
            
            # Show any other event fields not explicitly handled
            known_event_keys = {"event", "abnorBehavrs", "start", "expiry", "failNotifyCode"}
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

    def _timestamp(self) -> str:
        return datetime.now().strftime("%H:%M:%S")

    def log_message(self, format, *args):
        """Suppress default logging."""
        pass


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9090
    server = HTTPServer(("", port), NotificationHandler)
    print(f"🚀 Callback server listening on http://localhost:{port}")
    print("Press Ctrl+C to stop\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n👋 Server stopped")


if __name__ == "__main__":
    main()
