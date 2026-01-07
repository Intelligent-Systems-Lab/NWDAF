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
        print(f"Subscription ID: {notification.get('subscriptionId', 'N/A')}")

        event_notifications = notification.get("eventNotifications", [])
        for i, event in enumerate(event_notifications, 1):
            print(f"\n--- Event {i} ---")
            print(f"Event Type: {event.get('event', 'N/A')}")

            abnor_behavrs = event.get("abnorBehavrs", [])
            for j, ab in enumerate(abnor_behavrs, 1):
                print(f"\n  Abnormal Behaviour {j}:")
                excep = ab.get("excep", {})
                print(f"    ExceptionId: {excep.get('excepId', 'N/A')}")
                print(f"    ExceptionLevel: {excep.get('excepLevel', 'N/A')}")
                print(f"    ExceptionTrend: {excep.get('excepTrend', 'N/A')}")
                print(f"    Ratio: {ab.get('ratio', 'N/A')}%")
                print(f"    Confidence: {ab.get('confidence', 'N/A')}%")

                addtl = ab.get("addtMeasInfo", {})
                if ddos := addtl.get("ddosAttack"):
                    print(f"    DDoS Attack IPs: {ddos.get('ipv4Addrs', [])}")

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
