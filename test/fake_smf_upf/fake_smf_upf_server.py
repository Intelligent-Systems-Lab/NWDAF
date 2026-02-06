#!/usr/bin/env python3
"""
Fake SMF+UPF Server for NWDAF testing.

Simulates:
- SMF: Receives event subscription requests from NWDAF
- UPF: Sends periodic USER_DATA_USAGE_MEASURES notifications

Usage:
    uv run fake_smf_upf_server.py [port]

Example:
    uv run fake_smf_upf_server.py 8081
"""

import argparse
import json
import random
import sys
import threading
import time
import uuid
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler


class FakeSmfUpfHandler(BaseHTTPRequestHandler):
    """Handler for SMF subscription requests."""
    
    # Class-level storage for subscriptions
    subscriptions: dict = {}
    upf_notify_threads: dict = {}
    
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
            notification = self._build_upf_notification(correlation_id, target_type, target_value, count)
            
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
    
    def _build_upf_notification(self, correlation_id: str, target_type: str, target_value: str, count: int) -> dict:
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
                item = self._build_notification_item(count, ip_address=ip_addr)
                notification_items.append(item)
        elif target_type == "anyUe":
            # Simulate 2-4 random UEs
            num_ues = random.randint(2, 4)
            for i in range(num_ues):
                ip_addr = f"10.60.{random.randint(0, 255)}.{random.randint(1, 254)}"
                item = self._build_notification_item(count, ip_address=ip_addr)
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
            item = self._build_notification_item(count, ip_address=ip_addr)
            notification_items.append(item)
        
        return {
            "correlationId": correlation_id,
            "notificationItems": notification_items,
        }
    
    def _build_notification_item(self, count: int, ip_address: str = None) -> dict:
        """Build a single notification item with traffic data."""
        # Simulate varying traffic - different patterns for testing
        base_ul = 100000 + random.randint(0, 50000)  # 100KB-150KB
        base_dl = 500000 + random.randint(0, 200000)  # 500KB-700KB
        
        # Occasionally generate high traffic to test rule variations
        if count % 5 == 0:
            base_ul *= 10  # ~1MB spike
            base_dl *= 10  # ~5MB spike
        
        item = {
            "eventType": "USER_DATA_USAGE_MEASURES",
            "dnn": "internet",
            "timeStamp": datetime.now().astimezone().isoformat(),
            "userDataUsageMeasurements": [
                {
                    "volumeMeasurement": {
                        "ulVolume": base_ul,
                        "dlVolume": base_dl,
                        "totalVolume": base_ul + base_dl,
                    },
                    "throughputMeasurement": {
                        "ulThroughput": f"{base_ul * 8 // 1000} kbps",
                        "dlThroughput": f"{base_dl * 8 // 1000} kbps",
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


def main():
    parser = argparse.ArgumentParser(description="Fake SMF+UPF Server for NWDAF testing")
    parser.add_argument("port", nargs="?", type=int, default=8081, help="Port to listen on")
    args = parser.parse_args()
    
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
║ Behavior:                                                    ║
║   - Accepts SMF event subscriptions                          ║
║   - Sends UPF notifications every 10s to bundledEventNotify  ║
║   - Simulates varying traffic (occasional high-traffic spks) ║
╚══════════════════════════════════════════════════════════════╝
Press Ctrl+C to stop
""")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n👋 Server stopped")


if __name__ == "__main__":
    main()
