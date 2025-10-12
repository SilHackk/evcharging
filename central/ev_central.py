import socket
import threading
from kafka_logger import get_kafka_producer, log_message

# Configuration
HOST = '127.0.0.1'
PORT = 65432
kafka_producer = get_kafka_producer()

# Store client connections and their IDs
connected_clients = {}

def handle_client(conn, addr):
    print(f"[NEW CONNECTION] {addr} connected.")
    log_message(kafka_producer, str(addr), {"event": "new_connection", "client_ip": addr[0]})
    cp_id = None

    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break # Connection closed by client

            message = data.decode().strip()
            print(f"[{addr}] Received: {message}")

            # Extract CP_ID from messages that include it
            if ":" in message:
                cp_id_from_msg = message.split(":")[0].strip()
                if cp_id_from_msg.startswith("CP_"):
                    cp_id = cp_id_from_msg

            # Message Routing
            if "register" in message.lower():
                connected_clients[addr] = (conn, cp_id)
                print(f"[{addr}] Registered as {cp_id}")
                log_message(kafka_producer, cp_id, {"event": "registration", "status": "success"})
                conn.sendall(b"Central: Registration successful")

            elif "alive" in message.lower():
                conn.sendall(b"Central: Alive received")

            elif "progress" in message.lower():
                progress = message.split(":")[2].strip()
                print(f"[PROGRESS] {cp_id} is at {progress}.")
                log_message(kafka_producer, cp_id, {"event": "charging_progress", "progress": progress})
                # No response needed for progress updates to avoid network clutter

            elif "fault" in message.lower():
                fault_type = message.split(":")[2].strip()
                print(f"🚨 [FAULT DETECTED] {cp_id} reported: {fault_type}")
                log_message(kafka_producer, cp_id, {"event": "fault_reported", "fault_type": fault_type, "severity": "critical"})
                conn.sendall(b"Central: Fault acknowledged. Taking action.")

            elif "request charge" in message.lower():
                # This part is for a driver/app client, not the CP itself.
                # Hardcoded for demonstration.
                assigned_cp = "CP_001"
                print(f"[{addr}] Driver requested charging. Assigning to {assigned_cp}")
                log_message(kafka_producer, assigned_cp, {"event": "charge_request_received", "assigned_to": assigned_cp})
                send_command_to_cp(assigned_cp, "start charging")
                conn.sendall(f"Central: Charging Point {assigned_cp} assigned.".encode())

            else:
                conn.sendall(b"Central: Unknown command.")

    except ConnectionResetError:
        print(f"[CONNECTION LOST] {addr} disconnected unexpectedly.")
    finally:
        print(f"[DISCONNECT] {addr}")
        if cp_id:
            log_message(kafka_producer, cp_id, {"event": "disconnect", "client_ip": addr[0]})
        conn.close()
        connected_clients.pop(addr, None)

def send_command_to_cp(cp_id: str, command: str):
    """Sends a command to a specific registered Charging Point."""
    for addr, (conn, cid) in connected_clients.items():
        if cid == cp_id:
            print(f"[CENTRAL] Sending '{command}' to {cp_id}")
            log_message(kafka_producer, cp_id, {"event": "command_sent", "command": command})
            conn.sendall(command.encode('utf-8'))
            return True
    print(f"[CENTRAL] Could not find registered CP with ID {cp_id}.")
    return False

def start_server():
    """Starts the EV Central socket server."""
    print(f"[STARTING] EV_Central server on {HOST}:{PORT}")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen()
        print("[LISTENING] Server is ready for connections...")

        while True:
            conn, addr = s.accept()
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.start()
            print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")

if __name__ == "__main__":
    start_server()