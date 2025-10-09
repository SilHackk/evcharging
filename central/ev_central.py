import socket
import threading

# Configuration
HOST = '127.0.0.1'  # localhost
PORT = 65432        # non-privileged port

# Optional: keep track of connected clients
connected_clients = {}


def handle_client(conn, addr):
    print(f"[NEW CONNECTION] {addr}")
    cp_id = None

    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break

            message = data.decode().strip()
            print(f"[{addr}] {message}")

            if "register" in message.lower():
                cp_id = message.split(":")[0].strip()
                connected_clients[addr] = (conn, cp_id)
                print(f"[{addr}] Registered as {cp_id}")
                conn.sendall(b"Central: Registration successful")

            elif "alive" in message.lower():
                conn.sendall(b"Central: Alive received")

            elif "request charge" in message.lower():
                print(f"[{addr}] Driver requested charging.")
                conn.sendall(b"Central: Charging request received. Searching for available CP...")

                # Choose a CP (hardcoded example)
                assigned_cp = "CP_001"
                send_command_to_cp(assigned_cp, "start charging")

                conn.sendall(
                    f"Central: Charging Point {assigned_cp} assigned and started.".encode("utf-8")
                )

            elif "cancel" in message.lower():
                conn.sendall(b"Central: Request cancelled.")
                break

            else:
                conn.sendall(b"Central: Unknown command.")

    finally:
        print(f"[DISCONNECT] {addr}")
        conn.close()
        connected_clients.pop(addr, None)

def start_charging_for_cp(cp_id):
    """Send 'start charging' command to the registered CP."""
    for conn, cid in connected_clients.values():
        if cid == cp_id:
            print(f"[CENTRAL] Sending start command to {cp_id}")
            conn.sendall(b"start charging")
            return
    print(f"[CENTRAL] No CP with ID {cp_id} found.")

def start_server():
    """Starts the EV_Central socket server."""
    print(f"[STARTING] EV_Central server on {HOST}:{PORT}")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        print("[LISTENING] Server ready for connections...")

        while True:
            conn, addr = s.accept()
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.start()
            print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")

def send_command_to_cp(cp_id: str, command: str):
    """Send a command (like 'start charging' or 'stop') to a registered CP."""
    for conn, cid in connected_clients.values():
        if cid == cp_id:
            print(f"[CENTRAL] Sending '{command}' to {cp_id}")
            conn.sendall(command.encode('utf-8'))
            return True
    print(f"[CENTRAL] No CP with ID {cp_id} found.")
    return False

if __name__ == "__main__":
    start_server()
