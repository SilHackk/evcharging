import socket
import threading

# Configuration
HOST = '127.0.0.1'  # localhost
PORT = 65432        # non-privileged port

# Optional: keep track of connected clients
connected_clients = {}


def handle_client(conn, addr):
    """Handles communication with a single client."""
    print(f"[NEW CONNECTION] {addr} connected.")
    connected_clients[addr] = conn

    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break  # Client disconnected

            message = data.decode('utf-8').strip()
            print(f"[{addr}] Message: {message}")

            # Handle different message types
            if "i'm alive" in message.lower():
                print(f"[{addr}] Heartbeat received.")
                conn.sendall(b"Central: Alive received")

            elif "register" in message.lower():
                print(f"[{addr}] Registration attempt.")
                conn.sendall(b"Central: Registration successful")

            elif "request charge" in message.lower():
                print(f"[{addr}] Driver requested charging.")
                conn.sendall(
                    b"Central: Charging request received. "
                    b"Searching for available CP..."
                )

                # Simulate assigning a charging point
                # (In a real system you'd query Kafka, a DB, etc.)
                conn.sendall(b"Central: Charging Point CP-01 assigned. Proceed to location.")

            elif "cancel" in message.lower():
                print(f"[{addr}] Driver cancelled the request.")
                conn.sendall(b"Central: Request cancelled. Goodbye!")

            else:
                print(f"[{addr}] Unknown command: {message}")
                conn.sendall(b"Central: Unknown command. Try 'register', 'request charge', or 'cancel'.")

    except ConnectionResetError:
        print(f"[ERROR] Connection with {addr} reset by peer.")
    except Exception as e:
        print(f"[ERROR] Exception handling {addr}: {e}")
    finally:
        print(f"[DISCONNECT] Connection closed for {addr}")
        conn.close()
        connected_clients.pop(addr, None)

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

if __name__ == "__main__":
    start_server()
