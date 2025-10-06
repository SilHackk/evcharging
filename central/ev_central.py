import socket
import threading

# Configuration
HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
PORT = 65432        # Port to listen on (non-privileged ports are > 1023)

def handle_client(conn, addr):
    """Handles communication with a single client."""
    print(f"Connected by {addr}")
    try:
        while True:
            # Receive data from the client
            data = conn.recv(1024)
            if not data:
                break # Connection closed by client

            message = data.decode('utf-8')
            print(f"[{addr}] Received: {message}")

            # Check for the registration or "I'm alive" message
            if "I'm alive" in message:
                print(f"[{addr}] Acknowledging 'I'm alive'.")
                conn.sendall(b"Central: Alive received")
            elif "register" in message.lower():
                print(f"[{addr}] Registration attempt received.")
                conn.sendall(b"Central: Registration successful")

    except ConnectionResetError:
        print(f"Connection with {addr} reset by peer.")
    except Exception as e:
        print(f"Error handling client {addr}: {e}")
    finally:
        print(f"Connection closed for {addr}")
        conn.close()

def start_server():
    """Starts the EV_Central socket server."""
    print(f"Starting EV_Central server on {HOST}:{PORT}...")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        print("Server is listening for connections...")

        while True:
            # Accept a new connection
            conn, addr = s.accept()
            # Start a new thread to handle the client connection
            client_thread = threading.Thread(target=handle_client, args=(conn, addr))
            client_thread.start()

if __name__ == "__main__":
    # To run this, save it as a file (e.g., ev_central.py) and execute:
    # python ev_central.py
    start_server()