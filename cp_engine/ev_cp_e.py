import socket
import time
import threading

# Configuration
CENTRAL_HOST = '127.0.0.1'  # IP address of the Central Server
CENTRAL_PORT = 65432        # Port of the Central Server
CP_ID = "CP_001"            # Unique ID for this Charging Point

stop_flag = threading.Event()

def listen_to_central(sock):
    """Background listener for Central commands."""
    while not stop_flag.is_set():
        try:
            data = sock.recv(1024)
            if not data:
                break
            msg = data.decode().strip()
            print(f"[CENTRAL CMD] {msg}")

            if "start charging" in msg.lower():
                print("[CP] Charging... ⚡")

            elif "stop" in msg.lower():
                print("[CP] Stopping charge.")
                stop_flag.set()
                break

        except Exception as e:
            print(f"[CP ERROR] Listener: {e}")
            break

def start_cp_engine():
    """Connects to Central, registers, and starts sending 'I'm alive' messages."""
    print(f"Starting EV_CP_E {CP_ID}...")
    try:
        # Create a socket object
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # Connect to the server
            s.connect((CENTRAL_HOST, CENTRAL_PORT))
            print(f"Successfully connected to Central at {CENTRAL_HOST}:{CENTRAL_PORT}")

            # 1. Initial registration message
            reg_message = f"{CP_ID}: register"
            print(f"Sending registration message: '{reg_message}'")
            s.sendall(reg_message.encode('utf-8'))

            # Wait for and print registration acknowledgement
            # After receiving Central's response to registration
            ack_data = s.recv(1024)
            print(f"Central Response: {ack_data.decode('utf-8')}")

            # Start listening for Central commands in the background
            threading.Thread(target=listen_to_central, args=(s,), daemon=True).start()


            # 2. Start sending "I'm alive" messages (Goal part)
            while True:
                alive_message = f"{CP_ID}: I'm alive"
                print(f"Sending keep-alive message: '{alive_message}'")
                s.sendall(alive_message.encode('utf-8'))

                # Wait for acknowledgment
                ack_data = s.recv(1024)
                print(f"Central Response: {ack_data.decode('utf-8')}")

                time.sleep(5)  # Wait 5 seconds before next keep-alive

    except ConnectionRefusedError:
        print("Connection failed: Central server is likely not running.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("EV_CP_E stopped.")

if __name__ == "__main__":
    # To run this, save it as a file (e.g., ev_cp_engine.py) and execute:
    # python ev_cp_engine.py
    # NOTE: Ensure Person A's server script is running FIRST.
    start_cp_engine()