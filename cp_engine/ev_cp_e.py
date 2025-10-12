import socket
import time
import threading
import random
from kafka_logger import get_kafka_producer, log_message

# Configuration
CENTRAL_HOST = '127.0.0.1'
CENTRAL_PORT = 65432
CP_ID = "CP_001"
kafka_producer = get_kafka_producer()

# Threading events to control flow
stop_event = threading.Event()
fault_event = threading.Event()
charging_active = threading.Event()

def listen_to_central(sock):
    """Listens for commands from the Central Server in a background thread."""
    while not stop_event.is_set():
        try:
            data = sock.recv(1024)
            if not data:
                print("[CP] Connection to Central lost.")
                stop_event.set()
                break

            msg = data.decode().strip()
            print(f"\n[CENTRAL CMD] Received: '{msg}'")
            log_message(kafka_producer, CP_ID, {"event": "command_received", "command": msg})

            if "start charging" in msg.lower():
                if not charging_active.is_set():
                    charging_active.set()
                    fault_event.clear() # Reset fault status before starting
                    charge_thread = threading.Thread(target=simulate_charging, args=(sock,))
                    charge_thread.start()
                else:
                    print("[CP] Already charging.")

            elif "stop" in msg.lower():
                print("[CP] Stop command received. Halting operations.")
                stop_event.set()

        except Exception as e:
            print(f"[CP ERROR] Listener error: {e}")
            stop_event.set()
            break

def simulate_charging(sock):
    """Simulates the charging process, sending progress updates every second."""
    print("[CP] Charging process initiated... ⚡")
    log_message(kafka_producer, CP_ID, {"event": "charging_started"})
    for progress in range(0, 101):
        if stop_event.is_set() or fault_event.is_set():
            print("[CP] Charging interrupted.")
            log_message(kafka_producer, CP_ID, {"event": "charging_interrupted", "reason": "fault or stop command"})
            break

        progress_msg = f"{CP_ID}: progress: {progress}%"
        try:
            sock.sendall(progress_msg.encode('utf-8'))
            print(f"  -> Sent progress: {progress}%")
            time.sleep(1) # Send update every 1 second
        except Exception as e:
            print(f"[CP ERROR] Failed to send progress: {e}")
            stop_event.set()
            break

    if not fault_event.is_set() and not stop_event.is_set():
        print("[CP] Charging complete! ✅")
        log_message(kafka_producer, CP_ID, {"event": "charging_complete"})
    
    charging_active.clear()

def monitor_faults(sock):
    """Simulates a random fault and notifies the Central Server."""
    # Wait a random time before a fault occurs
    fault_time = random.randint(10, 25)
    print(f"[MONITOR] Fault simulation active. A fault may occur in {fault_time} seconds.")
    time.sleep(fault_time)
    
    if not charging_active.is_set():
        print("[MONITOR] No charging in progress. Monitor standing down.")
        return

    fault_event.set()
    fault_type = random.choice(["Overheating", "Power Fluctuation", "Connection Error"])
    print(f"\n🚨 [MONITOR] Fault simulated: {fault_type}!")
    
    fault_msg = f"{CP_ID}: fault: {fault_type}"
    try:
        sock.sendall(fault_msg.encode('utf-8'))
        log_message(kafka_producer, CP_ID, {"event": "fault_detected", "fault_type": fault_type})
    except Exception as e:
        print(f"[MONITOR] Could not send fault message: {e}")
        stop_event.set()

def start_cp_engine():
    """Main function to connect to Central, register, and start operations."""
    log_message(kafka_producer, CP_ID, {"event": "cp_startup"})
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print(f"[CP] Connecting to Central at {CENTRAL_HOST}:{CENTRAL_PORT}...")
            s.connect((CENTRAL_HOST, CENTRAL_PORT))
            print("[CP] Connected successfully.")

            # 1. Register with the Central Server
            reg_message = f"{CP_ID}: register"
            s.sendall(reg_message.encode('utf-8'))
            ack = s.recv(1024).decode('utf-8')
            print(f"[CENTRAL] {ack}")

            # 2. Start background threads
            listener = threading.Thread(target=listen_to_central, args=(s,))
            listener.start()

            # For demonstration, we'll start the fault monitor automatically.
            # In a real scenario, it would run continuously.
            monitor = threading.Thread(target=monitor_faults, args=(s,))
            monitor.start()

            # 3. Main loop for sending "I'm alive" messages
            while not stop_event.is_set():
                alive_message = f"{CP_ID}: I'm alive"
                s.sendall(alive_message.encode('utf-8'))
                time.sleep(10) # Send keep-alive every 10 seconds

    except ConnectionRefusedError:
        print("[CP ERROR] Connection failed. Is the Central Server running?")
    except Exception as e:
        print(f"[CP ERROR] An unexpected error occurred: {e}")
    finally:
        stop_event.set()
        print(f"[CP] {CP_ID} is shutting down.")
        log_message(kafka_producer, CP_ID, {"event": "cp_shutdown"})

if __name__ == "__main__":
    start_cp_engine()