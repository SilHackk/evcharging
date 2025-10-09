import socket

HOST = '127.0.0.1'
PORT = 65432

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    s.sendall(b"register")
    print(s.recv(1024).decode())

    s.sendall(b"request charge")
    print(s.recv(1024).decode())
    print(s.recv(1024).decode())

    s.sendall(b"cancel")
    print(s.recv(1024).decode())
