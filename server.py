import socket
import os
import hashlib
import struct
import argparse
import threading
import tempfile

HOST = "0.0.0.0"
BUFFER_SIZE = 65536
MAX_FILENAME_SIZE = 4096
MAX_CHUNK_SIZE = 16 * 1024 * 1024
CLIENT_TIMEOUT = 120   # seconds of silence before a stale connection is dropped

STORE_LOCK = threading.Lock()


def receive_all(conn, size):
    data = bytearray()
    while len(data) < size:
        packet = conn.recv(min(BUFFER_SIZE, size - len(data)))
        if not packet:
            raise ConnectionError("Connection closed")
        data.extend(packet)
    return bytes(data)


def calculate_checksum(data):
    return hashlib.sha256(data).hexdigest()


def receive_filename(conn):
    filename_size = struct.unpack("!I", receive_all(conn, 4))[0]
    if filename_size == 0 or filename_size > MAX_FILENAME_SIZE:
        raise ValueError("Invalid filename length")
    filename = receive_all(conn, filename_size).decode("utf-8")
    if (filename in ("", ".", "..") or
            os.path.basename(filename) != filename or
            "\x00" in filename):
        raise ValueError("Invalid filename")
    return filename


def receive_chunk(conn):
    header = receive_all(conn, 8)
    chunk_id, data_size = struct.unpack("!II", header)
    if data_size > MAX_CHUNK_SIZE:
        raise ValueError("Chunk size is too large")
    checksum = receive_all(conn, 64).decode("ascii")
    data = receive_all(conn, data_size)
    return chunk_id, data, calculate_checksum(data) == checksum


def store_chunk(chunk_id, data, file_dir):
    filename = os.path.join(file_dir, f"chunk_{chunk_id}")
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
                mode="wb", dir=file_dir, prefix=f".chunk_{chunk_id}_",
                suffix=".tmp", delete=False) as file:
            temp_path = file.name
            file.write(data)
            file.flush()
            os.fsync(file.fileno())
        os.chmod(temp_path, 0o644)
        os.replace(temp_path, filename)
    finally:
        if temp_path is not None and os.path.exists(temp_path):
            os.remove(temp_path)


def get_received_chunks(file_dir):
    chunks = set()
    for filename in os.listdir(file_dir):
        if filename.startswith("chunk_"):
            chunk_text = filename[6:]
            if chunk_text.isdigit():
                chunks.add(int(chunk_text))
    return chunks


def send_status(conn, status):
    # Every acknowledgement is exactly 6 bytes
    conn.sendall(status.ljust(6).encode("ascii"))


def handle_client(conn, address, backup_dir):
    print("Connected by:", address)
    conn.settimeout(CLIENT_TIMEOUT)
    try:
        filename = receive_filename(conn)
        file_dir = os.path.join(backup_dir, filename)
        os.makedirs(file_dir, exist_ok=True)

        with STORE_LOCK:
            received_chunks = get_received_chunks(file_dir)

        print("File:", filename)
        print("Already received chunks:", len(received_chunks))

        conn.sendall(b"READY")
        conn.sendall(struct.pack("!I", len(received_chunks)))
        for chunk_id in sorted(received_chunks):
            conn.sendall(struct.pack("!I", chunk_id))

        while True:
            chunk_id, data, valid = receive_chunk(conn)

            if not valid:
                print("Checksum failed for chunk", chunk_id)
                send_status(conn, "BAD")
                continue

            with STORE_LOCK:
                chunk_path = os.path.join(file_dir, f"chunk_{chunk_id}")
                if os.path.exists(chunk_path):
                    send_status(conn, "EXISTS")
                    continue
                store_chunk(chunk_id, data, file_dir)

            print("Chunk", chunk_id, "received successfully")
            send_status(conn, "OK")

    except (ConnectionError, OSError) as error:
        print("Connection ended for", address, ":", error)
    except (ValueError, UnicodeError, struct.error) as error:
        print("Invalid client data from", address, ":", error)
    finally:
        conn.close()
        print("Connection closed:", address)


def start_server(backup_dir, port):
    os.makedirs(backup_dir, exist_ok=True)
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, port))
    server.listen(5)
    print("Server listening on port", port, flush=True)
    print("Backup directory:", os.path.abspath(backup_dir), flush=True)
    try:
        while True:
            conn, address = server.accept()
            threading.Thread(target=handle_client,
                             args=(conn, address, backup_dir),
                             daemon=True).start()
    except OSError as error:
        print("Server stopped:", error)
    finally:
        server.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Backup TCP Server")
    parser.add_argument("--dir", default="backups",
                        help="Directory for storing backup chunks")
    parser.add_argument("--port", type=int, default=5000,
                        help="TCP port to listen on")
    args = parser.parse_args()
    start_server(args.dir, args.port)
