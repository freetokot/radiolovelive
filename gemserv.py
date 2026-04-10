import requests
import threading
import time
import logging
import struct
import json
from collections import deque
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

# --- Configuration ---
# Server configuration
HOST = '0.0.0.0'
PORT = 8000

# Stream Bitrate and Buffer Configuration
BITRATE_KBPS = 128  # Assumed bitrate of the source stream
BUFFER_DURATION_S = 330  # 5 minutes of audio buffer
BYTES_PER_SECOND = BITRATE_KBPS * 1000 // 8
BUFFER_SIZE_BYTES = BYTES_PER_SECOND * BUFFER_DURATION_S
CHUNK_SIZE_BYTES = 4096  # How much data to read from the source at a time

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Global Shared State ---
# A deque is used for efficient appends and pops from both ends
audio_buffer = deque()
buffer_lock = threading.Lock()
# This must only be accessed when buffer_lock is held
chunk_id_counter = 0

class StreamCacher(threading.Thread):
    """
    A thread that continuously connects to an audio source, downloads the stream,
    and caches it in a global, thread-safe buffer.
    """
    def __init__(self, stream_url_template):
        super().__init__(name="StreamCacher")
        self.stream_url_template = stream_url_template
        self.daemon = True

    def run(self):
        global chunk_id_counter
        global audio_buffer
        pending_data = bytearray() # Промежуточный буфер

        while True:
            try:
                stream_url = self.stream_url_template.format(token=int(time.time()))
                with requests.get(stream_url, stream=True, timeout=10) as r:
                    for raw_data in r.iter_content(chunk_size=1024): # Читаем мелко
                        if not raw_data: continue
                        pending_data.extend(raw_data)

                        # Пока накопилось больше или равно CHUNK_SIZE_BYTES
                        while len(pending_data) >= CHUNK_SIZE_BYTES:
                            chunk_to_save = bytes(pending_data[:CHUNK_SIZE_BYTES])
                            del pending_data[:CHUNK_SIZE_BYTES]

                            with buffer_lock:
                                audio_buffer.append((chunk_id_counter, chunk_to_save))
                                chunk_id_counter += 1

                            # Trim the buffer from the left if it exceeds the target size
                            while total_bytes_in_buffer > BUFFER_SIZE_BYTES:
                                _id, old_chunk = audio_buffer.popleft()
                                total_bytes_in_buffer -= len(old_chunk)
                                logging.debug(f"Removed old chunk ID {_id} to maintain buffer size.")

            except Exception as e:
                logging.error(f"StreamCacher error: {e}. Reconnecting in 5 seconds.")
                time.sleep(5)

class APIRequestHandler(BaseHTTPRequestHandler):
    """
    Handles incoming HTTP requests for stream data and buffer information.
    """
    def version_string(self):
        # Hides the Python version for minor security hardening
        return "IntelligentStreamServer/1.0"
    
    def _send_headers(self, status_code=200, content_type='application/json', extra_headers=None):
        self.send_response(status_code)
        self.send_header('Content-type', content_type)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
        self.send_header('Access-Control-Expose-Headers', 'X-Chunks-Sent, X-Next-Chunk-Id')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        if extra_headers:
            for key, value in extra_headers.items():
                self.send_header(key, value)
        self.end_headers()

    def _send_json_response(self, data, status_code=200):
        self._send_headers(status_code=status_code, content_type='application/json; charset=utf-8')
        self.wfile.write(json.dumps(data, indent=2).encode('utf-8'))

    def do_OPTIONS(self):
        """Handle CORS preflight requests."""
        self._send_headers(status_code=204)

    def do_GET(self):
        """Routes GET requests to the appropriate handler."""
        client_ip = self.client_address[0]
        logging.info(f"Request from {client_ip}: {self.command} {self.path}")
        
        parsed_path = urlparse(self.path)
        try:
            if parsed_path.path == '/buffer_info':
                self.handle_buffer_info()
            elif parsed_path.path == '/fetch':
                self.handle_fetch_chunks(parsed_path)
            else:
                self._send_json_response({"error": "Not Found"}, status_code=404)
        except Exception as e:
            logging.error(f"Unhandled exception while processing request for {self.path}: {e}", exc_info=True)
            if not self.wfile.closed:
                try:
                    self._send_json_response({"error": "Internal Server Error"}, status_code=500)
                except Exception as ex:
                    logging.error(f"Failed to send 500 error response: {ex}")

    def handle_buffer_info(self):
        """Responds with metadata about the current state of the audio buffer."""
        with buffer_lock:
            if not audio_buffer:
                info = {
                    "buffer_status": "empty",
                    "oldest_chunk_id": None,
                    "latest_chunk_id": None,
                    "total_chunks": 0,
                    "buffer_duration_seconds": 0
                }
            else:
                oldest_id = audio_buffer[0][0]
                latest_id = audio_buffer[-1][0]
                total_chunks = len(audio_buffer)
                buffered_bytes = sum(len(c) for _, c in audio_buffer)
                duration = buffered_bytes / BYTES_PER_SECOND
                info = {
                    "buffer_status": "ok",
                    "oldest_chunk_id": oldest_id,
                    "latest_chunk_id": latest_id,
                    "total_chunks": total_chunks,
                    "buffer_duration_seconds": round(duration)
                }
        logging.info(f"Serving /buffer_info: {info}")
        self._send_json_response(info)

    def handle_fetch_chunks(self, parsed_path):
        """
        Serves a binary package of audio chunks starting from a requested ID.
        This is the recommended method for clients to consume the stream.
        """
        params = parse_qs(parsed_path.query)
        try:
            start_id = int(params['start_id'][0])
        except (KeyError, IndexError, ValueError):
            logging.warning(f"Bad request for /fetch: missing or invalid 'start_id'. Query: {parsed_path.query}")
            self._send_json_response({"error": "Missing or invalid 'start_id' parameter."}, status_code=400)
            return

        with buffer_lock:
            if not audio_buffer:
                logging.warning("Client requested /fetch, but buffer is empty.")
                self._send_json_response({"error": "Service Unavailable: Buffer is currently empty."}, status_code=503)
                return

            oldest_id = audio_buffer[0][0]
            if start_id < oldest_id:
                logging.warning(f"Client requested stale chunk ID {start_id} (oldest is {oldest_id}).")
                self._send_json_response(
                    {"error": f"Requested chunk ID {start_id} is no longer in the buffer. Oldest available is {oldest_id}."},
                    status_code=410  # Gone
                )
                return

            # Find the starting index in the deque for the requested chunk ID
            start_index = -1
            for i, (chunk_id, _) in enumerate(audio_buffer):
                if chunk_id >= start_id:
                    start_index = i
                    break

            if start_index == -1:
                logging.warning(f"Client requested chunk ID {start_id}, which was not found in the buffer.")
                self._send_json_response({"error": f"Chunk ID {start_id} not found."}, status_code=404)
                return

            # Limit chunks sent in one response to prevent excessively large HTTP responses
            MAX_CHUNKS_PER_RESPONSE = 25
            end_index = min(start_index + MAX_CHUNKS_PER_RESPONSE, len(audio_buffer))
            
            # Prepare the binary response data while holding the lock
            response_data = bytearray()
            for i in range(start_index, end_index):
                chunk_id, chunk_data = audio_buffer[i]
                # Pack chunk ID (unsigned long long, 8 bytes) and size (unsigned int, 4 bytes)
                header = struct.pack("!QI", chunk_id, len(chunk_data))
                response_data.extend(header)
                response_data.extend(chunk_data)

            # Determine the ID of the next chunk that will be available
            next_chunk_id = audio_buffer[end_index][0] if end_index < len(audio_buffer) else None
            chunks_sent = end_index - start_index
        
        # Send headers and binary data AFTER releasing the lock
        logging.info(f"Serving /fetch for start_id {start_id}. Sending {chunks_sent} chunks, next ID is {next_chunk_id}.")
        extra_headers = {
            'X-Chunks-Sent': str(chunks_sent),
            # Send the next ID so the client knows what to ask for next
            'X-Next-Chunk-Id': str(next_chunk_id) if next_chunk_id is not None else ''
        }
        self._send_headers(status_code=200, content_type='application/octet-stream', extra_headers=extra_headers)
        self.wfile.write(response_data)

def run_server(host, port):
    server_address = (host, port)
    httpd = HTTPServer(server_address, APIRequestHandler)
    logging.info(f"Intelligent streaming server starting on http://{host}:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        logging.info("Server shutting down.")

if __name__ == '__main__':
    # The source URL requires a unique token per connection. Using unix timestamp is a simple way.
    SOURCE_URL_TEMPLATE = "https://streaming.radiostreamlive.com/radiolovelive_devices?token={token}"
    
    cacher = StreamCacher(SOURCE_URL_TEMPLATE)
    cacher.start()
    
    logging.info("Waiting for initial buffer fill (10s)...")
    time.sleep(10)
    
    run_server(host=HOST, port=PORT)