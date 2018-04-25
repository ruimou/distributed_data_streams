# Source code modified by: Jan Tache
#
# Base source code:
# https://gist.github.com/mafayaz/faf938a896357c3a4c9d6da27edcff08
#
# Usage: python <filename> <port number> <IP address>
# Very simple webserver

from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
from SocketServer import ThreadingMixIn
import threading
import argparse
import socket

data = []
dataLock = threading.Lock()

class HTTPRequestHandler(BaseHTTPRequestHandler):
    # Code to handle HTTP POST
    def do_GET(s):
        if s.path == '/heatmapdata':
            s.send_response(200)
            s.send_header("Content-type", "text")
            s.end_headers()

            dataLock.acquire()
            for v in data:
                s.wfile.write(v)
            del data[:]
            dataLock.release()

            # one fixed write in case there is no data
            s.wfile.write("done")
            return

        s.send_response(200)
        s.send_header("Content-type", "text/html")
        s.end_headers()

        if s.path == '/':
            with open('index.html', 'r') as f:
                s.wfile.write(f.read())
                return

        try:
            with open(s.path[1:], 'r') as f:
                s.wfile.write(f.read())
        except:
            s.wfile.write("")

# Rest of source code was unmodified from the original; setting up HTTP server
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    allow_reuse_address = True

    def shutdown(self):
        self.socket.close()
        HTTPServer.shutdown(self)

class SimpleHttpServer():
    def __init__(self, ip, port):
        self.server = ThreadedHTTPServer((ip,port), HTTPRequestHandler)

    def start(self):
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()

    def waitForThread(self):
        self.server_thread.join()

    def addRecord(self, recordID, jsonEncodedRecord):
        LocalData.records[recordID] = jsonEncodedRecord

    def stop(self):
        self.server.shutdown()
        self.waitForThread()

class DataListener():
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port

    def start(self):
        t = threading.Thread(target=self.listenData)
        t.start()
    
    def listenData(self):
        ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ssock.bind((self.ip, self.port))
        ssock.listen(5)

        while True:
            (csock, addr) = ssock.accept()
            print("Got new connection!")
            t = threading.Thread(target=self.listenClient, args=[csock])
            t.start()

    def listenClient(self, csock):
        while True:
            d = csock.recv(512)
            if d == '':
                # No more data from this dood
                csock.close()
                return

            print("Received data:" + d)

            dataLock.acquire()
            data.append(d)
            dataLock.release()


if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Simple HTTP Server')
    parser.add_argument('http_ip', help='HTTP Server IP')
    parser.add_argument('http_port', type=int, help='Listening port for HTTP Server')
    parser.add_argument('internal_ip', help='Internal data API')
    parser.add_argument('internal_port', type=int, help='Internal data port')
    args = parser.parse_args()

    server = SimpleHttpServer(args.http_ip, args.http_port)
    listener = DataListener(args.internal_ip, args.internal_port)
    print 'HTTP Server Running...........'
    listener.start()
    server.start()
    server.waitForThread()
