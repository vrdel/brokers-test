import time
import sys
import stomp
import argparse

class MyListener(stomp.ConnectionListener):
    def __init__(self, arg, conn, *args, **kwargs):
        self.arg = arg
        self.conn = conn
        self.consumed = 0
        super(MyListener, self) .__init__(*args, **kwargs)

    def on_connected(self, headers, body):
        print('Listener connected, session %s' % headers['session'])
    def on_error(self, headers, message):
        print('received an error %s' % message)
    def on_message(self, headers, message):
        if self.arg.t:
            time.sleep(float(self.arg.t[0]))
        else:
            time.sleep(1)
        fo = open(self.arg.f[0], 'a+')
        fo.write(message+'\n')
        self.consumed += 1
        fo.close()
        if self.arg.a:
            self.conn.ack(headers=headers)
        if self.arg.n:
            if self.consumed == int(self.arg.n[0]):
                self.conn.disconnect()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', nargs=1, required=True, help='broker', metavar='broker')
    parser.add_argument('-t', nargs=1, default=False, help='recv msg every sec', metavar='float')
    parser.add_argument('-a', action='store_true', default=False, help='client ack')
    parser.add_argument('-r', action='store_true', default=False, help='reconnect')
    parser.add_argument('-n', nargs=1, default=False, help='num msgs', metavar='int')
    parser.add_argument('-f', nargs=1, required=True, help='write to file', metavar='file')
    parser.add_argument('-d', nargs=1, required=True, help='destination', metavar='dest')
    args = parser.parse_args()

    conn = stomp.Connection([(args.s[0], 6163)], use_ssl=False)
    conn.set_listener('MyListener', MyListener(args, conn))
    conn.start()
    conn.connect()
    if args.a:
        conn.subscribe(destination=args.d[0], id=1, ack='client')
    else:
        conn.subscribe(destination=args.d[0], id=1, ack='auto')

    conn.create_thread_fc.join()

    while True:
        try:
            if args.t:
                time.sleep(float(args.t[0]))
            else:
                time.sleep(1)
        except KeyboardInterrupt:
            raise SystemExit(1)

main()
