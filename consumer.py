#!/usr/bin/python

from stompest.config import StompConfig
from stompest.protocol import StompSpec
from stompest.sync import Stomp
import stompest.error
import time
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', action='store_true', default=False, help='client ack')
    parser.add_argument('-b', action='store_true', default=False, help='write body only')
    parser.add_argument('-d', nargs=1, required=True, help='destination', metavar='dest')
    parser.add_argument('-f', nargs=1, required=True, help='file with recorded msgs', metavar='file')
    parser.add_argument('-n', nargs=1, default=False, help='number of msgs', metavar='int')
    parser.add_argument('-r', action='store_true', default=False, help='reconnect')
    parser.add_argument('-s', nargs=1, required=True, help='broker', metavar='broker')
    parser.add_argument('-t', nargs=1, default=False, help='recv msg every sec', metavar='float')
    args = parser.parse_args()

    broker = 'tcp://%s:6163' % (args.s[0])
    config = StompConfig(broker)

    client = Stomp(config)
    if not args.r:
        client.connect()
        if args.a:
            client.subscribe(args.d[0], {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL})
        else:
            client.subscribe(args.d[0], {StompSpec.ACK_HEADER: StompSpec.ACK_AUTO})
    try:
        consumed = 0
        while True:
            if args.r:
                client.connect()
                if args.a:
                    client.subscribe(args.d[0], {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL})
                else:
                    client.subscribe(args.d[0], {StompSpec.ACK_HEADER: StompSpec.ACK_AUTO})
            if args.t:
                time.sleep(float(args.t[0]))
            else:
                time.sleep(1)
            fo = open(args.f[0], 'a+')
            frame = client.receiveFrame()
            consumed += 1
            if args.b:
                fo.write(frame.body+'\n')
            else:
                fo.write(frame.info()+'\n')
            fo.close()
            if args.a:
                client.ack(frame)
            if args.r:
                client.disconnect()
            if args.n:
                if consumed == int(args.n[0]):
                    raise KeyboardInterrupt
    except KeyboardInterrupt:
        client.stop()
        client.disconnect()
        raise SystemExit(1)
    except stompest.error.StompProtocolError:
        pass

main()
