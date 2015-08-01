#!/usr/bin/python

from stompest.config import StompConfig
from stompest.sync import Stomp
import sys, random, time
import argparse

def send_msg(args, client, i, rd):
    if args.t:
        time.sleep(float(args.t[0]))
    else:
        time.sleep(1)
    if args.p:
        msg = '%s-MSG(%i): %s' % (args.p[0], i, args.m[0])
    else:
        msg = '%s-MSG(%i): %s' % (rd, i, args.m[0])
    print msg
    if args.e:
        client.send(args.d[0], msg, {'persistent': 'true'})
    else:
        client.send(args.d[0], msg)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', nargs=1, required=True, help='destination', metavar='dest')
    parser.add_argument('-e', action='store_true', default=False, help='persistent msgs flag')
    parser.add_argument('-m', nargs=1, required=True, help='msg', metavar='msg')
    parser.add_argument('-n', nargs=1, default=False, help='number of msgs', metavar='int')
    parser.add_argument('-p', nargs=1, default=False, help='msg prefix', metavar='msg prefix')
    parser.add_argument('-s', nargs=1, required=True, help='broker', metavar='broker')
    parser.add_argument('-t', nargs=1, default=False, help='send msg every sec', metavar='float')
    args = parser.parse_args()

    broker = 'tcp://%s:6163' % (args.s[0])
    config = StompConfig(broker)
    client = Stomp(config)
    client.connect()

    rd = ''.join(random.sample('abcdefghijklmno', 2))

    try:
        i = 0
        if args.n:
            while i < int(args.n[0]):
                send_msg(args, client, i, rd)
                i += 1
        else:
            while True:
                send_msg(args, client, i, rd)
                i += 1
    except KeyboardInterrupt:
        client.disconnect()
        raise SystemExit(1)

main()
