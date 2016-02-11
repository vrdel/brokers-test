#!/usr/bin/python

from stompest.config import StompConfig
from stompest.sync import Stomp
import sys, random, time
import argparse

def send_msg(args, client, msg):
    if args.t:
        time.sleep(float(args.t[0]))
    else:
        time.sleep(1)
    if args.e:
        client.send(args.d[0], msg, {'persistent': 'true'})
    else:
        client.send(args.d[0], msg)

def gen_msg(prefix):
    yearl = ['2014', '2015', '2016']
    statusl = ['OK', 'WARNING', 'MISSING', 'CRITICAL', 'UKNOWN', 'DOWNTIME']
    monthl = ['%s' % '{:=02}'.format(i) for i in range(1,12)]
    # monthl = ["%s" % "{:=02}".format(i) for i < 31 i += 1]
    daysl = ['%s' % '{:=02}'.format(i) for i in range(1,31)]
    secl = ['%s' % '{:=02}'.format(i) for i in range(1,60)]
    minutl = ['%s' % '{:=02}'.format(i) for i in range(1,60)]
    hourl = ['%s' % '{:=02}'.format(i) for i in range(1,12)]

    service = 'serviceType: %s\n' % ''.join(random.sample('abcdefghijklmno', 10))
    timestamp = 'timestamp: %s-%s-%sT%s:%s:%sZ\n' % (random.choice(yearl), random.choice(monthl), random.choice(daysl),
                                                     random.choice(hourl), random.choice(minutl), random.choice(secl),)
    hostname = 'hostName: %s\n' % ''.join(random.sample('abcdefghijklmno', 10))
    metric = 'metricName: %s\n' % ''.join(random.sample('abcdefghijklmno', 10))
    status = 'metricStatus: %s\n' % random.choice(statusl)
    nagiosh = 'nagios_host: %s\n' % ''.join(random.sample('abcdefghijklmno', 10))
    summaryd = 'summaryData: %s\n' % ''.join(random.sample('abcdefghijklmno', 10))
    if prefix:
        msgd = 'detailsData: %s-%s\n' % (prefix, ''.join(random.sample('abcdefghijklmno', 10)))
    else:
        msgd = 'detailsData: %s\n' % ''.join(random.sample('abcdefghijklmno', 10))

    msg = service + hostname + timestamp + metric + status + nagiosh + summaryd + msgd + 'EOT\n'
    return msg

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', nargs=1, required=True, help='destination', metavar='dest')
    parser.add_argument('-e', action='store_true', default=False, help='persistent msgs flag')
    parser.add_argument('-m', nargs=1, required=True, help='msg', metavar='msg')
    parser.add_argument('-n', nargs=1, default=False, help='number of msgs', metavar='int')
    parser.add_argument('-p', nargs=1, default=False, help='msg prefix', metavar='msg prefix')
    parser.add_argument('-s', nargs=1, required=True, help='broker', metavar='broker')
    parser.add_argument('-t', nargs=1, default=False, help='send msg every sec', metavar='float')
    parser.add_argument('-v', action='store_true', default=False, help='verbose')
    args = parser.parse_args()

    broker = 'tcp://%s:6163' % (args.s[0])
    config = StompConfig(broker)
    client = Stomp(config)
    client.connect()

    try:
        i = 0
        if args.n:
            while i < int(args.n[0]):
                msg = gen_msg(args.p[0] if args.p else None)
                if args.v:
                    print str(i) + '\n' + '------------------'
                    print msg
                send_msg(args, client, msg)
                i += 1
        else:
            while True:
                msg = gen_msg(args.p[0] if args.p else None)
                if args.v:
                    print str(i) + '\n' + '------------------'
                    print msg
                send_msg(args, client, msg)
                i += 1
    except KeyboardInterrupt:
        client.disconnect()
        raise SystemExit(1)

main()
