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

def gen_msg(args):
    randsample = 'abcdefghijklmno'
    yearl = ['2014', '2015', '2016']
    statusl = ['OK', 'WARNING', 'MISSING', 'CRITICAL', 'UKNOWN', 'DOWNTIME']
    monthl = ['%s' % '{:=02}'.format(i) for i in range(1,12)]
    daysl = ['%s' % '{:=02}'.format(i) for i in range(1,31)]
    secl = ['%s' % '{:=02}'.format(i) for i in range(1,60)]
    minutl = ['%s' % '{:=02}'.format(i) for i in range(1,60)]
    hourl = ['%s' % '{:=02}'.format(i) for i in range(1,12)]

    i, details, = 0, ''
    while i < args.z/len(randsample):
        details += ''.join(random.sample(randsample, len(randsample)))
        i += 1
    prefix = args.p[0] if args.p else None
    if prefix:
        msgd = 'detailsData: %s-%s\n' % (prefix, details)
    else:
        msgd = 'detailsData: %s\n' % details
    if args.a:
        timestamp = 'timestamp: %sT%s:%s:%sZ\n' % (args.a[0], random.choice(hourl), random.choice(minutl), random.choice(secl))
    else:
        timestamp = 'timestamp: %s-%s-%sT%s:%s:%sZ\n' % (random.choice(yearl), random.choice(monthl), random.choice(daysl),
                                                        random.choice(hourl), random.choice(minutl), random.choice(secl),)
    if args.i:
        service = 'serviceType: %s,%s\n' %  (''.join(random.sample(randsample, len(randsample))), ''.join(random.sample(randsample, len(randsample))))
    else:
        service = 'serviceType: %s\n' % ''.join(random.sample(randsample, len(randsample)))
    hostname = 'hostName: %s\n' % ''.join(random.sample(randsample, len(randsample)))
    metric = 'metricName: %s\n' % ''.join(random.sample(randsample, len(randsample)))
    status = 'metricStatus: %s\n' % random.choice(statusl)
    nagiosh = 'nagios_host: %s\n' % ''.join(random.sample(randsample, len(randsample)))
    summaryd = 'summaryData: %s\n' % ''.join(random.sample(randsample, len(randsample)))

    msg = ''
    if args.w:
        mandfields = [timestamp, service, hostname, metric, status]
        mandfields.remove(random.choice(mandfields))
        for i in mandfields:
            msg += i
        msg = msg + nagiosh + summaryd + msgd + 'EOT\n'
    else:
        msg = service + hostname + timestamp + metric + status + nagiosh + summaryd + msgd + 'EOT\n'
    return msg

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', nargs=1, required=True, help='destination', metavar='dest')
    parser.add_argument('-e', action='store_true', default=False, help='persistent msgs flag')
    parser.add_argument('-a', nargs=1, required=False, help='fixed timestamp', metavar='fixed timestamp')
    parser.add_argument('-n', nargs=1, default=False, help='number of msgs', metavar='int')
    parser.add_argument('-o', default=6163, type=int, help='port', metavar='port')
    parser.add_argument('-z', default=16, type=int, help='size of msg payload', metavar='msg prefix')
    parser.add_argument('-p', nargs=1, default=False, help='msg prefix', metavar='msg prefix')
    parser.add_argument('-s', nargs=1, required=True, help='broker', metavar='broker')
    parser.add_argument('-i', action='store_true', required=False, default=False, help='paired service type')
    parser.add_argument('-w', action='store_true', default=False, help='format message wrongly')
    parser.add_argument('-t', nargs=1, default=False, help='send msg every sec', metavar='float')
    parser.add_argument('-v', action='store_true', default=False, help='verbose')
    args = parser.parse_args()

    broker = 'tcp://%s:%i' % (args.s[0], args.o)
    config = StompConfig(broker)
    client = Stomp(config)
    client.connect()

    try:
        i = 0
        if args.n:
            while i < int(args.n[0]):
                msg = gen_msg(args)
                if args.v:
                    print str(i)
                    print '%.128s' % msg
                send_msg(args, client, msg)
                i += 1
        else:
            while True:
                msg = gen_msg(args)
                if args.v:
                    print str(i)
                    print '%.128s' % msg
                send_msg(args, client, msg)
                i += 1
    except KeyboardInterrupt:
        client.disconnect()
        raise SystemExit(1)

main()
