#!/usr/bin/python

from stompest.config import StompConfig
from stompest.sync import Stomp

import argparse
import avro.schema
import datetime
import json
import random
import sys
import time
import requests

from avro.io import BinaryEncoder
from avro.io import DatumReader
from avro.io import DatumWriter
from io import BytesIO
from os import path
from base64 import b64encode


def send_msg(sleep, ingapi, msg):
    if sleep:
        time.sleep(float(sleep))
    else:
        time.sleep(1)

    try:
        response = requests.post(ingapi,
                                 data=msg,
                                 verify=False)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print 'Server returned error:'
        if isinstance(e, requests.exceptions.HTTPError):
            print repr(e) + ' - ' + str(response.json())
        else:
            print repr(e)
        raise SystemExit(1)

    return response

def construct_msg(args):
    def gen_msg(args):
        msg = {}
        randsample = 'abcdefghijklmno'
        yearl = ['2014', '2015', '2016']
        statusl = ['OK', 'WARNING', 'MISSING', 'CRITICAL', 'UNKNOWN', 'DOWNTIME']
        monthl = ['%s' % '{:=02}'.format(i) for i in range(1,12)]
        daysl = ['%s' % '{:=02}'.format(i) for i in range(1,31)]
        secl = ['%s' % '{:=02}'.format(i) for i in range(1,60)]
        minutl = ['%s' % '{:=02}'.format(i) for i in range(1,60)]
        hourl = ['%s' % '{:=02}'.format(i) for i in range(1,12)]

        i, details, = 0, ''
        while i < (args.z * 0.74988)/len(randsample):
            details += ''.join(random.sample(randsample, len(randsample)))
            i += 1
        prefix = args.p if args.p else None
        if prefix:
            msgd = '%s-%s' % (prefix, details)
        else:
            msgd = details
        msg['message'] = details

        if args.a:
            timestamp = '%sT%s:%s:%sZ' % (args.a, random.choice(hourl), random.choice(minutl), random.choice(secl))
        else:
            timestamp = '%s-%s-%sT%s:%s:%sZ' % (random.choice(yearl), random.choice(monthl), random.choice(daysl),
                                                            random.choice(hourl), random.choice(minutl), random.choice(secl),)
        msg['timestamp'] = timestamp

        service = ''.join(random.sample(randsample, len(randsample)))
        msg['service'] = service

        hostname = ''.join(random.sample(randsample, len(randsample)))
        msg['hostname'] = hostname

        metric = ''.join(random.sample(randsample, len(randsample)))
        msg['metric'] = metric

        status = random.choice(statusl)
        msg['status'] = status

        nagiosh = ''.join(random.sample(randsample, len(randsample)))
        msg['monitoring_host'] = nagiosh

        summaryd = ''.join(random.sample(randsample, len(randsample)))
        msg['summary'] = summaryd

        if args.w:
            mandfields = ['timestamp', 'service', 'hostname', 'metric', 'status']
            msg.pop(random.choice(mandfields))

        return msg

    def b64enc(args, msg):
        try:
            schema = open(args.c)
            avro_writer = DatumWriter(avro.schema.parse(schema.read()))
            bytesio = BytesIO()
            encoder = BinaryEncoder(bytesio)
            avro_writer.write(msg, encoder)
            raw_bytes = bytesio.getvalue()

            return b64encode(raw_bytes)

        except (IOError, OSError) as e:
            print e
            raise SystemExit(1)
        finally:
            schema.close()

    msg = b64enc(args, gen_msg(args))
    size = sys.getsizeof(msg)
    ingest_msg = {"messages": [{"attributes": {"type": "metric_data",
                                               "partition_date": datetime.datetime.now().strftime('%Y-%m-%d')},
                                "data": msg}]}

    return size, json.dumps(ingest_msg)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', default=False, type=str, required=False, help='fixed timestamp', metavar='fixed timestamp')
    parser.add_argument('-c', type=str, required=True, help='avro schema')
    parser.add_argument('-k', type=str, required=True, help='token', metavar='int')
    parser.add_argument('-n', default=False, type=int, help='number of msgs', metavar='int')
    parser.add_argument('-p', type=str, default=False, help='msg prefix', metavar='msg prefix')
    parser.add_argument('-s', required=True, type=str, help='ingestion hostname', metavar='ingestion')
    parser.add_argument('-t', default=False, type=float, help='send msg every sec')
    parser.add_argument('-v', action='store_true', default=False, help='verbose')
    parser.add_argument('-w', action='store_true', default=False, help='format message wrongly')
    parser.add_argument('-z', default=512, type=int, help='size of msg payload', metavar='float')
    args = parser.parse_args()

    ingapi = 'https://%s/v1/projects/EGI/topics/metric_data:publish?key=%s' % (args.s, args.k)

    try:
        i = 0
        if args.n:
            while i < int(args.n):
                size, msg = construct_msg(args)
                if args.v:
                    print 'Message: %d' % i
                    print 'Message payload: %d bytes' % size
                    print 'Message payload (trimmed to 256 char):'
                    print '%.256s' % format(msg)
                ret = send_msg(args.t, ingapi, msg)
                if args.v:
                    print 'Server return: %s\n' % ret
                i += 1
        else:
            while True:
                size, msg = construct_msg(args)
                if args.v:
                    print 'Message: %d' % i
                    print 'Message payload: %d bytes' % size
                    print 'Message payload (trimmed to 256 char):'
                    print '%.256s' % format(msg)
                ret = send_msg(args.t, ingapi, msg)
                if args.v:
                    print 'Server return: %s\n' % ret
                i += 1
    except KeyboardInterrupt:
        raise SystemExit(1)

main()
