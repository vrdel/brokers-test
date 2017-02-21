#!/usr/bin/python

import time
import argparse

from argo_ams_library.ams import ArgoMessagingService
from argo_ams_library.amsmsg import AmsMessage
from argo_ams_library.amsexceptions import AmsHandleExceptions

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', required=True, type=str, help='hostname', metavar='ingestion')
    parser.add_argument('-k', type=str, required=True, help='token', metavar='int')
    parser.add_argument('-u', type=str, required=True, help='token', metavar='subscription')
    parser.add_argument('-n', type=int, default=1, required=False, help='consume num messages')
    args = parser.parse_args()

    ams = ArgoMessagingService(endpoint=args.s, token=args.k, project='EGI')

    sub = ams.get_sub(args.u)
    ids = list()
    if sub:
        try:
            for ack, msg in ams.pull_sub(args.u, args.n):
                data = msg.get_data()
                print data
                ids.append(ack)
            if ids:
                ams.ack_sub(args.u, ids)
        except AmsHandleExceptions as e:
            print e
main()
