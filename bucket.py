#!/usr/bin/env python3

import sys
import os
import argparse
#from typing import NamedTuple
from s3misc.auth import AuthInfo
import s3misc.BucketPrinter

from s3misc.argparse_types import ArgParseChar

def main():
    parser = argparse.ArgumentParser("List aws buckets.")
    
    parser.add_argument('--sort', choices=['time','size','name'],
                help='Sort by <what>.')
    parser.add_argument('-R', type=bool, dest='reverse',
                help='Sort in reverse.')
    parser.add_argument('-r', type=bool, dest='recursive',
                help='List bucket recursively.')
    parser.add_argument('--delim', type=ArgParseChar, default='/',
                help='Delimiter for "directories" within object names.')
    parser.add_argument('--access-key-id', type=str,
                help='AWS access key. If not provided, will be taken from ~/.aws/credentials. If provided, you will be prompted for the secret access key.')
    parser.add_argument('bucket', type=str, nargs='*',
                help='Bucket(s) to list')

    args = parser.parse_args()

    auth = None
    if (args.access_key_id):
        auth_secret = input("AWS_SECRET_KEY (NOT INVISIBLE!!): ")

        auth = AuthInfo(access_key=args.access_key_id, secret_key=auth_secret)

    bucketprinter = s3misc.BucketPrinter.BucketPrinter(auth)
    bucketprinter.Test()
    for bucket in args.bucket:

        print("Bucket!")
        bucketinfo = bucket.split(':')
        if (len(bucketinfo) == 1):
            bucketinfo = [bucketinfo[0], '']
        params = dict()
        delim = args.delim
        if (args.recursive):
            params['recursive'] = True

        print("Printing bucket: " + bucketinfo[0])
        bucketprinter.PrintBucket(bucketinfo[0], delim, bucketinfo[1], **params)

if (__name__ == '__main__'):
    main()




