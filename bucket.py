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
    
    parser.add_argument('--delim', type=ArgParseChar, default='/',
                help='Delimiter for "directories" within object names.')
    parser.add_argument('--block-size', type=int, default=-1,
                help='Show size outputs in blocks of given size (default: auto)')
    parser.add_argument('--access-key-id', type=str,
                help='AWS access key. If not provided, will be taken from ~/.aws/credentials. If provided, you will be prompted for the secret access key.')
    parser.add_argument('--directory-totals', default=False, action="store_true",
                help='Provide totals at the end of directories, in addition to at the end of bucket listing.')
    parser.add_argument('bucket', type=str, nargs='*',
                help='Bucket(s) to list')

    args = parser.parse_args()

    auth = None
    if (args.access_key_id):
        auth_secret = input("AWS_SECRET_KEY (NOT INVISIBLE!!): ")

        auth = AuthInfo(access_key=args.access_key_id, secret_key=auth_secret)

    bucketprinter = s3misc.BucketPrinter.BucketPrinter(auth)
    if (args.block_size > 0):
        bucketprinter.SetBlockSize(args.block_size)
    if (args.directory_totals):
        bucketprinter.SetDirectoryTotals(True)

    bucketprinter.Test()
    for bucket in args.bucket:

        bucketinfo = bucket.split(':', 1)
        if (len(bucketinfo) == 1):
            bucketinfo = [bucketinfo[0], '']

        params = dict()
        delim = args.delim

        print("Printing bucket: " + bucketinfo[0])
        bucketprinter.PrintBucket(bucketinfo[0], delim, bucketinfo[1], **params)

if (__name__ == '__main__'):
    main()




