#!/usr/bin/env python3

import sys
import os
import argparse
#from typing import NamedTuple
from s3types.auth import AuthInfo
from s3misc.PrintBucket import PrintBucket

from s3misc.argparse_types import ArgParseChar

def HooverAuth():
    # Straight from ~/.aws/credentials!

    if (os.path.isfile("~/.aws/credentials"):
        return None

    auth_access_key = None
    auth_secret_key = None
    with f = open("~/.aws/credentials"):
        while (lin = f.readline()):
            lin = l.split('=')

            if (lin[0] == 'aws_access_key_id'):
                auth_access_key = lin[1]
            elif (lin[0] == 'aws_secret_access_key'):
                auth_secret_key = lin[1]

    if (auth_access_key is not None and auth_secret_key is not None):
        return AuthInfo(ccess_key=auth_access_key, secret_key=auth_secret_key)
    return None

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

    if (args['access-key-id']):
        auth_secret = input("AWS_SECRET_KEY: ")

        auth = Auth(access_key=args['access-key-id'], secret_key=auth_secret)
    else:
        auth = HooverAuth()

    if (auth is None):
        print(":-( Unable to get access key to list the bucket. Trying with no authentication. This will fail.")

    #s3client = boto3.client('s3')

    for bucket in args.bucket:
        print(bucket)

if (__name__ == '__main__'):
    main()




