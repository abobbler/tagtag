

# Get python3 pip if not already present
```
sudo apt-get install python3-pip
pip3 install boto3
```


# Running
Note that this script doesn't ask for many things, such as the region where
services are hosted.

You can use this script as:
```
python3 bucket.py your-bucket-name
python3 bucket.py your-bucket-name:/path/to/data/
```
and maybe (untested):
```
python3 bucket.py your-bucket-name:/path/to/files/*.py
```

## Authentication
Authentication will be handled by the AWS SDK. You can configure your system
with `aws configure`. You may specify the config file with the environment
variable `AWS_CONFIG_FILE`, `AWS_SHARED_CREDENTIALS_FILE`, etc. You might
specify a testing environment configuration with `AWS_PROFILE` that
corresponds to an entry within the credentials file.

You may also specify the AWS access key on the command line. You will be
prompted for the secret key, which will be shown on the screen (but hey the
combination won't be present in your bash_history).

# Examples

Upload some images and some zero-length files to AWS,


```
$ python3 bucket.py myparty
...
708.2 KB 2020-10-17 18:58:51+00:00: IMG_4682.jpg
718.2 KB 2020-10-17 18:58:51+00:00: IMG_4691.jpg
627.4 KB 2020-10-17 18:59:01+00:00: IMG_4698.jpg
IMG_/ totals:
207 directory objects; 137.1 MB directory size
    96 total subdirectory objects; 64.00 MB total subdirectory size
New; nextlen: 9; prevdir: IMG_4700/

IMG_4700/:
0 B 2020-10-17 18:51:23+00:00: <directory object>
808.0 KB 2020-10-17 18:59:30+00:00: IMG_4705.jpg
677.1 KB 2020-10-17 18:59:30+00:00: IMG_4707.jpg
456.1 KB 2020-10-17 18:59:30+00:00: IMG_4711.jpg
838.3 KB 2020-10-17 18:59:31+00:00: IMG_4715.jpg
...
592.4 KB 2020-10-17 18:59:35+00:00: IMG_4751.jpg
IMG_4700/ totals:
19 directory objects; 9.919 MB directory size
New; nextlen: 5; prevdir: zero/

zero/:
0 B 2020-10-18 03:36:25+00:00: <directory object>
0 B 2020-10-18 03:36:48+00:00: 1
0 B 2020-10-18 03:36:48+00:00: 10
0 B 2020-10-18 03:37:11+00:00: 100
0 B 2020-10-18 03:51:55+00:00: 1000
0 B 2020-10-18 03:37:11+00:00: 101
...
zero/ totals:
1001 directory objects; 0 B directory size

Bucket totals:
0 directory objects; 0 B directory size
    1323 total subdirectory objects; 211.0 MB total subdirectory size
```

Greater than 1000 objects listed.

Change the delimiter,

```
$ python3 bucket.py  --delim=4 myparty
...

IMG_4700/IMG_4744:
264.2 KB 2020-10-17 18:59:33+00:00: .jpg
IMG_4700/IMG_4744 totals:
1 directory objects; 264.2 KB directory size

IMG_4700/IMG_474:
220.2 KB 2020-10-17 18:59:34+00:00: 5.jpg
665.7 KB 2020-10-17 18:59:35+00:00: 8.jpg
737.7 KB 2020-10-17 18:59:35+00:00: 9.jpg
IMG_4700/IMG_474 totals:
4 directory objects; 2.068 MB directory size
    1 total subdirectory objects; 264.2 KB total subdirectory size

IMG_4700/IMG_4:
592.4 KB 2020-10-17 18:59:35+00:00: 751.jpg
IMG_4700/IMG_4 totals:
12 directory objects; 7.087 MB directory size
    6 total subdirectory objects; 2.831 MB total subdirectory size
IMG_4 totals:
1 directory objects; 0 B directory size
    18 total subdirectory objects; 9.919 MB total subdirectory size

Bucket totals:
0 directory objects; 0 B directory size
    30 total subdirectory objects; 17.00 MB total subdirectory size
```
listed directories,
- IMG_4700/IMG_4744
- IMG_4700/IMG_474
- IMG_4700/IMG_4
- IMG_4


Some prefix work is in place (not well tested!)
```
$ python3 bucket.py myparty:IMG_4700/
AWS_SECRET_KEY (NOT INVISIBLE!!): Setting self._s3client
Printing bucket: myparty
Printing bucket: myparty, delim: /, match: IMG_4700

IMG_4700/:
0 B 2020-10-17 18:51:23+00:00: <directory object>
808.0 KB 2020-10-17 18:59:30+00:00: IMG_4705.jpg
...
592.4 KB 2020-10-17 18:59:35+00:00: IMG_4751.jpg
IMG_4700/ totals:
19 directory objects; 9.919 MB directory size

Bucket totals:
0 directory objects; 0 B directory size
    19 total subdirectory objects; 9.919 MB total subdirectory size
```

NOT TESTED:
wildcard matches. `python3 bucket.py 'myparty:IMG_4700/IM*'`

