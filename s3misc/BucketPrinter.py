
import sys
import fnmatch
import boto3
from typing import List
import datetime

from s3misc.auth import AuthInfo

bucket_units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']

def foursigfloat(num: int, units: List[str]):
    """ Convert a number to four significant digits, given base-2 set of units.
        Breaks down at 1PB.
    """
    # Presumably three ifs are faster than one logarithm
    if (num >= (1 << 20)):
        if (num >= (1 << 30)):
            if (num >= (1 << 40)):
                if (num > (1 << 50)):
                    # PB: TB with no decimal, more than three whole numbers.
                    return ('{:.0f}'.format(num / (1 << 40)) + " " + units[4])
                else:
                    # TB with at least one decimal.
                    numstring = numstring[0:max(numstring.find('.'),  + 2)]
                return (('{:.0f}'.format(num / (1 << 40)))[0:5] + " " + units[4])
            else: # < 1TB
                return (('{:1.3f}'.format(num / (1 << 30)))[0:5] + " " + units[3])
        else: # < 1GB
            return (('{:1.3f}'.format(num / (1 << 20)))[0:5] + " " + units[2])
    else: # < 1MB
        if (num >= (1 << 10)):
            return (('{:1.3f}'.format(num / (1 << 10)))[0:5] + " " + units[1])
        else:
            return ((str(num))[0:5] + " " + units[0])

def DirectoryAccounting(dirstats: dict, curdir: str, delim: str, stats: List[int]):
    """ Subtotal accounting: for each item in the path, add this directory's stats to
    its parent.
    """
                
    if (curdir not in dirstats):
        # First time seeing this directory
        dirstats[curdir] = stats

    parentstats = None
    while (curdir != '' and curdir is not None):
        parentidx = curdir.rfind(delim, 0, len(curdir) - 1) + 1
        if (parentidx > 0):
            curdir = curdir[0:parentidx]
        else:
            curdir = ''

        if (curdir not in dirstats):
            # If parent dir contained no other files/directories (or they occurr after curdir)
            dirstats[curdir] = [0,0,0,0]
        parentstats = dirstats[curdir]

        parentstats[2] = parentstats[2] + stats[0]
        parentstats[3] = parentstats[3] + stats[1]


def WrapUpDirectory(dirstats: dict, prevdir: str, delim: str, nextdir: str):
    """ When you've encountered an item that is no longer within the current directory,
        we need to wrap up the current directory, print its subtotals, and check to
        see if we need to do likewise with the previous directory's parent, and parent's
        parent as well.
        """

    # Is this a subdir of the previous directory? or should we subtotals the directory?
    while (prevdir != nextdir[0:len(prevdir)]):
        prevdirstats = dirstats[prevdir]
        print(prevdir + " totals:")
        print("{} directory objects; {} directory size".format(
                                prevdirstats[0],
                                foursigfloat(prevdirstats[1], bucket_units)))

        # If there were sub directories, print out subtotals
        if (prevdirstats[2] > 0):
            print("    {} total subdirectory objects; {} total subdirectory size".format(
                                prevdirstats[2],
                                foursigfloat(prevdirstats[3], bucket_units)))

        # keep our active directory count down.
        if (prevdir != ''):
            del dirstats[prevdir]
        else:
            break

        # Next-previous, if the next->item is not within the previous dir's parent.
        idx = prevdir.rfind(delim, 0, len(prevdir) - 1)
        if (idx >= 0):
            prevdir = prevdir[0:idx + 1]
        else:
            prevdir = ''
        


class BucketPrinter:
    # Authentication for the bucket. Should come from ~/.aws/credentials, or from
    # command line.
    _auth = None

    # One client for the bucket printer, configured by __init__.
    _s3client = None

    # The actual wildcard matching data. The whole path-to-file given by the user.
    _match = None

    # Match info - a caching mechanism.
    # Call PrintBucket, calls GetMatchPrefix, which sets _matchinfo. Used by KeyMatch.
    _matchinfo = None

    # Only for listing directories
    _recursive = True

    # If supplied via command line
    _auth = None

    # Path delimiter, if any
    _delim = None


    def __init__(self, authinfo: AuthInfo):
        """ Authinfo may be none. If it is, we'll try parsing it from ~/.aws/credentials. """
        if (authinfo is not None):
            self._auth = authinfo

        self.InitClient()


    def InitClient(self):
        params = dict()
        if (self._auth is not None):
            params['aws_access_key_id'] = self._auth.access_key
            params['aws_secret_access_key'] = self._auth.secret_key

        print("Setting self._s3client")
        self._s3client = boto3.client('s3', **params)

    def SetAuthInfo(self, authinfo: AuthInfo):
        # Allows you to change auth info later.
        self._auth = authinfo
        self.InitClient()

    def PrintBucket(self, bucket: str, delim: str, match: str, recursive = True):
        """
        Parse and print a bucket. See ParseBucket.
        """

        self._delim = delim
        self._match = match
        self._bucket = bucket

        print("Printing bucket: {}, delim: {}, match: {}".format(bucket, delim, match))

        # reset this every listing
        if (recursive):
            self._recursive = True
        else:
            self._recursive = False

        # Statistics: items in this dir (directly), size of this dir (directly),
        #    items in this dir and subdirs, size in this dir and subdirs
        self.PrintItems(self.ParseBucket(bucket))

        # Done. Summarize total.
        #print("{} directory objects; {} directory size\n".format(dir_items, foursigfloat(dir_size)))
        #print("{} total objects; {} total size\n".format(dir_items, foursigfloat(dir_size)))

    def PrintItems(self, items):
        dirstats = dict()

        prevdir = ''
        prevlen = -1
        nextlen = 0
        nextdir = None
        stats = [0, 0, 0, 0]

        for item in items:
            boolnew = False
            nextlen = item['Key'].rfind(self._delim) + 1
            nextdir = item['Key'][0:nextlen]

            if (nextdir[0:prevlen] != prevdir):
                # Then we have just exited a directory.

                # Subtotals now include the just-completed directory
                DirectoryAccounting(dirstats, prevdir, self._delim, stats)

                # Print out subtotals.
                WrapUpDirectory(dirstats, prevdir, self._delim, nextdir)

                # this is a new directory. Create fresh stats.
                if (nextdir not in dirstats):
                    dirstats[nextdir] = [0, 0, 0, 0]
                stats = dirstats[nextdir]

                boolnew = True
            elif (nextlen > prevlen):
                # Then we have a new subdirectory.
                dirstats[nextdir] = [0, 0, 0, 0]
                stats = dirstats[nextdir]
                boolnew = True

            if (boolnew == True):
                boolnew = False
                prevdir = nextdir
                prevlen = nextlen
                # Header for the new directory
                print()
                print(item['Key'][0:nextlen] + ":")

            stats[0] = stats[0] + 1
            stats[1] = stats[1] + item['Size']

            keyname = item['Key'][prevlen:]
            if (len(keyname) == 0):
                keyname = "<directory object>"
            # size.1 GB 2020-05-22: MyEntry.txt
            print(foursigfloat(item['Size'], bucket_units) + " " + str(item['LastModified']) + ": " + keyname)

        # We've exhausted all directories.

        # Finish accounting for all of the last directories
        finaldir = prevdir
        while (prevdir != ''):
            DirectoryAccounting(dirstats, prevdir, self._delim, stats)
            idx = prevdir.rfind(self._delim, 0, -1) + 1
            prevdir = prevdir[0:idx]

        WrapUpDirectory(dirstats, finaldir, self._delim, '')
        
        print("\nBucket totals:")
        if ('' in dirstats):
            # If we're listing the whole bucket..
            stats = dirstats['']
            print("{} directory objects; {} directory size".format(
                                    stats[0],
                                    foursigfloat(stats[1], bucket_units)))

            # If there were sub directories, print out subtotals
            if (stats[2] > 0):
                print("    {} total subdirectory objects; {} total subdirectory size".format(
                                    stats[2],
                                    foursigfloat(stats[3], bucket_units)))


    def ParseBucket(self, bucket: str):
        """ Get and process a list of objects from a bucket.

        `match` may be None. If specified, any portion before the first wildcard is the prefix,
        and wildcards will not match delimiters.

        e.g.:
            match: my/files/myf*
            matches:
            - my/files/myfiles
            - my/files/myfamily
            no match:
            - my/files/myfamily/tree
            - my/files/myfunny/stuff

            match: 
        """

        params = {
                'Bucket': bucket,
                'MaxKeys': 1000 # server-side limit: 1000
        }


        # Prefix is the non-wild patch of the matching clause
        prefix = self.BucketMatch(self._match)
        if (prefix is not None):
            params['Prefix'] = prefix
            
        # For hints, we're going to use pagination.
        paginator = self._s3client.get_paginator('list_objects_v2')
        paginator = paginator.paginate(**params)

        for page in paginator:
            for item in page['Contents']:
                if (not self.KeyMatch(item['Key'])):
                    # Don't return non-matching objects
                    continue

                yield item

    def BucketMatch(self, match):
        """ 
            Configure the bucket matching parameter.
            Will return the prefix portion of the match (the part before any wildcards),
            and store various match info for the KeyMatch method.
        """

        if (match is None):
            self._matchinfo = None
            return None

        # Some processing required. Cache it.
        matchprefixlen = -1
        matchprefix = None
        matchwild = None
        if (match):
            for wildmatcher in ('*', '?', '['):
                idx = match.find(wildmatcher)
                if (idx > -1 and (matchprefixlen < 0 or idx < matchprefixlen)):
                    matchprefix = match[0:idx]
                    matchwild = match[idx:]

        if (matchprefix is None):
            # No wild characters, then the whole thing will be a prefix.
            matchprefix = match
            matchwild = ''

        self._matchinfo = [ len(matchprefix), matchwild ]

        return matchprefix


    def KeyMatch(self, key:str):
        """ Given a key, does it match the given match?

            e.g.:
            /buc/ket/stuff, /buc/ket/stuff* -> matches
            /buc/ket/stuff/1, /buc/ket/stuff* -> does not match
            /buc/ket/stuffy, /buc/ket/stuff* -> matches
            /buc/ket/stuft, /buc/ket/stuf? -> matches
            /buc/ket/stuff/, /buc/ket/stuff -> matches (if recursive)
            /buc/ket/stuff/subfile, /buc/ket/stuff -> matches (if recursive)
            /buc/ket/stuff/subfile, /buc/ket/stuff/ -> matches (if recursive)
        """

        # If not using a match condition, then everything matches.
        if (self._matchinfo is None):
            return True

        # matchinfo[prefix-length, wild-part]
        matchinfo = self._matchinfo

        # Does the prefix match? Yes, of course it does, by definition...
        matchmaybe = key[matchinfo[0]:]

        # easy condition: if we didn't include a wildcard, and the prefix is followed by (or
        # ends with) a delimeter, then we match if there is no other delimeter
        if (len(matchinfo[1]) == 0):
            if (len(matchmaybe) == 0):
                # the match was a prefix, and this key matches that prefix exactly.
                return True
            elif (matchmaybe[0] == self._delim and (self._recursive or
                        len(matchmaybe) > 0 and matchmaybe.find(self._delim, 1) > 0)):
                # matchprefix wasn't a directory but we found it to be one
                return True
            elif (matchinfo[0] > 0 and key[matchinfo[0] - 1] == self._delim and
                        (self._recursive or len(matchmaybe) > 0 and matchmaybe.find(self._delim, 1) > 0)):
                # matchprefix is a directory and this is a file under it
                return True
            else:
                # This is /path/to/myfileEXTRA
                # and I only want /path/to/myfile
                return False

        # Is this object a subdirectory? We don't match any part of subdirectories.
        if (matchmaybe.find(self._delim) >= 0):
            return False

        # Lastly, does this object actually match the wild portion?
        return fnmatch.fnmatch(matchmaybe, matchinfo[1])


    def Test(self):
        # Set up a few match texts. Only one can be used per bucket listing.

        return True

        self._delim = '/'

        dirstats = dict( {
                '/test/ing/123/': [0,0,0,0],
                '/test/ing/456/': [0,0,0,0],
                '/test/ing/': [0,0,0,0],
                '/test/': [0,0,0,0],
                '/': [0,0,0,0]
            })
        stats = dirstats['/test/ing/123/']
        stats[0] = stats[0] + 1
        stats[1] = stats[1] + 1
        DirectoryAccounting(dirstats, '/test/ing/123/', '/', stats)
        stats = dirstats['/test/ing/456/']
        stats[0] = stats[0] + 2
        stats[1] = stats[1] + 3
        DirectoryAccounting(dirstats, '/test/ing/456/', '/', stats)
        stats = dirstats['/test/ing/']
        stats[0] = stats[0] + 1
        stats[1] = stats[1] + 3
        DirectoryAccounting(dirstats, '/test/ing/', '/', stats)
        stats = dirstats['/test/']
        stats[0] = stats[0] + 1
        stats[1] = stats[1] + 1
        DirectoryAccounting(dirstats, '/test/', '/', stats)
        stats = dirstats['/']
        stats[0] = stats[0] + 1
        stats[1] = stats[1] + 1
        DirectoryAccounting(dirstats, '/', '/', stats)
        assert(dirstats['/test/ing/'] == list((1,3,3,4)))
        assert(dirstats['/'] == list((1,1,5,8)))
        dirstats = dict({
                 'test/ing/123/': [0,0,0,0],
                 'test/ing/456/': [0,0,0,0],
                 'test/ing/': [0,0,0,0],
                 'test/': [0,0,0,0]
            })
        stats = dirstats['test/ing/123/']
        stats[0] = stats[0] + 1
        stats[1] = stats[1] + 1
        DirectoryAccounting(dirstats, 'test/ing/123/', '/', stats)
        stats = dirstats['test/ing/456/']
        stats[0] = stats[0] + 2
        stats[1] = stats[1] + 3
        DirectoryAccounting(dirstats, 'test/ing/456/', '/', stats)
        stats = dirstats['test/ing/']
        stats[0] = stats[0] + 1
        stats[1] = stats[1] + 3
        DirectoryAccounting(dirstats, 'test/ing/', '/', stats)
        stats = dirstats['test/']
        stats[0] = stats[0] + 1
        stats[1] = stats[1] + 1
        DirectoryAccounting(dirstats, 'test/', '/', stats)
        assert(dirstats['test/ing/'] == list((1,3,3,4)))
        assert(dirstats[''] == list((0,0,5,8)))
        dirstats = dict()
        DirectoryAccounting(dirstats, 'test/', '/', [1,1,0,0]) 

        assert(self.BucketMatch('/test/ing/123') == '/test/ing/123')
        assert(self.BucketMatch('/test/ing/123?') == '/test/ing/123')
        assert(self.BucketMatch('/test/ing/*') == '/test/ing/')

        self.BucketMatch('IMG_4700/')
        assert(self.KeyMatch('IMG_4700/IMG_4783.jpg'))
        # This will pass because there's no wild. The bucket listing will include `IMG_4700`
        # as the prefix, and that's an assumption in the matcher: it doesn't consider the prefix.
        assert(self.KeyMatch('IMG_4800/IMG_4783.jpg'))
        self.BucketMatch('IMG_4700/*83*')
        assert(self.KeyMatch('IMG_4700/IMG_4783.jpg'))
        assert(not self.KeyMatch('IMG_4800/IMG_4733.jpg'))
        

        self.BucketMatch('/buc/ket/stuff*')
        assert(self.KeyMatch('/buc/ket/stuff'))
        self.BucketMatch('/buc/ket/stuff*')
        assert(not self.KeyMatch('/buc/ket/stuff/1'))
        self.BucketMatch('/buc/ket/stuff*')
        assert(self.KeyMatch('/buc/ket/stuffy'))
        self.BucketMatch('/buc/ket/stuf?')
        assert(self.KeyMatch('/buc/ket/stuft'))

        self._recursive = False
        self.BucketMatch('/buc/ket/stuff')
        assert(not self.KeyMatch('/buc/ket/stuff/'))
        self.BucketMatch('/buc/ket/stuff')
        assert(not self.KeyMatch('/buc/ket/stuff/subfile'))
        self.BucketMatch('/buc/ket/stuff/')
        assert(not self.KeyMatch('/buc/ket/stuff/subfile'))

        self._recursive = True
        self.BucketMatch('/buc/ket/stuff')
        assert(self.KeyMatch('/buc/ket/stuff/')) # -> matches (if recursive)
        self.BucketMatch('/buc/ket/stuff')
        assert(self.KeyMatch('/buc/ket/stuff/subfile')) # -> matches (if recursive)
        self.BucketMatch('/buc/ket/stuff/')
        assert(self.KeyMatch('/buc/ket/stuff/subfile'))#  -> matches (if recursive)

        foursigfloat(789236482, bucket_units)
        self._delim = '/'

        def genlist():
            myitem = dict({
                 'Key': 'img_' + str(3) + '/img_' + str(5) + '.jpg',
                 'Size': 78364876,
                 'LastModified': datetime.datetime.today()
            })
            #yield myitem
            #return
            
            for i in (1,2, 3, 4,):
                myitem = dict({
                        'Key': 'img_' + str(i) + '/',
                        'Size': 0,
                        'LastModified': datetime.datetime.today()
                    })
                yield myitem
                for j in (range(1,5)):
                    myitem = dict({
                            'Key': 'img_' + str(i) + '/img_' + str(j) + '.jpg',
                        'Size': 78364876,
                        'LastModified': datetime.datetime.today()
                    })
                    yield myitem

        self.PrintItems(genlist())

        sys.exit(0)

