
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
        dirstats[curdir] = stats
    else:
        # A subdir occurred first. Sub in these direct-adds.
        curdirstats = dirstats[curdir]
        curdirstats[0] = curdirstats[0] + stats[0]
        curdirstats[1] = curdirstats[1] + stats[1]

    parentstats = None
    while (curdir != '') and curdir is not None:
        parentidx = curdir.rfind(delim, 0, len(curdir) - 1) + 1
        if (parentidx > 0):
            curdir = curdir[0:parentidx]
        else:
            curdir = ''

        # If parent dir contained no other files/directories, or they occurred after curdir
        if (curdir not in dirstats):
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
        print("{} directory objects; {} directory size\n".format(
                                prevdirstats[0],
                                foursigfloat(prevdirstats[1], bucket_units)))

        # If there were sub directories, print out subtotals
        if (prevdirstats[2] > 0):
            print("    {} total subdirectory objects; {} total subdirectory size".format(
                                prevdirstats[2],
                                foursigfloat(prevdirstats[3], bucket_units)))
    
        # keep our active directory count down.
        del dirstats[prevdir]

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
    _recursive = False

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

    def PrintBucket(self, bucket: str, delim: str, match: str, recursive = False):
        """
        Parse and print a bucket. See ParseBucket.
        """

        self._delim = delim
        self._match = match

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
                dirstats[nextdir] = [0, 0, 0, 0]
                stats = dirstats[nextdir]

                boolNew = True
            elif (nextlen > prevlen):
                # Then we have a new subdirectory.
                dirstats[nextdir] = [0,0,0,0]
                stats = dirstats[nextdir]
                boolNew = True

            if (boolNew):
                prevdir = nextdir
                prevlen = nextlen
                # Header for the new directory
                print(nextdir + ":")

            keyname = item['Key'][prevlen:]
            # size.1 GB 2020-05-22: MyEntry.txt
            print(foursigfloat(item['Size'], bucket_units) + " " + str(item['LastModified']) + ": " + keyname)

        # We've exhausted all directories.
        WrapUpDirectory(dirstats, prevdir, self._delim, '')

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
            print("Got page!")
            for item in page['Contents']:
                print("Got item! key: " + item['Key'])
                if (self.KeyMatch(item['Key'])):
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
            if (matchmaybe[0] == self._delim and (self._recursive or matchmaybe.find(self._delim, 1) > 0)):
                # matchprefix wasn't a directory but we found it to be one
                return True
            elif (matchinfo[0] > 0 and key[matchinfo[0] - 1] == self._delim and
                        (self._recursive or matchmaybe.find(self._delim, 1) > 0)):
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

        self._delim = '/'

        dirstats = dict()
        DirectoryAccounting(dirstats, '/test/ing/123/', '/', [1,1,0,0]) 
        DirectoryAccounting(dirstats, '/test/ing/456/', '/', [2,3,0,0])
        DirectoryAccounting(dirstats, '/test/ing/', '/', [1,3,0,0])
        DirectoryAccounting(dirstats, '/test/', '/', [1,1,0,0])
        DirectoryAccounting(dirstats, '/', '/', [1,1,0,0])
        assert(dirstats['/test/ing/'] == list((1,3,3,4)))
        assert(dirstats['/'] == list((1,1,5,8)))
        #print(str(dirstats))
        dirstats = dict()
        DirectoryAccounting(dirstats, 'test/ing/123/', '/', [1,1,0,0]) 
        DirectoryAccounting(dirstats, 'test/ing/456/', '/', [2,3,0,0])
        DirectoryAccounting(dirstats, 'test/ing/', '/', [1,3,0,0])
        DirectoryAccounting(dirstats, 'test/', '/', [1,1,0,0])
        print(str(dirstats))
        assert(dirstats['test/ing/'] == list((1,3,3,4)))
        assert(dirstats[''] == list((0,0,5,8)))
        dirstats = dict()
        DirectoryAccounting(dirstats, 'test/', '/', [1,1,0,0]) 

        assert(self.BucketMatch('/test/ing/123') == '/test/ing/123')
        assert(self.BucketMatch('/test/ing/123?') == '/test/ing/123')
        assert(self.BucketMatch('/test/ing/*') == '/test/ing/')

        self.BucketMatch('/buc/ket/stuff*')
        assert(self.KeyMatch('/buc/ket/stuff'))
        self.BucketMatch('/buc/ket/stuff*')
        assert(not self.KeyMatch('/buc/ket/stuff/1'))
        self.BucketMatch('/buc/ket/stuff*')
        assert(self.KeyMatch('/buc/ket/stuffy'))
        self.BucketMatch('/buc/ket/stuf?')
        assert(self.KeyMatch('/buc/ket/stuft'))

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

        def genlist():
            for i in (1,2, 3, 4,):
                myitem = dict({
                        'Key': 'img_' + str(i) + '/',
                        'Size': 0,
                        'LastModified': datetime.datetime.today()
                    })
                yield myitem
                for j in (range(1,800)):
                    myitem = dict({
                            'Key': 'img_' + str(j) + '.jpg',
                        'Size': 78364876,
                        'LastModified': datetime.datetime.today()
                    })
                    yield myitem

        self.PrintItems(genlist())

