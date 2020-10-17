
import sys
import fnmatch
import boto3

from s3types.auth import AuthInfo


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

    while (curdir is not None):
        parentstats = dirstats[curdir]
        parentstats[2] = parentstats[2] + stats[0]
        parentstats[3] = parentstats[3] + stats[3]

        parentidx = curdir.rfind(delim) + 1
        if (parentidx > 0):
            curdir = curdir[0:parentidx]
        else:
            curdir = None

def WrapUpDirectory(dirstats: dict, prevdir: str, prevdirlen: int, nextdir: str, nextdirlen: int):
    """ When you've encountered an item that is no longer within the current directory,
        we need to wrap up the current directory, print its subtotals, and check to
        see if we need to do likewise with the previous directory's parent, and parent's
        parent as well.
        """
    # Is this a subdir of the previous directory? or should we subtotals the directory?
    while (prevdir != nextdir[0:prevlen]):
        prevdirstats = dirstats[prevdir]
        print(prevdir + " totals:")
        print("{} directory objects; {} directory size\n".format(
                                prevdirstats[0],
                                foursigfloat(prevdirstats[1])))

        # If there were sub directories, print out subtotals
        if (prevdirstats[2] > 0):
            print("    {} total subdirectory objects; {} total subdirectory size".format(
                                prevdirstats[2],
                                foursigfloat(prevdirstats[3])))

        # Next-previous, if the next->item is not within the previous dir's parent.
        prevdir = prevdir[0:prevdir.find(delim, 0, prevlen - 1) + 1]
        prevlen = len(prevdir)


class BucketPrinter:
    # Authentication for the bucket. Should come from ~/.aws/credentials, or from
    # command line.
    _auth = None

    # One client for the bucket printer, configured by __init__.
    _s3client = None

    # Match info - a caching mechanism.
    _matchinfo = dict()

    # Only for listing directories
    _recursive = False

    def __init__(self, authinfo: AuthInfo):
        """ Authinfo may be none. If it is, we'll try parsing it from ~/.aws/credentials. """
        if (authinfo is not None):
            self._auth = authinfo
        else:
            self._auth = HooverAuth()

        params = dict()
        if (authinfo is not None):
            params['aws_access_key_id'] = authinfo.access_key
            params['aws_secret_access_key'] = authinfo.secret_key
        self._client = boto3.client('s3', **params)


    def SetAuthInfo(self, authinfo: AuthInfo):
        # Allows you to change auth info later.
        self._auth = authinfo

    def PrintBucket(self, bucket: str, delim: str, match: str, recursive = False):
        """
        Parse and print a bucket. See ParseBucket.
        """

        # reset this every listing
        if (recursive):
            self._recursive = True
        else:
            self._recursive = False

        dirstats = dict()
        prevdir = ''
        prevlen = -1
        # Statistics: items in this dir (directly), size of this dir (directly),
        #    items in this dir and subdirs, size in this dir and subdirs
        stats = [0, 0, 0, 0]
        for item in ParseBucket(bucket, delim, match):
            # If this key isn't in the same dilimited-directory, print a new directory header
            if (item['Key'][0:prevlen] != prevdir or item['Key'].rfind(delim) + 1 != prevlen):

                # Subtotals now include the just-completed directory
                DirectoryAccounting(dirstats, prevdir, delim, stats)

                # Get the current directory for this item['key']
                nextlen = item['Key'].rfind(delim) + 1
                nextdir = item['Key'][0:nextlen]

                # Print out subtotals, if we're moving up.
                WrapUpDirectory(dirstats, prevdir, prevlen, nextdir, nextlen)

                # Start a new stats object
                stats = dirstats[nextdir]
                if (stats is None):
                    stats = [0, 0, 0, 0]
                    dirstats[nextdir] = stats

                # Header for the new directory
                print(nextdir + ":")

                prevdir = nextdir
                prevlen = nextlen

            keyname = item['Key'][prevlen:]
            # size.1 GB 2020-05-22: MyEntry.txt
            print(foursigfloat(item['Size']) + " " + str(item['LastModified']) + ": " + keyname)

        # Done. Summarize total.
        print("{} directory objects; {} directory size\n".format(dir_items, foursigfloat(dir_size)))
        print("{} total objects; {} total size\n".format(dir_items, foursigfloat(dir_size)))

    def ParseBucket(self, bucket: str, delim: str, match: str):
        """ Get and process a list of objects from a bucket.

        delim: if you want to use a delimiter. No default provided.

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
                'MaxKeys': sys.maxsize  # This bucket holds _how_ many items?
        }

        # Match-prefix-length is the length of `match` up to the first wild (exclusive)
        # and `match`, up to the first wild, becomes the prefix.

        prefix = GetMatchPrefix(match)

        if (prefix is not None):
            params['Prefix'] = prefix
            
        # For hints, we're going to use pagination.
        paginator = self._s3client.get_paginator('list_objects_v2')
        paginator = paginator.paginate(**params)

        for page in paginator:
            for item in page['Contents']:
                if (not KeyMatch(item['Key'], delim, match)):
                    # Don't return non-matching objects
                    continue

                yield item

    def GetMatchPrefix(self, match):
        """ Given a path past the bucket, we'll use the patch to match objects. It might have a 
        wild, or might not. Anyway, get the prefix (any portion before the wild) for this match.
        """

        if (match is None):
            self._matchinfo = None
            return None

        # Some processing required. Cache it.
        matchprefix = None
        matchwild = None
        if (match):
            for wildmatcher in ('*', '?', '['):
                idx = match.find(wildmatcher)
                if (idx > -1 and (matchprefixlen < 0 or idx < matchprefixlen)):
                    matchprefix = match[0:idx]
                    matchwild = match[idx:]
        if (matchprefix is not None):
            self._matchinfo = [ len(matchprefix), matchwild ]
        else:
            # No wild characters, then the whole thing will be a prefix.
            matchprefix = match
            self._matchinfo = [ len(match), '' ]

        return matchprefix


    def KeyMatch(self, key:str, delim: str, match:str):
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
        if (match is None):
            return True

        # matchinfo[prefix-length, wild-part]
        matchinfo = self._matchinfo[match]
        if (matchinfo is None):
            self.GetMatchPrefix(match)
            matchinfo = self._matchinfo[match]

        # Does the prefix match? Yes, of course it does, by definition...
        matchmaybe = key[matchinfo[0]:]

        # easy condition: if we didn't include a wildcard, and the prefix is followed by (or
        # ends with) a delimeter, then we match if there is no other delimeter
        if (len(matchinfo[1]) == 0):
            if (matchmaybe[0] == delim and (self._recursive or matchmaybe.find(delim, 1) < 0)):
                # matchprefix wasn't a directory but we found it to be one
                return True
            elif (matchmaybe[-1:] == delim and (self._recursive or matchmaybe.find(delim, 1) < 0)):
                # matchprefix is a directory and this is a file under it
                return True
            else:
                # This is /path/to/myfileEXTRA
                # and I only want /path/to/myfile
                return False

        # Is this object a subdirectory? We don't match any part of subdirectories.
        if (matchmaybe.find(delim) >= 0):
            return False

        # Lastly, does this object actually match the wild portion?
        return fnmatch.fnmatch(matchmaybe, matchinfo[1])


    def Test(self):
        # Set up a few match texts. Only one can be used per bucket listing.

        assert(self.GetMatchPrefix('/test/ing/123') == '/test/ing/123')
        assert(self.GetMatchPrefix('/test/ing/123?') == '/test/ing/123')
        assert(self.getMatchPrefix('/test/ing/*') == '/test/ing/')

        assert(self.KeyMatch('/buc/ket/stuff', '/', '/buc/ket/stuff*'))
        assert(not self.KeyMatch('/buc/ket/stuff/1', '/', '/buc/ket/stuff*'))
        assert(self.KeyMatch('/buc/ket/stuffy', '/', '/buc/ket/stuff*'))
        assert(self.KeyMatch('/buc/ket/stuft', '/', '/buc/ket/stuf?'))

        assert(not self.KeyMatch('/buc/ket/stuff/', '/', '/buc/ket/stuff'))
        assert(not self.KeyMatch('/buc/ket/stuff/subfile', '/', '/buc/ket/stuff'))
        assert(not self.KeyMatch('/buc/ket/stuff/subfile', '/', '/buc/ket/stuff/'))

        self._recursive = True
        assert(self.KeyMatch('/buc/ket/stuff/', '/', '/buc/ket/stuff')) # -> matches (if recursive)
        assert(self.KeyMatch('/buc/ket/stuff/subfile', '/', '/buc/ket/stuff')) # -> matches (if recursive)
        assert(self.KeyMatch('/buc/ket/stuff/subfile', '/', '/buc/ket/stuff/'))#  -> matches (if recursive)

