#!/usr/bin/python
from __future__ import print_function

__author__ = 'Martijn Berger <mberger@denc.nl>'
__version__ = 0.01
__docformat__ = 'plaintext'
__license__ = 'GPLv2 or later'
__copyright__ = 'Copyright (c) 2012, DENC'


import sys, os
import argparse
import fnmatch
import logging
import gzip
import subprocess
import shutil
import signal
import time
from subprocess import check_call


log = logging
logging.basicConfig(format='%(levelname)s %(asctime)s %(funcName)s %(lineno)d %(message)s', level=logging.DEBUG, file=sys.stderr)

def findMaildirs(startPath):
    for root, dirnames, filenames in os.walk(startPath):
        if 'cur' in dirnames and 'tmp' in dirnames and 'new' in dirnames:
            yield root

def findCompressableFiles(maildir):
    path = os.path.join(maildir, 'cur')
    for root, dirnames, filesnames in os.walk(path):
        for filename in fnmatch.filter(filesnames, '*,S=*[!Z]'):
            yield filename, maildir

def compressMail(filename, maildir):
    in_path = os.path.join(os.path.join(maildir, 'cur'), filename)
    out_path = os.path.join(os.path.join(maildir, 'tmp'), filename + 'Z')
    log.info('compressing {} -> {}'.format(in_path, out_path))
    f_in = open(in_path, 'rb')
    f_out = gzip.open(out_path, 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()
    check_call(['touch', '-r', in_path, out_path]) # could replace by some python call
    return in_path, out_path


def getLock(maildir):
    # function is broken IE really unsafe for general use...

    cmd = subprocess.Popen(['/usr/lib/dovecot/maildirlock', os.path.join(maildir, 'cur'), '30'], stdout = subprocess.PIPE )
    pid = -1
    while cmd.poll() is None:
        pass

    if cmd.returncode != 0:
        return -1

    try:
        for l in cmd.stdout:
            pid = int(l)
            break
    except ValueError as e:
        pass

    return pid

def releaseLock(pid):
    #unlock
    log.info("PID: {}".format(pid))
    os.kill(pid , signal.SIGTERM)

def lockAndMove(files_list, maildir):
    if len(files_list) < 1:
        return

    lock = -1

    while lock == -1:
        lock = getLock(maildir)
        time.sleep(0.5)


    if lock != -1:
        for old, new in files_list:
            log.info("moving {} to {}".format(new, os.path.join(maildir, 'cur')))
            shutil.move(new, os.path.join(maildir, 'cur'))
            log.info("removing: {}".format(old))
            os.remove(old)

    releaseLock(lock)
    return

def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

def main():
    parser = argparse.ArgumentParser(description='compress emails using zlib')
    parser.add_argument('--dir', dest='dir', help="directory to start searching")

    args = parser.parse_args()
    chunksize = 10

    if args.dir:
        for maildir in findMaildirs(args.dir):
            for chunk in chunks(list(findCompressableFiles(maildir)), chunksize):
                res = []
                for f, m in chunk:
                    i, o = compressMail(f,m)
                    print('.', end="")
                    res.append((i,o))
                log.info("Compressed {} mails in {}".format(len(res),maildir))
                # should make the max movable chunk size smaller....
                lockAndMove(res,maildir)

    sys.exit(0)


if __name__ == "__main__":
    main()