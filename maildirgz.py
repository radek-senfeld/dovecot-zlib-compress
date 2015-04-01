#!/usr/bin/python
#
# Based on https://github.com/denc-nl/dovecot-zlib-compress
#

from __future__ import print_function

import os
import sys
import argparse
import fnmatch
import logging
import gzip
import subprocess
import shutil
import signal
import time
import binascii


log = logging
logging.basicConfig(format="%(asctime)s %(levelname)8s [%(funcName)s:%(lineno)d] %(message)s", level=logging.DEBUG, file=sys.stderr)

def findMaildirs(startPath):
	for root, dirnames, filenames in os.walk(startPath):
		if "cur" in dirnames and "tmp" in dirnames and "new" in dirnames:
			yield root

def findCompressableFiles(maildir):
	path = os.path.join(maildir, "cur")

	for root, dirnames, filesnames in os.walk(path):
		for filename in fnmatch.filter(filesnames, "*,S=*[!Z]"):
			yield filename, maildir

def compressMail(filename, maildir):
	in_path = os.path.join(os.path.join(maildir, "cur"), filename)
	out_path = os.path.join(os.path.join(maildir, "tmp"), filename + "Z")

	log.info("compressing {} -> {}".format(in_path, out_path))
	f_in = open(in_path, "rb")
	if binascii.hexlify(f_in.read(3)) == "1f8b08":
		log.error("file is already a gzip file - not compressing")
		f_out = open(out_path, "wb")
	else:
		f_out = gzip.open(out_path, "wb")

	f_in.seek(0)
	f_out.writelines(f_in)
	f_out.close()
	f_in.close()
	clone_touch(in_path, out_path)

	return in_path, out_path


def getLock(maildir):
	# function is broken IE really unsafe for general use...
	cmd = subprocess.Popen(["/usr/libexec/dovecot/maildirlock", os.path.join(maildir, "cur"), "30"], stdout = subprocess.PIPE )
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
			log.info("moving {} to {}".format(new, os.path.join(maildir, "cur")))
			shutil.move(new, os.path.join(maildir, "cur"))

			log.info("removing: {}".format(old))
			os.remove(old)

	releaseLock(lock)

	return

def chunks(l, n):
	""" Yield successive n-sized chunks from l.
	"""
	for i in xrange(0, len(l), n):
		yield l[i:i+n]

def clone_touch(in_path, out_path):
	st = os.stat(in_path)

	with open(out_path, "a") as f:
		os.utime(out_path, (st.st_atime, st.st_mtime))
		os.fchown(f.fileno(), st.st_uid, st.st_gid)
		os.fchmod(f.fileno(), st.st_mode)

def main():
	parser = argparse.ArgumentParser(description="compress emails using zlib")
	parser.add_argument("--dir", dest="dir", help="directory to start searching")

	args = parser.parse_args()
	chunksize = 100

	if args.dir:
		for maildir in findMaildirs(args.dir):
			for chunk in chunks(list(findCompressableFiles(maildir)), chunksize):
				res = []
				for f, m in chunk:
					i, o = compressMail(f, m)
					res.append((i, o))

					print(".", end="")
				log.info("Compressed {} mails in {}".format(len(res), maildir))
				# should make the max movable chunk size smaller....
				lockAndMove(res, maildir)

if __name__ == "__main__":
	main()
