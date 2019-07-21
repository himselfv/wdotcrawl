from mercurial import scmutil, osutil
from types import MethodType
from mercurial import encoding
import codecs

# Patches commit-message unicode handling on Python 2.x

# Mercurial is internally unicode. But because it runs from ASCII console, it tries to convert
# all input from "input encoding" (set in mercurial/encoding.py)

# Problem 1:
#   If you just pass it u'unicode string', it'll fail. Even if you set "input encoding" to utf-8,
#   it'll still try to decode it to ASCII.
# Solution:
#   Patch this decoding function to pass unicode unchanged.

old_fromlocal = None

def better_fromlocal(s):
	if isinstance(s, str):
		return s.encode('utf-8')
	global old_fromlocal
	return old_fromlocal(s)

old_fromlocal = encoding.fromlocal
encoding.fromlocal = better_fromlocal


# Problem 2:
#   Separate from actual log, Mercurial stores commit message in commit-message.txt.
#   Unfortunately it uses default Python 2.x file.open which expects ASCII and auto-conversion fails.
# Solution:
#   Patch virtual-fs open() function to use codecs.open wrapper in this particular case.

old_vfs_call = None

def better_vfs_call(self, path, mode="r", text=False, atomictemp=False, notindexed=False, backgroundclose=False):
	fp = old_vfs_call(self, path, mode, text, atomictemp, notindexed, backgroundclose)
	if path.endswith('last-message.txt'):
		# Create a wrapper like codecs.open does:
		info = codecs.lookup("utf-8")
		fp = codecs.StreamReaderWriter(fp, info.streamreader, info.streamwriter, 'strict')
		fp.encoding = 'utf-8'
	return fp

old_vfs_call = scmutil.vfs.__call__
scmutil.vfs.__call__ = better_vfs_call



