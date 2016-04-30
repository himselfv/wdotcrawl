import argparse
import sys
import locale
import codecs
import os
from mercurial import commands, ui, hg
import cPickle as pickle
from wikidot import Wikidot
import hgpatch

# TODO: Store page parent
# TODO: Files.
# TODO: Delays.
# TODO: Ability to download new transactions since last dump.
#   We'll probably check the last revision time, then query all transactions and select those with greater revision time (not equal, since we would have downloaded equals at the previous dump)

rawStdout = sys.stdout
rawStderr = sys.stderr
sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout, 'xmlcharrefreplace')
sys.stderr = codecs.getwriter(locale.getpreferredencoding())(sys.stderr, 'xmlcharrefreplace')

parser = argparse.ArgumentParser(description='Queries Wikidot')
parser.add_argument('site', help='URL of Wikidot site')
# Actions
parser.add_argument('--list-pages', action='store_true', help='List all pages on this site')
parser.add_argument('--source', action='store_true', help='Print page source (requires --page)')
parser.add_argument('--content', action='store_true', help='Print page content (requires --page)')
parser.add_argument('--log', action='store_true', help='Print page revision log (requires --page)')
parser.add_argument('--dump', type=str, help='Download page revisions to this directory')
# Debug actions
parser.add_argument('--list-pages-raw', action='store_true')
parser.add_argument('--log-raw', action='store_true')
# Action settings
parser.add_argument('--page', type=str, help='Query only this page')
parser.add_argument('--depth', type=int, default='10000', help='Query only last N revisions')
parser.add_argument('--revids', action='store_true', help='Store last revision ids in the repository')
# Common settings
parser.add_argument('--debug', action='store_true', help='Print debug info')
parser.add_argument('--delay', type=int, default='100', help='Delay between consequent calls to Wikidot (not implemented)')
args = parser.parse_args()


wd = Wikidot(args.site)
wd.debug = args.debug
wd.delay = args.delay


def force_dirs(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != os.errno.EEXIST:
            raise

if args.list_pages_raw:
	print wd.list_pages_raw(args.depth)

elif args.list_pages:
	for page in wd.list_pages(args.depth):
		print page

elif args.source:
	if not args.page:
		raise "Please specify --page for --source."
	
	page_id = wd.get_page_id(args.page)
	if not page_id:
		raise "Page not found: "+args.page
	
	revs = wd.get_revisions(page_id, 1) # last revision
	print wd.get_revision_source(revs[0]['id'])

elif args.content:
	if not args.page:
		raise "Please specify --page for --source."
	
	page_id = wd.get_page_id(args.page)
	if not page_id:
		raise "Page not found: "+args.page
	
	revs = wd.get_revisions(page_id, 1) # last revision
	print wd.get_revision_version(revs[0]['id'])

elif args.log_raw:
	if not args.page:
		raise "Please specify --page for --log."

	page_id = wd.get_page_id(args.page)
	if not page_id:
		raise "Page not found: "+args.page

	print wd.get_revisions_raw(page_id, args.depth)


elif args.log:
	if not args.page:
		raise "Please specify --page for --log."

	page_id = wd.get_page_id(args.page)
	if not page_id:
		raise "Page not found: "+args.page
	for rev in wd.get_revisions(page_id, args.depth):
		print unicode(rev)


elif args.dump:
	print "Downloading pages to "+args.dump
	force_dirs(args.dump)
	
	if os.path.isfile(args.dump+'\\.wrevs'):
		print "Loading cached revision list..."
		fp = open(args.dump+'\\.wrevs', 'rb')
		all_revs = pickle.load(fp)
		fp.close()
	else:
		print "Building revision list..."
		pages = [args.page] if args.page else wd.list_pages(10000)
		all_revs = []
		for page in pages:
			print "Querying page: "+page
			page_id = wd.get_page_id(page)
			print "ID: "+str(page_id)
			revs = wd.get_revisions(page_id, args.depth)
			print "Revisions: "+str(len(revs))
			for rev in revs:
				all_revs.append({
				  'page_id' : page_id,
				  'page_name' : page, # name atm, not at revision time
				  'rev_id' : rev['id'],
				  'date' : rev['date'],
				  'user' : rev['user'],
				  'comment' : rev['comment'],
				})

		# Save a cached copy
		fp = open(args.dump+'\\.wrevs', 'wb')
		pickle.dump(all_revs, fp)
		fp.close()
		print ""

	print "Total revisions: "+str(len(all_revs))

	print "Sorting revisions..."
	all_revs.sort(key=lambda rev: rev['date'])
	print ""
	
	if args.debug:
		print "Revision list: "
		for rev in all_revs:
			print str(rev)+"\n"
		print ""


	# Create a new repository or continue from aborted dump
	ui=ui.ui()
	last_names = {} # Tracks page renames: name atm -> last name in repo

	if os.path.isfile(args.dump+'\\.wstate'):
		print "Continuing from aborted dump state..."
		fp = open(args.dump+'\\.wstate', 'rb')
		rev_no = pickle.load(fp)
		last_names = pickle.load(fp)
		fp.close()
		repo = hg.repository(ui, args.dump)

	else: # create a new repository (will fail if one exists)
		print "Initializing repository..."
		commands.init(ui, args.dump)
		repo = hg.repository(ui, args.dump)
		rev_no = 0
		
		# Add revision id file to the new repo
		fname = args.dump+'\\.revid'
		codecs.open(fname, "w", "UTF-8").close()
		commands.add(ui, repo, str(fname))


	print "Downloading revisions..."
	while rev_no < len(all_revs):
		rev = all_revs[rev_no]
		source = wd.get_revision_source(rev['rev_id'])
		# Page title and unix_name changes are only available through another request:
		details = wd.get_revision_version(rev['rev_id'])
		
		# Store revision_id for last commit
		# Without this, empty commits (e.g. file uploads) will be skipped by Mercurial
		if args.revids:
			fname = args.dump+'\\.revid'
			outp = codecs.open(fname, "w", "UTF-8")
			outp.write(rev['rev_id']) # rev_ids are unique amongst all pages, and only one page changes in each commit anyway
			outp.close()
		
		unixname = rev['page_name']
		rev_unixname = details['unixname'] # may be different in revision than atm
		
		# If the page is tracked and its name just changed, tell HG
		rename = (unixname in last_names) and (last_names[unixname] <> rev_unixname)
		if rename:
			commands.rename(ui, repo, args.dump+'\\'+str(last_names[unixname])+'.txt', args.dump+'\\'+str(rev_unixname)+'.txt')
		
		fname = args.dump+'\\'+rev_unixname+'.txt'
		outp = codecs.open(fname, "w", "UTF-8")
		if details['title']:
			outp.write('title:'+details['title']+'\n')
		outp.write(source)
		outp.close()
		
		if not unixname in last_names: # never before seen
			commands.add(ui, repo, str(fname))
		last_names[unixname] = rev_unixname

		if rev['comment'] <> '':
			commit_msg = rev_unixname + ': ' + rev['comment']
		else:
			commit_msg = rev_unixname
		if rev['date']:
			commit_date = str(rev['date']) + ' 0'
		else:
			commit_date = None
		print "Commiting: "+str(rev_no)+'. '+commit_msg

		commands.commit(ui, repo, message=commit_msg, user=rev['user'], date=commit_date)
		rev_no += 1

		# Update operation state
		fp = open(args.dump+'\\.wstate', 'wb')
		pickle.dump(rev_no, fp)
		pickle.dump(last_names, fp)
		fp.close()
		
	# Delete operation state
	os.remove(args.dump+'\\.wstate')