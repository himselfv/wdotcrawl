import argparse
import sys
import locale
import codecs
import os
from wikidot import Wikidot

# TODO: Store page title (subtitle?)
  # Handle page name changes with revisions (at the very least, WD seems to track this)
# TODO: Store page parent
# TODO: Files.
# TODO: Remove current query limit of 1
# TODO: Unicode commit messages.

rawStdout = sys.stdout
rawStderr = sys.stderr
sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout, 'xmlcharrefreplace')
sys.stderr = codecs.getwriter(locale.getpreferredencoding())(sys.stderr, 'xmlcharrefreplace')

parser = argparse.ArgumentParser(description='Queries Wikidot')
parser.add_argument('site', help='URL of Wikidot site')
# Actions
parser.add_argument('--list-pages', action='store_true', help='List all pages on this site')
parser.add_argument('--source', action='store_true', help='Print page source (requires --page)')
parser.add_argument('--log', action='store_true', help='Print page revision log (requires --page)')
parser.add_argument('--dump', type=str, help='Download page revisions to this directory')
# Action settings
parser.add_argument('--page', type=str, help='Query only this page')
parser.add_argument('--depth', type=int, default='10000', help='Query only last N revisions')
# Common settings
parser.add_argument('--debug', action='store_true', help='Print debug info')
parser.add_argument('--delay', type=int, default='100', help='Delay between consequent calls to Wikidot (not implemented)')
args = parser.parse_args()


wd = Wikidot(args.site)
wd.debug = args.debug
wd.delay = args.delay


if args.list_pages:
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
			  'page_name' : page,
			  'rev_id' : rev['id'],
			  'date' : rev['date'],
			  'comment' : rev['comment']
			})
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

	print "Creating repository "+args.dump
	from mercurial import commands, ui, hg
	ui=ui.ui()
	commands.init(ui, args.dump)
	repo = hg.repository(ui, args.dump)
	
	print "Downloading revisions..."
	for rev in all_revs:
		page_source = wd.get_revision_source(rev['rev_id'])
		fname = args.dump+'\\'+rev['page_name']+'.txt'
		outp = codecs.open(fname, "w", "UTF-8")
		outp.write(page_source)
		outp.close()
		commands.add(ui, repo, str(fname))
		if rev['comment'] <> '':
			commit_msg = rev['page_name'] + ': ' + rev['comment']
		else:
			commit_msg = rev['page_name']
		if rev['date']:
			commit_date = str(rev['date']) + ' 0'
		else:
			commit_date = None
		print "Commiting: "+commit_msg
		
		# Things are a bit shit when it comes to commit messages.
		# Mercurial.py accepts u'message' even on Python 2.7, EXCEPT it writes
		# last-commit.txt naively, and fails.
		# We can pre-encode u'message' to str, but then mojibake will be in actual log.
		
		# This is bad. Perphas we'd be better off just calling the command line version of it.
		# At least it's Python3, so consistent.

		commands.commit(ui, repo, message=commit_msg.encode('utf-8'), date=commit_date)

