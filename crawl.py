import argparse
import sys
import locale
import codecs
import os
from wikidot import Wikidot
import hgpatch

# TODO: Store page parent
# TODO: Files.

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
# Common settings
parser.add_argument('--debug', action='store_true', help='Print debug info')
parser.add_argument('--delay', type=int, default='100', help='Delay between consequent calls to Wikidot (not implemented)')
args = parser.parse_args()


wd = Wikidot(args.site)
wd.debug = args.debug
wd.delay = args.delay


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
			  'user' : rev['user'],
			  'comment' : rev['comment'],
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

	
	# Track page renames: name atm -> last name in repo
	last_name = {}
	
	print "Downloading revisions..."
	commit_no = 0
	for rev in all_revs:
		source = wd.get_revision_source(rev['rev_id'])
		# Page title and unix_name changes are only available through another request:
		details = wd.get_revision_version(rev['rev_id'])
		
		unixname = rev['page_name']
		rev_unixname = details['unixname'] # may be different in revision than atm
		
		# If the page is tracked and its name just changed, tell HG
		rename = (unixname in last_name) and (last_name[unixname] <> rev_unixname)
		if rename:
			commands.rename(ui, repo, args.dump+'\\'+str(last_name[unixname])+'.txt', args.dump+'\\'+str(rev_unixname)+'.txt')
		
		fname = args.dump+'\\'+rev_unixname+'.txt'
		outp = codecs.open(fname, "w", "UTF-8")
		if details['title']:
			outp.write('title:'+details['title']+'\n')
		outp.write(source)
		outp.close()
		
		if not unixname in last_name: # never before seen
			commands.add(ui, repo, str(fname))
		last_name[unixname] = rev_unixname

		if rev['comment'] <> '':
			commit_msg = rev_unixname + ': ' + rev['comment']
		else:
			commit_msg = rev_unixname
		if rev['date']:
			commit_date = str(rev['date']) + ' 0'
		else:
			commit_date = None
		print "Commiting: "+str(commit_no)+'. '+commit_msg

		commands.commit(ui, repo, message=commit_msg, user=rev['user'], date=commit_date)
		commit_no += 1


