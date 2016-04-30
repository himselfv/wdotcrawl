import argparse
import sys
import locale
import codecs
import os
from mercurial import commands, ui, hg, cmdutil
import cPickle as pickle
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
parser.add_argument('--continue', dest='cont', action='store_true', help='Continue the --dump which was aborted')
parser.add_argument('--revids', action='store_true', help='Store last revision ids in the repository')
# Common settings
parser.add_argument('--debug', action='store_true', help='Print debug info')
parser.add_argument('--delay', type=int, default='100', help='Delay between consequent calls to Wikidot (not implemented)')
args = parser.parse_args()


wd = Wikidot(args.site)
wd.debug = args.debug
wd.delay = args.delay


def hglog(repo, *args, **kwargs):
	revs, expr, filematcher = cmdutil.getlogrevs(repo, args, kwargs)
	return revs
	
# Inspects repository, matches wikidot page revisions to hg changesets.
#  wdrevs: Wikidot revisions (of all relevant pages, sorted).
#  hgrevs: Existing Mercurial revisions.
# Rebuilds the last name dictionary. This is faster than querying Wikidot about each revision.
def rebuild_last_names(wrevs, hgrevs):
	if len(wrevs) < len(hgrevs):
		raise Exception("More local revisions are available than there are revisions on the server. ")
	

	hgrevs = sorted(hgrevs)
	last_names = {}

	i = 0
	for rev in hgrevs:
		ctx = repo[rev]
		
		# Each revision in a compatible repo must contain at most one change to one file.
		# Since we know its No, we can tell which file-atm it is.

		unixname = wrevs[i]['page_name']
		files = ctx.p1().status(ctx)
		modified, added, removed = files[0], files[1], files[2]
		
		#print "%d. u:%s -> m:%s a:%s r:%s" % rev, unixname, ",".join(modified), ",".join(added), ",".join(removed)
		print str(rev)+". u:"+unixname+" -> m:"+",".join(modified)+" a:"+",".join(added)+" r:"+",".join(removed)
		
		# remove special .revid file to not be included in counting
		if (len(added) >= 1) and ('.revid' in added):
			print "removing .revid from added"
			added.remove('.revid')
		if (len(modified) >= 1) and ('.revid' in modified):
			print "removing .revid from modified"
			modified.remove('.revid')
		
		if (len(modified) > 1) or (len(added) > 1) or (len(removed) > 1):
			# It's not enough to test for len(added)+len(removed)+len(modified). Renames are tracked as add+remove == 2
			raise Exception("Several files are modified in the hg revision "+str(ctx)+". ")
		
		
		if len(modified) > 0:
			fname = os.path.splitext(modified[0])
			if fname[1] != '.txt':
				raise Exception("Files other than .txt are modified in the hg revision "+str(ctx))
			
			# Since it's modification, the entry must be there
			if not unixname in last_names:
				raise Exception("Inconsistent modification in hg revision "+str(ctx)+": "+fname[0]+" modified, "
					+"but matching wikidot revision mentions "+unixname+" which have not been seen before.")
			last_names[unixname] = fname[0]
			
		elif len(added) > 0:
			add_fname = os.path.splitext(added[0])
			if add_fname[1] != '.txt':
				raise Exception("Files other than .txt are modified in the hg revision "+str(ctx))
			
			# This covers both addition and rename (delete+add)
			if len(removed) > 0: # rename
				remove_fname = os.path.splitext(removed[0])
				if remove_fname[1] != '.txt':
					raise Exception("Files other than .txt are modified in the hg revision "+str(ctx))
				
				# Since it's rename, the entry must be there and contain old name
				if not unixname in last_names:
					raise Exception("Inconsistent rename in hg revision "+str(ctx)+": "+remove_fname[0]+" renamed, "
						+"but matching wikidot revision mentions "+unixname+" for the first time.")
				if last_names[unixname] <> remove_fname[0]:
					raise Exception("Inconsistent rename in hg revision "+str(ctx)+": "+remove_fname[0]+" renamed, "
						+"but matching wikidot revision of "+unixname+" has a different name ("+last_names[unixname]+") at the time.")
				
			else: # add
				# Since it's addition, the entry must not be there
				if unixname in last_names:
					raise Exception("Inconsistent addition in hg revision "+str(ctx)+": "+add_fname[0]+" added, "
						+"but matching wikidot revision mentions "+unixname+" which is already known.")

			last_names[unixname] = add_fname[0]

		i += 1
	return last_names


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
	
	if os.path.isfile(args.dump+'\\.hg\\.wrevs'):
		print "Loading cached revision list..."
		fp = open(args.dump+'\\.hg\\.wrevs', 'rb')
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
		fp = open(args.dump+'\\.hg\\.wrevs', 'wb')
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

	print "Initializing repository "+args.dump
	from mercurial import commands, ui, hg
	ui=ui.ui()
	last_name = {} # Tracks page renames: name atm -> last name in repo

	if args.cont:
		repo = hg.repository(ui, args.dump)
		hgrevs = hglog(repo)
		last_name = rebuild_last_names(all_revs, hgrevs)
		rev_no = len(hgrevs)
	
	else: # create a new repository
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
		print "Commiting: "+str(rev_no)+'. '+commit_msg

		commands.commit(ui, repo, message=commit_msg, user=rev['user'], date=commit_date)
		rev_no += 1


