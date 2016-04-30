import os
import codecs
from mercurial import commands, ui, hg
import hgpatch
import cPickle as pickle
import wikidot

# Repository builder and maintainer
# Contains logic for actual loading and maintaining the repository over the course of its construction.

# Usage:
#   rm = RepoMaintainer(wikidot, path)
#   rm.buildRevisionList(pages, depth)
#   rm.openRepo()
#   while rm.commitNext():
#		pass
#   rm.cleanup()

# Talkative.

class RepoMaintainer:
	def __init__(self, wikidot, path):
		# Settings
		self.wd = wikidot			# Wikidot instance
		self.path = path			# Path to repository
		self.debug = False			# = True to enable more printing
		self.storeRevIds = True		# = True to store .revid with each commit
		
		# Internal state
		self.wrevs = None			# Compiled wikidot revision list (history)
		
		self.rev_no	= 0				# Next revision to process
		self.last_names = {}		# Tracks page renames: name atm -> last name in repo
		self.last_parents = {}		# Tracks page parent names: name atm -> last parent in repo
		
		self.ui = None				# Mercurial UI object
		self.repo = None			# Mercurial repo object


	#
	# Saves and loads revision list from file
	#
	def saveWRevs(self):
		fp = open(self.path+'\\.wrevs', 'wb')
		pickle.dump(self.wrevs, fp)
		fp.close()
	
	def loadWRevs(self):
		fp = open(self.path+'\\.wrevs', 'rb')
		self.wrevs = pickle.load(fp)
		fp.close()

	#
	# Compiles a combined revision list for a given set of pages, or all pages on the site.
	#  pages: compile history for these pages
	#  depth: download at most this number of revisions.
	#
	# If there exists a cached revision list at the repository destination,
	# it is loaded and no requests are made.
	#
	def buildRevisionList(self, pages = None, depth = 10000):
		if os.path.isfile(self.path+'\\.wrevs'):
			print "Loading cached revision list..."
			self.loadWRevs()
		else:
			print "Building revision list..."
			if not pages:
				pages = self.wd.list_pages(10000)
			self.wrevs = []
			for page in pages:
				print "Querying page: "+page
				page_id = self.wd.get_page_id(page)
				print "ID: "+str(page_id)
				revs = self.wd.get_revisions(page_id, depth)
				print "Revisions: "+str(len(revs))
				for rev in revs:
					self.wrevs.append({
					  'page_id' : page_id,
					  'page_name' : page, # name atm, not at revision time
					  'rev_id' : rev['id'],
					  'date' : rev['date'],
					  'user' : rev['user'],
					  'comment' : rev['comment'],
					})
			self.saveWRevs() # Save a cached copy
			print ""
		
		
		print "Total revisions: "+str(len(self.wrevs))
		
		print "Sorting revisions..."
		self.wrevs.sort(key=lambda rev: rev['date'])
		print ""
		
		if self.debug:
			print "Revision list: "
			for rev in self.wrevs:
				print str(rev)+"\n"
			print ""


	#
	# Saves and loads operational state from file
	#
	def saveState(self):
		fp = open(self.path+'\\.wstate', 'wb')
		pickle.dump(self.rev_no, fp)
		pickle.dump(self.last_names, fp)
		pickle.dump(self.last_parents, fp)
		fp.close()
	
	def loadState(self):
		fp = open(self.path+'\\.wstate', 'rb')
		self.rev_no = pickle.load(fp)
		self.last_names = pickle.load(fp)
		try:
			self.last_parents = pickle.load(fp)
		except EOFError:
			pass
		fp.close()


	#
	# Initializes the construction process, after the revision list has been compiled.
	# Either creates a new repo, or loads the existing one at the target path
	# and restores its construction state.
	#
	def openRepo(self):
		# Create a new repository or continue from aborted dump
		self.ui=ui.ui()
		self.last_names = {} # Tracks page renames: name atm -> last name in repo
		self.last_parents = {} # Tracks page parent names: name atm -> last parent in repo
		
		if os.path.isfile(self.path+'\\.wstate'):
			print "Continuing from aborted dump state..."
			self.loadState()
			self.repo = hg.repository(self.ui, self.path)
		
		else: # create a new repository (will fail if one exists)
			print "Initializing repository..."
			commands.init(self.ui, self.path)
			self.repo = hg.repository(self.ui, self.path)
			self.rev_no = 0
			
			# Add revision id file to the new repo
			fname = self.path+'\\.revid'
			codecs.open(fname, "w", "UTF-8").close()
			commands.add(self.ui, self.repo, str(fname))
	
	
	#
	# Takes an unprocessed revision from a revision log, fetches its data and commits it.
	# Returns false if no unprocessed revisions remain.
	#
	def commitNext(self):
		if self.rev_no >= len(self.wrevs):
			return False
			
		rev = self.wrevs[self.rev_no]
		source = self.wd.get_revision_source(rev['rev_id'])
		# Page title and unix_name changes are only available through another request:
		details = self.wd.get_revision_version(rev['rev_id'])
		
		# Store revision_id for last commit
		# Without this, empty commits (e.g. file uploads) will be skipped by Mercurial
		if self.storeRevIds:
			fname = self.path+'\\.revid'
			outp = codecs.open(fname, "w", "UTF-8")
			outp.write(rev['rev_id']) # rev_ids are unique amongst all pages, and only one page changes in each commit anyway
			outp.close()
		
		unixname = rev['page_name']
		rev_unixname = details['unixname'] # may be different in revision than atm
		
		# Unfortunately, there's no exposed way in Wikidot to see page breadcrumbs at any point in history.
		# The only way to know they were changed is revision comments, though evil people may trick us.

		# BUT!
		# If we set parent page (this is registered in revision log),
		# then rename parent page
		# then the child page still links to the correct parent, though no adjustment entry exists in the revision log.
		
		# This seems to be impossible to correct.
		# Even if there existed a way to get exact parent page info from a revision,
		# even if there existed a way to get exact parent page info for any page at any point of time,
		# we would still have to check ALL pages on EVERY revisions.
		
		# Though... now that I think about it... we could be smart!
		# We can keep track of parent pages for every page we've seen.
		# Then we know all children of every page.
		# If the page is renamed, in that same revision we must adjust all children.
		# Though this will require us to parse children text (we have nowhere to regenerate it from).
		if rev['comment'].startswith('Parent page set to: "'):
			parent_unixname = rev['comment'][21:-2]
		else:
			parent_unixname = None
		self.last_parents[unixname] = parent_unixname
		
		# If the page is tracked and its name just changed, tell HG
		rename = (unixname in self.last_names) and (self.last_names[unixname] <> rev_unixname)
		if rename:
			self.updateChildren(self.last_names[unixname], rev_unixname) # Update children which reference us -- see comments there
			commands.rename(self.ui, self.repo, self.path+'\\'+str(self.last_names[unixname])+'.txt', self.path+'\\'+str(rev_unixname)+'.txt')
		
		# Ouput contents
		fname = self.path+'\\'+rev_unixname+'.txt'
		outp = codecs.open(fname, "w", "UTF-8")
		if details['title']:
			outp.write('title:'+details['title']+'\n')
		if parent_unixname:
			outp.write('parent:'+parent_unixname+'\n')
		outp.write(source)
		outp.close()
		
		# Add new page
		if not unixname in self.last_names: # never before seen
			commands.add(self.ui, self.repo, str(fname))

		self.last_names[unixname] = rev_unixname

		# Commit
		if rev['comment'] <> '':
			commit_msg = rev_unixname + ': ' + rev['comment']
		else:
			commit_msg = rev_unixname
		if rev['date']:
			commit_date = str(rev['date']) + ' 0'
		else:
			commit_date = None
		print "Commiting: "+str(self.rev_no)+'. '+commit_msg

		commands.commit(self.ui, self.repo, message=commit_msg, user=rev['user'], date=commit_date)
		self.rev_no += 1

		self.saveState() # Update operation state
		return True


	#
	# Updates all children of the page to reflect parent's unixname change.
	#
	# Any page may be assigned a parent, which adds entry to revision log. We store this as parent:unixname in the page body.
	# A parent may then be renamed.
	# Wikidot logs no additional changes for child pages, yet they stay linked to the parent.
	#
	# Therefore, on every rename we must update all linked children in the same revision.
	#
	def updateChildren(self, oldunixname, newunixname):
		for child in self.last_parents.keys():
			if self.last_parents[child] == oldunixname:
				self.updateParentField(child, self.last_parents[child], newunixname)
	
	#
	# Processes a page file and updates "parent:..." string to reflect a change in parent's unixname.
	# The rest of the file is preserved.
	#
	def updateParentField(self, child_unixname, parent_oldunixname, parent_newunixname):
		with codecs.open(self.path+'\\'+fname+'.txt', "r", "UTF-8") as f:
			content = f.readlines()
		# Since this is all tracked by us, we KNOW there's a line in standard format somewhere
		idx = content.index('parent:'+parent_oldunixname)
		if idx < 0:
			raise Exception("Cannot update child page "+child_unixname+": "
				+"it is expected to have parent set to "+parent_oldunixname+", but there seems to be no such record in it.");
		content[idx] = 'parent:'+parent_newunixname
		with codecs.open(self.path+'\\'+fname+'.txt', "w", "UTF-8") as f:
			f.writelines(content)


	#
	# Finalizes the construction process and deletes any temporary files.
	#
	def cleanup(self):
		os.remove(self.path+'\\.wstate')
		os.remove(self.path+'\\.wrevs')