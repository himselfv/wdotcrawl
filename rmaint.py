import wikidot

# Basic python stuff
import os
import codecs
import pickle as pickle

# git stuff
from git import Repo, Actor
import time # For parsing unix epoch timestamps from wikidot and convert to normal timestamps
import re # For sanitizing usernames to fake email addresses

# Repository builder and maintainer
# Contains logic for actual loading and maintaining the repository over the course of its construction.

# Usage:
#   rm = RepoMaintainer(wikidot, path)
#   rm.buildRevisionList(pages, depth)
#   rm.openRepo()
#   while rm.commitNext():
#       pass
#   rm.cleanup()

# Talkative.

class RepoMaintainer:
    def __init__(self, wikidot, path):
        # Settings
        self.wd = wikidot           # Wikidot instance
        self.path = path            # Path to repository
        self.debug = False          # = True to enable more printing
        self.storeRevIds = True     # = True to store .revid with each commit

        # Internal state
        self.wrevs = None           # Compiled wikidot revision list (history)
        self.fetcheds_revids = []   # Compiled wikidot revision list (history)

        self.rev_no = 0             # Next revision to process
        self.last_names = {}        # Tracks page renames: name atm -> last name in repo
        self.last_parents = {}      # Tracks page parent names: name atm -> last parent in repo

        self.repo = None            # Git repo object
        self.index = None           # Git current index object


    #
    # Saves and loads revision list from file
    #
    def saveWRevs(self):
        fp = open(self.path+'/.wrevs', 'wb')
        pickle.dump(self.wrevs, fp)
        fp.close()

    def loadWRevs(self):
        fp = open(self.path+'/.wrevs', 'rb')
        self.wrevs = pickle.load(fp)
        fp.close()

    def savePages(self, pages):
        fp = open(self.path+'/.pages', 'wb')
        pickle.dump(pages, fp)
        fp.close()

    def saveFetched(self):
        fp = open(self.path+'/.fetched', 'wb')
        pickle.dump(self.fetched_revids, fp)
        fp.close()

    def loadFetched(self):
        fp = open(self.path+'/.fetched', 'rb')
        self.fetched_revids = pickle.load(fp)
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
        if os.path.isfile(self.path+'/.wrevs'):
            print("Loading cached revision list...")
            self.loadWRevs()
        else:
            self.wrevs = []
            print('no wrevs')

        if os.path.isfile(self.path+'/.fetched'):
            self.loadFetched()
        else:
            self.fetched_revids = []

        print("Building revision list...")
        if not pages:
            if os.path.isfile(self.path+'/.pages'):
                print('loading fetched pages')
                fp = open(self.path+'/.pages', 'rb')
                pages = pickle.load(fp)
                fp.close()

            print('need to fetch pages')
            if not pages:
                pages = self.wd.list_pages(10000)
                self.savePages(pages)


        fetched_pages = []

        for wrev in self.wrevs:
            page_name = wrev['page_name']

            if page_name in fetched_pages:
                continue

            fetched_pages.append(page_name)

        print("fetched " + str(len(fetched_pages)) + " of " + str(len(pages)))

        #self.wrevs = []
        fetched = 0
        for page in pages:
            if page in fetched_pages:
                #print('already fetched', page)
                continue

            if page == "sandbox":
                print("Skipping", page)
                continue

            print("Querying page: " + page + " " + str(fetched) + "/" + str(len(pages) - len(fetched_pages)))
            fetched += 1
            page_id = self.wd.get_page_id(page)
            print(("ID: "+str(page_id)))
            if page_id is None:
                print('page lost', page)
                continue

            revs = self.wd.get_revisions(page_id, depth)
            print(("Revisions: "+str(len(revs))))
            for rev in revs:
                if rev['id'] in self.fetched_revids:
                    print(rev['id'], 'already fetched')
                    continue

                self.wrevs.append({
                  'page_id' : page_id,
                  'page_name' : page, # name atm, not at revision time
                  'rev_id' : rev['id'],
                  'date' : rev['date'],
                  'user' : rev['user'],
                  'comment' : rev['comment'],
                })
            self.saveWRevs() # Save a cached copy

        print("")
        
        
        print(("Total revisions: "+str(len(self.wrevs))))
        
        print("Sorting revisions...")
        print(self.wrevs[0])
        print(self.wrevs[0]['date'])
        self.wrevs.sort(key=lambda rev: rev['date'])
        print("")
        
        if self.debug:
            if len(self.wrevs) < 100:
                print("Revision list: ")
                for rev in self.wrevs:
                    print((str(rev)+"\n"))
                print("")
            else:
                print("Too many revisions, not printing everything")


    #
    # Saves and loads operational state from file
    #
    def saveState(self):
        fp = open(self.path+'/.wstate', 'wb')
        pickle.dump(self.rev_no, fp)
        pickle.dump(self.last_names, fp)
        pickle.dump(self.last_parents, fp)
        fp.close()
    
    def loadState(self):
        fp = open(self.path+'/.wstate', 'rb')
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
        self.last_names = {} # Tracks page renames: name atm -> last name in repo
        self.last_parents = {} # Tracks page parent names: name atm -> last parent in repo

        if os.path.isfile(self.path+'/.git'):
            print("Continuing from aborted dump state...")
            self.loadState()
            self.repo = Repo(self.path)
            assert not self.repo.bare

        else: # create a new repository (will fail if one exists)
            print("Initializing repository...")
            self.repo = Repo.init(self.path)
            self.rev_no = 0

            if self.storeRevIds:
                # Add revision id file to the new repo
                fname = '/.revid'
                codecs.open(self.path + fname, "w", "UTF-8").close()
                self.repo.index.add([fname])
                self.index.commit("Initial creation of repo")
        self.index = self.repo.index

    #
    # Takes an unprocessed revision from a revision log, fetches its data and commits it.
    # Returns false if no unprocessed revisions remain.
    #
    def commitNext(self):
        if self.rev_no >= len(self.wrevs):
            return False

        rev = self.wrevs[self.rev_no]

        if rev['rev_id'] in self.fetched_revids:
            print(rev['rev_id'], 'already fetched')
            self.rev_no += 1

            self.saveState() # Update operation state
            return True

        source = self.wd.get_revision_source(rev['rev_id'])
        # Page title and unix_name changes are only available through another request:
        details = self.wd.get_revision_version(rev['rev_id'])

        # Store revision_id for last commit
        # Without this, empty commits (e.g. file uploads) will be skipped by Git
        if self.storeRevIds:
            fname = self.path+'/.revid'
            outp = codecs.open(fname, "w", "UTF-8")
            outp.write(rev['rev_id']) # rev_ids are unique amongst all pages, and only one page changes in each commit anyway
            outp.close()

        unixname = rev['page_name']
        rev_unixname = details['unixname'] # may be different in revision than atm

        # Unfortunately, there's no exposed way in Wikidot to see page breadcrumbs at any point in history.
        # The only way to know they were changed is revision comments, though evil people may trick us.
        if rev['comment'].startswith('Parent page set to: "'):
            # This is a parenting revision, remember the new parent
            parent_unixname = rev['comment'][21:-2]
            self.last_parents[unixname] = parent_unixname
        else:
            # Else use last parent_unixname we've recorded
            parent_unixname =  self.last_parents[unixname] if unixname in self.last_parents else None
        # There are also problems when parent page gets renamed -- see updateChildren

        # If the page is tracked and its name just changed, tell Git
        fname = str(rev_unixname) + '.txt'
        rename = (unixname in self.last_names) and (self.last_names[unixname] != rev_unixname)

        if rename:
            name_rename_from = str(self.last_names[unixname])+'.txt'

            if self.debug:
                print("moving", name_rename_from, "to", fname)

            self.updateChildren(self.last_names[unixname], rev_unixname) # Update children which reference us -- see comments there

            # Try to do the best we can, these situations usually stem from vandalism people have cleaned up
            if os.path.isfile(self.path + '/' + name_rename_from):
                self.index.move([name_rename_from, fname], force=True)
            else:
                print("source file does not exist, probably deleted or renamed from already", name_rename_from)

        # Ouput contents
        outp = codecs.open(self.path + '/' + fname, "w", "UTF-8")
        if details['title']:
            outp.write('title:'+details['title']+'\n')
        if parent_unixname:
            outp.write('parent:'+parent_unixname+'\n')
        outp.write(source)
        outp.close()

        commit_msg = ""

        # Add new page
        if not unixname in self.last_names: # never before seen
            commit_msg += "Created "
            if self.debug:
                print("adding", fname)
        elif rev['comment'] == '':
            commit_msg += "Updated "

        commit_msg += rev_unixname

        # Commit
        if rev['comment'] != '':
            commit_msg += ': ' + rev['comment']
        else:
            commit_msg += ' (no message)'
        if rev['date']:
            parsed_time = time.gmtime(int(rev['date'])) # TODO: assumes GMT
            commit_date = time.strftime('%Y-%m-%d %H:%M:%S', parsed_time)
        else:
            commit_date = None

        print(("Commiting: "+str(self.rev_no)+'. '+commit_msg))

        username = str(rev['user'])
        email = re.sub(pattern = r'[^a-zA-Z0-9\-.+]', repl='', string=username).lower() + '@' + self.wd.sitename

        author = Actor(username, email)

        self.index.add([str(fname)])
        self.last_names[unixname] = rev_unixname
        commit = self.index.commit(commit_msg, author=author, commit_date=commit_date)
        self.rev_no += 1

        if self.debug:
            print('committed', commit.name_rev, 'by', author)

        self.fetched_revids.append(rev['rev_id'])
        self.saveFetched()

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
        for child in list(self.last_parents.keys()):
            if self.last_parents[child] == oldunixname:
                self.updateParentField(child, self.last_parents[child], newunixname)

    #
    # Processes a page file and updates "parent:..." string to reflect a change in parent's unixname.
    # The rest of the file is preserved.
    #
    def updateParentField(self, child_unixname, parent_oldunixname, parent_newunixname):
        with codecs.open(self.path+'/'+child_unixname+'.txt', "r", "UTF-8") as f:
            content = f.readlines()
        # Since this is all tracked by us, we KNOW there's a line in standard format somewhere
        idx = content.index('parent:'+parent_oldunixname+'\n')
        if idx < 0:
            raise Exception("Cannot update child page "+child_unixname+": "
                +"it is expected to have parent set to "+parent_oldunixname+", but there seems to be no such record in it.");
        content[idx] = 'parent:'+parent_newunixname+'\n'
        with codecs.open(self.path+'/'+child_unixname+'.txt', "w", "UTF-8") as f:
            f.writelines(content)


    #
    # Finalizes the construction process and deletes any temporary files.
    #
    def cleanup(self):
        os.remove(self.path+'/.wstate')
        os.remove(self.path+'/.wrevs')

        if os.path.isfile(self.path+'/.pages'):
            os.remove(self.path+'/.pages')

