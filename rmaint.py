import wikidot

# Basic python stuff
import os
import codecs
import pickle as pickle
import json

# git stuff
from git import Repo, Actor
import time # For parsing unix epoch timestamps from wikidot and convert to normal timestamps
import re # For sanitizing usernames to fake email addresses

from tqdm import tqdm # for progress bar

# Repository builder and maintainer
# Contains logic for actual loading and maintaining the repository over the course of its construction.

# Usage:
#   rm = RepoMaintainer(wikidot, path)
#   rm.buildRevisionList(pages)
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

        self.rev_no = 0             # Next revision to process
        self.last_names = {}        # Tracks page renames: name atm -> last name in repo
        self.last_parents = {}      # Tracks page parent names: name atm -> last parent in repo

        self.repo = None            # Git repo object
        self.index = None           # Git current index object
        self.max_depth = 10000      # download at most this number of revisions
        self.max_page_count = 10000 # download at most this number of pages

        self.pbar = None
        self.first_fetched = 0      # For progress bar
        self.fetched_revids = set()

        self.revs_to_skip = []


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

    def appendFetchedRevid(self, revid):
        fp = open(self.path+'/.fetched.txt', 'a')
        fp.write(revid + '\n')
        fp.close()

    def loadFetchedRevids(self):
        self.fetched_revids = set([line.rstrip() for line in open(self.path+'/.fetched.txt', 'r')])

    def saveFailedImages(self):
        file_path = self.path + '/.failed-images.txt'
        fp = open(file_path, 'w')
        for failed in self.wd.failed_images:
            fp.write(failed + '\n')
        fp.close()

    def loadFailedImages(self):
        file_path = self.path + '/.failed-images.txt'
        if not os.path.isfile(file_path):
            return
        self.wd.failed_images = set([line.rstrip() for line in open(file_path, 'r')])

    # Persistent metadata about the repo:
    #  - Tracks page renames: name atm -> last name in repo
    #  - Tracks page parent names: name atm -> last parent in repo
    def saveMetadata(self):
        metadata = {'names': self.last_names, 'parents': self.last_parents }
        fp = open(self.path+'/.metadata.json', 'w')
        json.dump(metadata, fp)
        fp.close()

    def loadMetadata(self):
        fp = open(self.path+'/.metadata.json', 'r')
        metadata = json.load(fp)
        self.last_names = metadata['names']
        self.last_parents = metadata['parents']
        fp.close()

        self.loadFetchedRevids()
    #
    # Compiles a combined revision list for a given set of pages, or all pages on the site.
    #  pages: compile history for these pages
    #
    # If there exists a cached revision list at the repository destination,
    # it is loaded and no requests are made.
    #
    def buildRevisionList(self, pages = None):
        if os.path.isfile(self.path+'/.wrevs'):
            print("Loading cached revision list...")
            self.loadWRevs()
        else:
            self.wrevs = []
            if self.debug:
                print('No existing wrevs')

        if os.path.isfile(self.path+'/.fetched.txt'):
            self.loadFetchedRevids()
            print(len(self.fetched_revids), 'revisions already fetched')
        else:
            self.fetched_revids = set()

        if self.debug:
            print("Building revision list...")

        if not pages:
            if os.path.isfile(self.path+'/.pages'):
                print('Loading fetched pages')
                fp = open(self.path+'/.pages', 'rb')
                pages = pickle.load(fp)
                fp.close()


            if not pages or len(pages) < self.max_page_count:
                if self.debug:
                    print('Need to fetch pages')
                pages = self.wd.list_pages(self.max_page_count)
                self.savePages(pages)
            elif self.debug:
                print(len(pages), 'pages loaded')

        fetched_pages = set()

        for wrev in tqdm(self.wrevs, desc='Collecting pages we already got revisions for'):
            page_name = wrev['page_name']

            if page_name in fetched_pages:
                continue

            fetched_pages.add(page_name)

        if self.debug:
            print("Already fetched revisions for " + str(len(fetched_pages)) + " of " + str(len(pages)))

        fetched = 0
        for page in tqdm(pages, desc='Updating list of revisions to fetch'):
            if page in fetched_pages:
                continue

            # TODO: more generic blacklisting
            if page == "sandbox":
                if self.debug:
                    print("Skipping", page)
                continue

            fetched += 1
            page_id = self.wd.get_page_id(page)

            if self.debug:
                print(("ID: "+str(page_id)))

            if page_id is None:
                print('Page gone?', page)
                continue

            revs = self.wd.get_revisions(page_id=page_id, limit=self.max_depth)
            for rev in revs:
                if rev['id'] in self.fetched_revids:
                    continue

                self.wrevs.append({
                  'page_id' : page_id,
                  'page_name' : page, # current name, not at revision time (revisions can rename them)
                  'rev_id' : rev['id'],
                  'date' : rev['date'],
                  'user' : rev['user'],
                  'comment' : rev['comment'],
                })
            self.saveWRevs() # Save a cached copy

        print("Number of revisions already fetched", len(self.fetched_revids), len(self.wrevs))

        if os.path.isfile(self.path+'/.metadata.json'):
            self.loadMetadata()

        print("")

        print(("Total revisions: "+str(len(self.wrevs))))

        if self.debug:
            print("Sorting revisions...")

        self.wrevs.sort(key=lambda rev: rev['date'])
        
        if self.debug:
            if len(self.wrevs) < 100:
                print("")
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
        fp.close()
    
    def loadState(self):
        if not os.path.isfile(self.path+'/.wstate'):
            return
        fp = open(self.path+'/.wstate', 'rb')
        self.rev_no = pickle.load(fp)
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
        self.loadFailedImages()

        if os.path.isdir(self.path+'/.git'):
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
                fname = '.revid'
                codecs.open(self.path + '/' + fname, "w", "UTF-8").close()
                self.repo.index.add([fname])
                self.index.commit("Initial creation of repo")
        self.index = self.repo.index

    #
    # Takes an unprocessed revision from a revision log, fetches its data and commits it.
    # Returns false if no unprocessed revisions remain.
    #
    def commitNext(self, rev):
        if self.rev_no >= len(self.wrevs):
            return False

        if rev['rev_id'] in self.fetched_revids:
            self.rev_no += 1

            self.saveState() # Update operation state
            return True

        if rev['rev_id'] in self.revs_to_skip:
            print("Skipping", rev)
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
            if self.debug:
                print('Parent changed', parent_unixname)
            self.last_parents[unixname] = parent_unixname
        else:
            # Else use last parent_unixname we've recorded
            parent_unixname =  self.last_parents[unixname] if unixname in self.last_parents else None

        ## TODO: test
        #if rev['comment'].startswith('Removed tags: ') or rev['comment'].startswith('Added tags: '):
        #    self.updateTags(rev['comment'], rev_unixname)

        # There are also problems when parent page gets renamed -- see updateChildren

        # If the page is tracked and its name just changed, tell Git
        fname = str(rev_unixname) + '.txt'
        rename = (unixname in self.last_names) and (self.last_names[unixname] != rev_unixname)

        commit_msg = ""

        added_file_paths = []

        if rename:
            name_rename_from = str(self.last_names[unixname])+'.txt'

            if self.debug:
                print("Moving renamed", name_rename_from, "to", fname)

            self.updateChildren(self.last_names[unixname], rev_unixname) # Update children which reference us -- see comments there

            # Try to do the best we can, these situations usually stem from vandalism people have cleaned up
            if os.path.isfile(self.path + '/' + name_rename_from):
                self.index.move([name_rename_from, fname], force=True)
                commit_msg += "Renamed from " + str(self.last_names[unixname]) + ' to ' + str(rev_unixname) + ' '
            else:
                print("Source file does not exist, probably deleted or renamed from already?", name_rename_from)

        # Add new page
        elif not os.path.isfile(self.path + '/' + fname): # never before seen
            commit_msg += "Created "
            if self.debug:
                print("Adding", fname)
        elif rev['comment'] == '':
            commit_msg += "Updated "

        self.last_names[unixname] = rev_unixname

        # Ouput contents
        outp = codecs.open(self.path + '/' + fname, "w", "UTF-8")
        if details['title']:
            outp.write('title:' + details['title']+'\n')
        if parent_unixname:
            outp.write('parent:'+parent_unixname+'\n')
        outp.write(source)
        outp.close()

        added_file_paths.append(str(fname))

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

        got_images = False;

        # Add some spacing in the commit message
        if len(details['images']) > 0:
            commit_msg += '\n'

        for image in details['images']:
            if self.wd.maybe_download_file(image['src'], self.path + '/' + image['filepath']):
                commit_msg += '\nAdded image: ' + image['src']
                got_images = True
                # If we do this gitpython barfs on itself
                #added_file_paths.append(image['filepath'])
            else:
                self.saveFailedImages()


        if got_images:
            added_file_paths.append("images")
        print("Committing: " + str(self.rev_no) + '. '+commit_msg)

        # Include metadata in the commit (if changed)
        self.appendFetchedRevid(rev['rev_id'])
        self.saveMetadata()
        added_file_paths.append('.metadata.json')
        self.index.add(added_file_paths)

        username = str(rev['user'])
        email = re.sub(pattern = r'[^a-zA-Z0-9\-.+]', repl='', string=username).lower() + '@' + self.wd.sitename
        author = Actor(username, email)

        commit = self.index.commit(commit_msg, author=author, author_date=commit_date)

        if self.debug:
            print('Committed', commit.name_rev, 'by', author)

        self.fetched_revids.add(rev['rev_id'])

        self.rev_no += 1
        self.saveState() # Update operation state

        return True

    def fetchAll(self):
        to_fetch = []
        for rev in tqdm(self.wrevs, desc='Creating list of revisions to fetch'):
            if rev['rev_id'] not in self.fetched_revids:
                to_fetch.append(rev)
        for rev in tqdm(to_fetch, desc='Downloading'):
            self.commitNext(rev)

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
        if self.debug:
            print('Updating parents for', oldunixname, newunixname)

        for child in list(self.last_parents.keys()):
            if self.last_parents[child] == oldunixname and self.last_parents[child] != newunixname:
                self.updateParentField(child, self.last_parents[child], newunixname)

    def updateTags(self, comment, unixname):
        file_name = self.path+'/'+unixname+'.txt'
        removed = []
        removed_match = re.search(pattern = r'Removed tags: ([^.]+,?)\.')
        if removed_match is not None:
            removed = removed_match.group(1).split(', ')

        tags = []

        with codecs.open(file_name, "r", "UTF-8") as f:
            content = f.readlines()

        tagsline = None
        for line in content:
            if line.startswith('tags:'):
                tagsline = line
                break

        # Father forgive me for the indentation depth
        idx = -1
        if tagsline is not None:
            idx = content.index(tagsline)
            for tag in tagsline.split(','):
                if not tag in removed:
                    tags.append(tag)


        added_match = re.search(pattern = r'Added tags: ([^.]+,?)\.')
        if added_match is not None:
            tags += added_match.group(1).split(', ')

        tags.sort()

        newtagsline = 'tags:' + ','.join(tags) + '\n'
        if idx != -1:
            contents[idx] = newtagsline
        else:
            contents = newtagsline + contents

        with codecs.open(file_name, "w", "UTF-8") as f:
            f.writelines(content)

    #
    # Processes a page file and updates "parent:..." string to reflect a change in parent's unixname.
    # The rest of the file is preserved.
    #
    def updateParentField(self, child_unixname, parent_oldunixname, parent_newunixname):
        child_path = self.path+'/'+child_unixname+'.txt'

        ## TODO: find out when this happens
        if not os.path.isfile(child_path):
            print('Failed to find child file!', child_path)
            return
        with codecs.open(child_path, "r", "UTF-8") as f:
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
        if os.path.exists(self.path+'/.wstate'):
            os.remove(self.path+'/.wstate')
        else:
            print("wstate does not exist?")

        if os.path.exists(self.path+'/.wrevs'):
            os.remove(self.path+'/.wrevs')
        else:
            print("wrevs does not exist?")

        if os.path.exists(self.path+'/.pages'):
            os.remove(self.path+'/.pages')

        if self.rev_no > 0:
            self.index.add(['.fetched.txt'])
            self.index.commit('Updating fetched revisions')
