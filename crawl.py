import argparse
import sys
import locale
import codecs
import os
from wikidot import Wikidot
from rmaint import RepoMaintainer

# TODO: Files.
# TODO: Forum and comment pages.
# TODO: Ability to download new transactions since last dump.
#   We'll probably check the last revision time, then query all transactions and select those with greater revision time (not equal, since we would have downloaded equals at the previous dump)

parser = argparse.ArgumentParser(description='Queries Wikidot')
parser.add_argument('site', help='URL of Wikidot site')
# Actions
parser.add_argument('--list-pages', action='store_true', help='List all pages on this site')
parser.add_argument('--max-page-count', type=int, default='10000', help='Only list/fetch up to this amount of pages')
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
parser.add_argument('--delay', type=int, default='200', help='Delay between consequent calls to Wikidot')
args = parser.parse_args()


wd = Wikidot(args.site)
wd.debug = args.debug
wd.delay = args.delay


def force_dirs(path):
    os.makedirs(path, exist_ok=True)

if args.list_pages_raw:
    print((wd.list_pages_raw(limit = args.max_pages_count)))

elif args.list_pages:
    for page in wd.list_pages(limit = args.max_pages_count):
        print(page)

elif args.source:
    if not args.page:
        raise Exception("Please specify --page for --source.")
    
    page_id = wd.get_page_id(page_unix_name=args.page)
    if not page_id:
        raise Exception("Page not found: "+args.page)
    
    revs = wd.get_revisions(page_id, 1) # last revision
    print((wd.get_revision_source(revs[0]['id'])))

elif args.content:
    if not args.page:
        raise Exception("Please specify --page for --source.")
    
    page_id = wd.get_page_id(page_unix_name=args.page)
    if not page_id:
        raise Exception("Page not found: "+args.page)
    
    revs = wd.get_revisions(page_id, 1) # last revision
    print((wd.get_revision_version(revs[0]['id'])))

elif args.log_raw:
    if not args.page:
        raise Exception("Please specify --page for --log.")

    page_id = wd.get_page_id(page_unix_name=args.page)
    if not page_id:
        raise Exception("Page not found: "+args.page)

    print((wd.get_revisions_raw(page_id, args.depth)))


elif args.log:
    if not args.page:
        raise Exception("Please specify --page for --log.")

    page_id = wd.get_page_id(page_unix_name=args.page)
    if not page_id:
        raise Exception("Page not found: "+args.page)
    for rev in wd.get_revisions(page_id, args.depth):
        print((str(rev)))


elif args.dump:
    print(("Downloading pages to "+args.dump))
    force_dirs(args.dump)

    rm = RepoMaintainer(wd, args.dump)
    rm.debug = args.debug
    rm.storeRevIds = args.revids
    rm.max_depth = args.depth
    rm.max_page_count = args.max_page_count
    rm.buildRevisionList([args.page] if args.page else None)
    rm.openRepo()

    print("Downloading revisions")
    rm.fetchAll()

    rm.cleanup()
    print("Done.")
