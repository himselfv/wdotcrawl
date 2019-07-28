import requests
import random
from bs4 import BeautifulSoup
import time
from urllib.parse import urlparse

# Implements various queries to Wikidot engine through its AJAX facilities


class Wikidot:
    def __init__(self, site):
        self.site = site        # Wikidot site to query
        self.sitename = urlparse(site).hostname.lower()
        self.delay = 200        # Delay between requests in msec
        self.debug = False      # Print debug messages
        self.next_timeslot = time.clock()   # Can call immediately


    # To honor usage rules, we wait for self.delay between requests.
    # Low-level query functions call this before every request to Wikidot./
    def _wait_request_slot(self):
        tm = time.clock()
        if self.next_timeslot - tm > 0:
            time.sleep(self.next_timeslot - tm)
        self.next_timeslot = tm + self.delay / 1000
        pass

    # Makes a Wikidot AJAX query. Returns the response+title or throws an error.
    def queryex(self, params, urlAppend = None):
        token = "".join(random.choice('abcdefghijklmnopqrstuvwxyz0123456789') for i in range(8))
        cookies = {"wikidot_token7": token}
        params['wikidot_token7'] = token

        if self.debug:
            print(params)
            print(cookies)

        self._wait_request_slot()
        url = self.site+'/ajax-module-connector.php'
        if urlAppend is not None:
            url += urlAppend

        req = requests.request('POST', url, data=params, cookies=cookies)
        try:
            json = req.json()
        except JSONDecodeError as e:
            print(e, req, url, params)
            raise e
        #print(json)

        if json['status'] == 'ok':
            return json['body'], (json['title'] if 'title' in json else '')
        else:
            raise Exception(req.text)

    # Same but only returns the body, most responses don't have titles
    def query(self, params, urlAppend = None):
        return self.queryex(params, urlAppend)[0]


    # List all pages for the site.

    # Raw version
    # For the supported formats (module_body) see:
    # See https://github.com/gabrys/wikidot/blob/master/php/modules/list/ListPagesModule.php
    def list_pages_raw(self, limit, offset):
        res = self.query({
          'moduleName': 'list/ListPagesModule',
          'limit': limit if limit else '10000',
          'perPage': limit if limit else '10000',
          'module_body': '%%page_unix_name%%',
          'separate': 'false',
          'p': str(offset),
          'order': 'dateCreatedDesc',  # This way limit makes sense. This is also the default
        }, '/p/' + str(offset))
        return res

    # Client version
    def list_pages(self, limit):
        offset = 1
        pages = []

        while True:
            raw = self.list_pages_raw(limit, offset).replace('<br/>',"\n")
            soup = BeautifulSoup(raw, 'html.parser')


            for entry in soup.div.p.text.split('\n'):
                pages.append(entry)
            if self.debug:
                print('Pages found:', len(pages))

            targets = soup.find_all('span','target')
            if len(targets) < 2:
                print("unable to find next target")
                break

            next_url = targets[-1].a.get('href').split('/')
            if len(next_url) > 0 and next_url[-1].isnumeric():
                next_page = int(next_url[-1])
                print('next page', next_page)
            else:
                print("invalid next url", next_url)
                break

            #next_page = int(targets[0].a.text)

            current_spans = soup.find_all('span','current')
            if len(current_spans) > 0:
                current_page = int(current_spans[0].text)
                print('current page', current_page)
            else:
                print("unable to find current page")
                break;

            if next_page != offset + 1:
                print('next page is wrong', next_page)
                break

            offset += 1
        return pages


    # Retrieves internal page_id by page unix_name.
    # Page IDs are required for most of page functions.

    def get_page_id(self, page_unix_name):
        # The only freaking way to get page ID is to load the page! Wikidot!
        self._wait_request_slot()
        req = requests.request('GET', self.site+'/'+page_unix_name)
        soup = BeautifulSoup(req.text, 'html.parser')
        for item in soup.head.find_all('script'):
            text = item.text
            pos = text.find("WIKIREQUEST.info.pageId = ")
            if pos >= 0:
                pos += len("WIKIREQUEST.info.pageId = ")
                crlf = text.find(";", pos)
                if crlf >= 0:
                    return int(text[pos:crlf])
                else:
                    return int(text[pos:])
        return None


    # Retrieves a list of revisions for a page.
    # See https://github.com/gabrys/wikidot/blob/master/php/modules/history/PageRevisionListModule.php

    # Raw version
    def get_revisions_raw(self, page_id, limit):
        res = self.query({
          'moduleName': 'history/PageRevisionListModule',
          'page_id': page_id,
          'page': '1',
          'perpage': limit if limit else '10000',
          'options': '{"all":true}'
        })

        soup = BeautifulSoup(res, 'html.parser')
        return soup.table.contents

    # Client version
    def get_revisions(self, page_id, limit):
        revs = []
        for tr in self.get_revisions_raw(page_id, limit):
            if tr.name != 'tr': continue # there's a header + various junk

            # RevID is stored as a value of an INPUT field
            rev_id = tr.input['value'] if tr.input else None
            if rev_id is None: continue # can't parse

            # Unixtime is stored as a CSS class time_*
            rev_date = 0
            date_span = tr.find("span", attrs={"class": "odate"})
            if date_span is not None:
                for cls in date_span['class']:
                    if cls.startswith('time_'):
                        rev_date = int(cls[5:])

            # Username in a last <a> under <span class="printuser">
            user_span = tr.find("span", attrs={"class": "printuser"})
            last_a = None
            for last_a in user_span.find_all('a'): pass
            rev_user = last_a.getText() if last_a else None


            # Comment is in the last TD of the row
            last_td = None
            for last_td in tr.find_all('td'): pass
            rev_comment = last_td.getText() if last_td else ""

            revs.append({
                'id': rev_id,
                'date': rev_date,
                'user': rev_user,
                'comment': rev_comment,
            })
        return revs


    # Retrieves revision source for a revision.
    # There's no raw version because there's nothing else in raw.
    def get_revision_source(self, rev_id):
        res = self.query({
          'moduleName': 'history/PageSourceModule',
          'revision_id': rev_id,
          # We don't need page id
        })
        # The source is HTMLified but BeautifulSoup's getText() will decode that
        # - htmlentities
        # - <br/>s in place of linebreaks
        # - random real linebreaks (have to be ignored)
        soup = BeautifulSoup(res, 'html.parser')
        return soup.div.getText().lstrip(' \r\n')

    # Retrieves the rendered version + additional info unavailable in get_revision_source:
    # * Title
    # * Unixname at the time
    def get_revision_version_raw(self, rev_id):
        res = self.queryex({
          'moduleName': 'history/PageVersionModule',
          'revision_id': rev_id,
        })
        return res

    def get_revision_version(self, rev_id):
        res = self.get_revision_version_raw(rev_id) # this has title!
        soup = BeautifulSoup(res[0], 'html.parser')

        # First table is a flyout with revision details. Remove and study it.
        unixname = None
        details = soup.find("div", attrs={"id": "page-version-info"}).extract()
        for tr in details.find_all('tr'):
            tds = tr.find_all('td')
            if len(tds) < 2: continue
            if tds[0].getText().strip() == 'Page name:':
                unixname = tds[1].getText().strip()

        return {
          'rev_id': rev_id,
          'unixname': unixname,
          'title': res[1],
          'content': str(soup), # only content remains
        }
