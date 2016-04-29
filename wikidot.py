import requests
import random
from bs4 import BeautifulSoup

# Implements various queries to Wikidot engine through its AJAX facilities



class Wikidot:
	def __init__(self, site):
		self.site = site		# Wikidot site to query
		self.delay = 100		# Delay between requests (not implemented)
		self.debug = False		# Print debug messages


	# To honor usage rules, we wait for self.delay between requests.
	# Low-level query functions call this before every request to Wikidot./
	def _wait_request_slot(self):
		# Not implemented.
		# TODO: Implement with time.clock() and some kind of sleep
		pass

	# Makes a Wikidot AJAX query. Returns the response or throws an error.
	def query(self, params):
		token = "".join(random.choice('abcdefghijklmnopqrstuvwxyz0123456789') for i in range(8))
		cookies = {"wikidot_token7": token}
		params['wikidot_token7'] = token
	
		if self.debug:
			print params
			print cookies

		self._wait_request_slot()
		req = requests.request('POST', self.site+'/ajax-module-connector.php', data=params, cookies=cookies)
		json = req.json()
		if json['status'] == 'ok':
			return json['body']
		else:
			raise req.text


	# List all pages for the site.

	# Raw version
	# For the supported formats (module_body) see:
	# See https://github.com/gabrys/wikidot/blob/master/php/modules/list/ListPagesModule.php
	def list_pages_raw(self, limit):
		res = self.query({
		  'moduleName': 'list/ListPagesModule',
		  'limit': limit if limit else '10000',
		  'perPage': limit if limit else '10000',
		  'module_body': '%%page_unix_name%%',
		  'separate': 'false',
		  'order': 'dateCreatedDesc',  # This way limit makes sense. This is also the default
		})
		return res

	# Client version
	def list_pages(self, limit):
		raw = self.list_pages_raw(limit).replace('<br/>',"\n")
		soup = BeautifulSoup(raw, 'html.parser')
		pages = []
		for entry in soup.div.p.text.split('\n'):
			pages.append(entry)
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

			# Comment is in the last TD of the row
			last_td = None
			for last_td in tr.find_all('td'):pass
			rev_comment = last_td.getText() if last_td else ""

			revs.append({
				'id': rev_id,
				'date': rev_date,
				'comment': rev_comment,
			})
		return revs


	# Retrieves revision source for a revision

	def get_revision_source(self, rev_id):
		res = self.query({
		  'moduleName': 'history/PageSourceModule',
		  'revision_id': rev_id,
		  # We don't need page id
		})
		soup = BeautifulSoup(res, 'html.parser')
		# The source is HTMLified but BeautifulSoup's getText() will decode that
		# - htmlentities
		# - <br/>s in place of linebreaks
		# - random real linebreaks (have to be ignored)
		return soup.div.getText().lstrip(' \r\n')