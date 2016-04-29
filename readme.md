This is a Python command line client for relatively popular wiki hosting http://www.wikidot.com which lets you:

* List all pages on a site
* See all revisions of a page
* Query page source

Most interestingly, it allows you to download the whole site as a Mercurial repository, with proper commit dates and comments!

Examples:

    crawl.py http://example.wikidot.com --dump ExampleRepo
    crawl.py http://example.wikidot.com --log --page example-page

It uses internal Wikidot AJAX requests to do it's job. If you're from Wikidot, please don't break it. Thank you! We'll try to be nice and not put a load on your servers.

Useful links:

Wikidot code (very old) which simplifies things a bit:

* https://github.com/gabrys/wikidot/blob/master/php/modules/history/PageRevisionListModule.php

The descriptions for on-site modules are heavily correlated with AJAX ones:

* http://www.wikidot.com/doc-modules:listpages-module

Someone else did Wikidot AJAX:

* https://github.com/kerel-fs/ogn-rdb/blob/master/wikidotcrawler.py
