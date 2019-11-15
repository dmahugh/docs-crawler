"""Crawl all Cloud SQL documentation pages and write them to temp.csv.
"""
import collections
import string

from urllib.parse import urldefrag, urljoin, urlparse

import bs4
import requests

# configuration options
OUTPUT_FILE = "pages_visited.csv"
#STARTPAGE = "https://cloud.google.com/sql/docs/"
STARTPAGE = "https://cloud.google.com/sql/docs/sqlserver/admin-api/"
# Only pages in the SUBPATH path under the root domain of STARTPAGE will be
# visited, to avoid crawling content external to our docs pages. 
#SUBPATH="/sql/docs"
SUBPATH="/sql/docs/sqlserver"
PAGE_LIMIT = 999 # set to a lower number for quick testing/verification

def crawler(startpage, maxpages=100, singledomain=True, subpath=None) -> None:
    """Crawl the web starting from specified page.

    Args:
        startpage: URL of starting page
        maxpages: maximum number of pages to crawl
        singledomain: whether to only crawl links within startpage's domain
        subpath: optional subpath within singledomain, to only crawl pages
                 under that path

    Returns:
        None - calls pagehandler() for each page.
    """
    domain = urlparse(startpage).netloc if singledomain else None

    crawled = set() # pages already crawled
    failed = 0 # number of links that couldn't be crawled

    # create output file and write header row
    with open(OUTPUT_FILE, "w") as fhandle:
        fhandle.write(f"Page Title,URL\n")

    pagequeue: collections.deque = collections.deque()
    pagequeue.append(startpage)

    sess = requests.session()
    while len(crawled) < maxpages and pagequeue:
        url = pagequeue.popleft()  # next page to crawl (FIFO queue)

        try:
            response = sess.get(url) # read the page
        except (requests.exceptions.MissingSchema, requests.exceptions.InvalidSchema):
            print("*FAILED*:", url)
            failed += 1
            continue
        if not response.headers["content-type"].startswith("text/html"):
            continue  # don't crawl non-HTML content

        soup = bs4.BeautifulSoup(response.text, "html.parser")

        # process the page
        crawled.add(url)
        pagehandler(url, response, soup)

        # get links and add to the queue
        links = getlinks(url, domain, subpath, soup)
        for link in links:
            if not url_in_list(link, crawled) and not url_in_list(link, pagequeue):
                pagequeue.append(link)

    print(f"{len(crawled)} pages crawled, {failed} links failed.")


def getlinks(pageurl, domain, subpath, soup):
    """Return list of links to be crawled for a specified page.

    Args:
        pageurl: URL of the page
        domain: domain being crawled (None to return links to *any* domain)
        subpath: optional subpath within domain
        soup: BeautifulSoup object for the page

    Returns:
        A list of URLs to be followed.
    """

    # get target URLs for all links on the page
    links = [a.attrs.get("href") for a in soup.select("a[href]")]

    # remove fragment identifiers
    links = [urldefrag(link)[0] for link in links]

    # remove any empty strings
    links = [link for link in links if link]

    # if it's a relative link, change to absolute
    links = [
        link if bool(urlparse(link).netloc) else urljoin(pageurl, link)
        for link in links
    ]

    # handle domain and subpath restrictions, if any
    if domain:
        if subpath:
            links = [
                link
                for link in links
                if urlparse(link).path.startswith(subpath)
                and samedomain(urlparse(link).netloc, domain)
            ]
        else:
            links = [
                link for link in links if samedomain(urlparse(link).netloc, domain)
            ]

    return links


def pagehandler(pageurl, pageresponse, soup):
    """Process a found page.

    Args:
        pageurl: URL of this page
        pageresponse: page content; response object from requests module
        soup: Beautiful Soup object created from pageresponse

    Returns:
        None. Page's URL and title are written to OUTPUT_FILE.
    """

    # Cloud SQL page titles include breadcrumbs as xxx | yyy | zzz, so we
    # extract the first one only.
    full_title = soup.title.string
    page_title = full_title.split("|")[0].strip() if "|" in full_title else full_title

    print(f"{pageurl[:60]:60} {page_title}")
    with open(OUTPUT_FILE, "a") as fhandle:
        fhandle.write(f"{page_title},{pageurl}\n") # append to output file


def samedomain(netloc1, netloc2):
    """Determine whether two netloc values are in the same domain.

    Args:
        netloc1/netloc2: netloc values to be compared.

    Returns:
        True if netloc1 and netloc2 are in same domain, false otherwise.
        samedomain('www.microsoft.com', 'microsoft.com') == True
        samedomain('google.com', 'www.google.com') == True
        samedomain('api.github.com', 'www.github.com') == True
    """
    domain1 = netloc1.lower()
    if "." in domain1:
        domain1 = domain1.split(".")[-2] + "." + domain1.split(".")[-1]

    domain2 = netloc2.lower()
    if "." in domain2:
        domain2 = domain2.split(".")[-2] + "." + domain2.split(".")[-1]

    return domain1 == domain2


def url_in_list(url, url_list):
    """Determine whether a URL is in a list of URLs.

    Args:
        url: the url to be searched for. May start with http:// or https://.
        url_list: the list of URLs to be searched.

    Returns:
        True if url is in the list, regardless of whether http:// or https://.
        This function is used to avoid crawling the same page separately as
        http and https.
    """
    return (url.replace("https://", "http://") in url_list) or (
        url.replace("http://", "https://") in url_list
    )


if __name__ == "__main__":
    crawler(
        startpage=STARTPAGE,
        maxpages=PAGE_LIMIT,
        singledomain=True,
        subpath=SUBPATH,
    )
