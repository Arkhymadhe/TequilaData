import json
import os
import time

import pandas as pd
import numpy as np

import requests
from requests_html import HTMLSession
from bs4 import BeautifulSoup

from classes import PageHunter, Obtainer


class MegaObtainer:
    """Mega class which uses [an] instance[s] of the Obtainer class to extract data from multiple pages."""

    TEQUILA_STORE = os.path.join(
        os.getcwd().replace("scripts", "data"), "final", "tequilaData.json"
    )

    REVIEWER_STORE = os.path.join(
        os.getcwd().replace("scripts", "data"), "final", "tequilaReviews.json"
    )

    COMMUNITY_STORE = os.path.join(
        os.getcwd().replace("scripts", "data"), "final", "communityData.json"
    )

    ARCHIVE_STORE = os.path.join(
        os.getcwd().replace("scripts", "secrets"), "megaObtainerArchive.json"
    )

    tequila_index = 0
    review_index = 0
    community_index = 0

    REVIEWER_DB = dict()
    TEQUILA_DB = dict()
    COMMUNITY_DB = dict()

    def __init__(self, index=0, urls=None):
        if urls is None:
            try:
                self.loadLinks()

            except json.JSONDecodeError:
                self.webHunter = WebHunter()
                self.webHunter.run()
        else:
            self.urls = urls

        self.num_pages = len(self.urls)

        self.rand_page = np.random.randint(low=0, high=self.num_pages)

        self.index = index
        self.obtainer = Obtainer(None, None)

        try:
            with open(self.ARCHIVE_STORE, "r") as f:
                self.archive = json.load(f)
                f.close()

            size_now = self.numUrls()
            print(f"Present number of URLs to scrape through: {size_now}")

            self.trimUrls()
            size_later = self.numUrls()

            if size_now != size_later:
                print(
                    f"New number of URLs to scrape through: {size_later}."
                )

                print(
                    f"Reduction of {100 * (size_now - size_later) / size_now}%"
                )
        except json.JSONDecodeError:
            self.archive = dict()

        return

    def commit(self):
        """Commit data from Obtainer caches to MegaObtainer caches"""

        self.REVIEWER_DB.update(self.obtainer.reviewerData)

        self.TEQUILA_DB.update({self.tequila_index: self.obtainer.tequilaData})

        self.COMMUNITY_DB.update({self.community_index: self.obtainer.communityData})

        self.tequila_index += 1
        self.community_index += 1

        return

    def persist(self, mode="w"):
        """Persist all extracted data in MegaObtainer caches to storage"""

        with open(self.REVIEWER_STORE, "r") as f, open(
            self.TEQUILA_STORE, "r"
        ) as g, open(self.COMMUNITY_STORE, "r") as h:
            try:
                r = json.load(f)
                r.update(self.REVIEWER_DB)
            except json.JSONDecodeError:
                r = self.REVIEWER_DB

            try:
                teq = json.load(g)
                teq.update(self.TEQUILA_STORE)
            except json.JSONDecodeError:
                teq = self.TEQUILA_STORE

            try:
                com = json.load(h)
                com.update(self.COMMUNITY_DB)
            except json.JSONDecodeError:
                com = self.COMMUNITY_DB

            f.close()
            g.close()
            h.close()

        with open(self.REVIEWER_STORE, mode) as f, open(
            self.TEQUILA_STORE, mode
        ) as g, open(self.COMMUNITY_STORE, mode) as h:
            json.dump(r, f, indent=4)  # Reviewer data
            json.dump(teq, g, indent=4)  # Tequila data
            json.dump(com, h, indent=4)  # Community data

            f.close()
            g.close()
            h.close()

        self.clear()

        return

    def crawlUrl(self):
        """Crawl through a URL and extract all needed data"""

        num_pages = len(self.urls)

        rand_page = np.random.randint(low=0, high=num_pages)

        # Present link and previous link selected must not be on pages that are too far apart.
        # This would not be similar to a human web-browsing pattern, and may lead to flagging
        while abs(self.rand_page - rand_page) > 3:
            rand_page = np.random.randint(low=0, high=num_pages)
            self.archive.setdefault(rand_page, list())

            if not bool(self.urls[rand_page]):
                del self.urls[rand_page]
                continue

        # Store the randomly selected page from which link will be picked out
        self.rand_page = rand_page

        links = self.urls[self.rand_page]
        num_links = len(links)

        link_index = np.random.randint(low=0, high=num_links)

        self.obtainer.rerun(links[link_index])

        links_scraped = len(self.archive[self.rand_page])

        print(
            f"Link [{links_scraped+1}/{num_links + links_scraped}] for Page {self.rand_page} scraped!\n"
        )

        self.commit()

        print("Reviews, tequila details, and community characterizations committed!\n")

        self.persist()

        print("Reviews, tequila details, and community characterizations persisted!\n")

        # Eliminate scraped link from urls to be visited
        scraped_link = links.pop(link_index)
        self.urls[rand_page] = links

        # Save it to archived links and persist
        self.archive[rand_page].append(scraped_link)

        self.persistArchive()

        return

    def persistArchive(self):
        """Persist archived URLS i.e., URLs already crawled through"""

        with open(self.ARCHIVE_STORE, "w") as f:
            json.dump(self.archive, f)
            f.close()

        return

    def trimUrls(self):
        """Trim out redundant URLs"""

        self.urls = {
            key: list(set(urls) - set(self.archive[key]))
            for key, urls in self.urls.items()
        }

        return

    def numUrls(self):
        """Number of URLs to crawl through"""

        return sum([len(self.urls[k]) for k in self.urls])

    def run(self):
        while bool(self.urls):
            self.crawlUrl()
        return

    def loadLinks(self):
        """Load URLs from persisted storage"""

        with open(self.webHunter.LINK_STORE) as links:
            self.urls = json.load(links)

        return

    def clear(self):
        """Clear MegaObtainer caches"""

        self.TEQUILA_DB.clear()
        self.REVIEWER_DB.clear()
        self.COMMUNITY_DB.clear()
        return


class WebHunter(PageHunter):
    index = 0
    db = dict()
    page_num = 0

    SECRET = os.path.join(os.getcwd().replace("scripts", "secrets"), "webHunter.json")

    LINK_STORE = os.path.join(os.getcwd().replace("scripts", "data"), "links.json")

    def __init__(self, idx=None, maxPage=None):
        self.pattern = (
            "https://www.tequilamatchmaker.com/tequilas?q=&hPP=30&idx=BaseProduct&p={}"
        )

        try:
            with open(self.SECRET, "r") as f:
                params = json.load(f)

                self.index = params["idx"]
                maxPage = params["maxPage"]

                f.close()

            print("Proceeding from checkpoint...")
        except json.JSONDecodeError:
            print("Begin fresh site crawl...")
            if idx is not None:
                self.index = idx

        print(
            f"  Index    : {self.index}\n  Page Number: {self.index+1}\n  Max Page : {maxPage}\n"
        )

        self.url = self.pattern.format(self.index)

        super(WebHunter, self).__init__(self.url, maxPage)

        self.pageHunter = PageHunter(self.url, maxPage)

        if maxPage is None:
            self.pageHunter.run(set_max=True)
            self.commit()

            print(
                f"Extracted and committed page {self.index + 1}/{self.pageHunter.maxPage}...\n"
            )

        return

    def getSite(self):
        set_max = False

        for i in range(self.index + 1, self.pageHunter.maxPage):
            print(
                f"Attempting extraction for page {i+1}/{self.pageHunter.maxPage}...\n"
            )

            self.url = self.pattern.format(i)

            if i == (self.pageHunter.maxPage - 1):
                set_max = True

            self.pageHunter.rerun(self.url, set_max)
            self.page_num = i

            if self.pageHunter.cache_empty():
                self.index = i
                break

            self.commit()

            sleep_duration = np.random.randint(low=2, high=10)

            print(f"Sleeping for {sleep_duration} seconds...\n")

            time.sleep(sleep_duration)

            print("Awakening...\n")

        self.index = i
        self.persist()
        return

    def run(self):
        while True:
            self.getSite()

            if self.index + 1 == self.pageHunter.maxPage:
                break

        print("Entire site scraped!")
        return

    def commit(self):
        self.db.update(
            {
                self.page_num: dict(
                    zip(range(len(self.pageHunter.links)), self.pageHunter.links)
                )
            }
        )
        return

    def persist(self, mode="w"):
        # Retrieve and update previously persisted data, if any
        with open(self.LINK_STORE, "r") as f:
            try:
                loaded_dict = json.load(f)
                loaded_dict.update(self.db)

                print("Updating previous dumps...")

            except json.JSONDecodeError:
                loaded_dict = self.db
                print("Start dump...")

            f.close()

        # Persist recent data
        with open(self.LINK_STORE, mode) as f:
            json.dump(loaded_dict, f, indent=4)

        # Persist recent hyperparams
        with open(self.SECRET, "w") as g:
            json.dump(
                {"idx": self.index, "maxPage": self.pageHunter.maxPage}, g, indent=4
            )

        self.clear()

        self.pageHunter.clear()

        print("All links updated! ", "PageHunter instance cleared!\n")
        return

    def clear(self):
        self.db.clear()
        return


# hunter = WebHunter()

# hunter.run()
