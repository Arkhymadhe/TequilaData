import json
import os
import time

import numpy as np

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

    INDEX_ARCHIVE = os.path.join(
        os.getcwd().replace("scripts", "secrets"), "megaObtainerIndexes.json"
    )

    tequila_index = 0
    review_index = 0
    community_index = 0

    REVIEWER_DB = dict()
    TEQUILA_DB = dict()
    COMMUNITY_DB = dict()

    def __init__(self, index=0, urls=None):
        self.webHunter = WebHunter()

        try:
            with open(self.REVIEWER_STORE, "x") as f, open(
                self.TEQUILA_STORE, "x"
            ) as g, open(self.COMMUNITY_STORE, "x") as h:
                pass
        except:
            pass

        try:
            with open(self.INDEX_ARCHIVE, "x") as f, open(
                self.ARCHIVE_STORE, "x"
            ) as g:
                pass
        except:
            pass

        if urls is None:
            try:
                self.loadLinks()
                print(
                    "Loaded links from storage!"
                )

            except json.JSONDecodeError:
                print(
                    "Running WebHunter. Extracting required links..."
                )
                self.webHunter.run()

                self.loadLinks()
                print(
                    "Loaded links from storage!"
                )
        else:
            self.urls = urls

        self.index = index

        # Check for persisted indexes
        try:
            with open(self.INDEX_ARCHIVE, "r") as f:
                indexes = json.load(f)

                self.tequila_index = indexes['tequila_index'] + 1
                self.review_index = indexes['review_index'] + 1
                self.community_index = indexes['community_index'] + 1

                f.close()
        except (json.JSONDecodeError, FileNotFoundError):
            pass

        self.rand_page = np.random.choice(list(self.urls.keys()))
        k1 = np.random.choice(list(self.urls[self.rand_page].keys()))

        self.obtainer = Obtainer(self.urls[self.rand_page][k1], self.review_index)

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
        except (json.JSONDecodeError, FileNotFoundError):
            self.archive = dict()

        return

    def commit(self):
        """Commit data from Obtainer caches to MegaObtainer caches"""

        self.REVIEWER_DB.update(self.obtainer.reviewerData)

        self.TEQUILA_DB.update({self.tequila_index: self.obtainer.tequilaData})

        self.COMMUNITY_DB.update({self.community_index: self.obtainer.communityData})

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
                teq.update(self.TEQUILA_DB)
            except json.JSONDecodeError:
                teq = self.TEQUILA_DB

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

        page_keys = list(self.urls.keys())

        rand_page = np.random.choice(page_keys)
        num_trial = 0

        # Present link and previous link selected must not be on pages that are too far apart.
        # This would not be similar to a human web-browsing pattern, and may lead to flagging
        while abs(int(self.rand_page) - int(rand_page)) > 4:
            rand_page = np.random.choice(page_keys)

            if not bool(self.urls[rand_page]):
                del self.urls[rand_page]
                print(
                    f"Data extracted from all links on page {rand_page}!"
                )
                continue

            num_trial += 1

            if (abs(int(self.rand_page) - int(rand_page)) < 4) or (num_trial > 4):
                break

        self.archive.setdefault(rand_page, dict())
        # Store the randomly selected page from which link will be picked out
        self.rand_page = rand_page

        links = self.urls[self.rand_page]
        num_links = len(links)

        link_index = np.random.choice(list(links.keys()))

        self.obtainer.rerun(links[link_index])

        links_scraped = len(self.archive[self.rand_page])

        print(
            f"Link [{links_scraped+1}/{num_links + links_scraped}] for Page {self.rand_page} scraped!\n"
        )

        self.commit()

        print("Reviews, tequila details, and community characterizations committed!\n")

        # Eliminate scraped link from urls to be visited
        scraped_link = links.pop(link_index)
        self.urls[self.rand_page] = links

        self.persist()

        print("Reviews, tequila details, and community characterizations persisted!\n")

        # Persist updated indexes
        self.persistIndexes()

        # Update indexes
        self.tequila_index += 1
        self.community_index += 1

        # Save it to archived links and persist
        self.archive[self.rand_page][link_index] = scraped_link

        self.persistArchive()

        return

    def persistArchive(self):
        """Persist archived URLS i.e., URLs already crawled through"""

        with open(self.ARCHIVE_STORE, "w") as f:
            json.dump(self.archive, f, indent=4)
            f.close()

        return

    def persistIndexes(self):
        indexes = dict()

        indexes['tequila_index'] = self.tequila_index
        indexes['review_index'] = self.obtainer.idx - 1
        indexes['community_index'] = self.community_index

        with open(self.INDEX_ARCHIVE, "w") as f:
            json.dump(indexes, f, indent=4)
            f.close()
        return

    def trimUrls(self):
        """Trim out redundant URLs"""

        redundant_keys = [
            (k, k_)
            for k, v in self.urls.items()
            for k_ in v
            if (k in self.archive) and (k_ in self.archive[k])
        ]

        for k1, k2 in redundant_keys:
            del(self.urls[k1][k2])

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
    """Mega class which uses [an] instance[s] of the PageHunter[s] class to extract links from multiple pages."""

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

            print("Checkpoint below found...")

        except json.JSONDecodeError:
            print("Begin fresh site crawl...")
            if idx is not None:
                self.index = idx

        print(
            f"  >>> Index       : {self.index}\n  >>> Page Number : {self.index+1}\n  >>> Max Page    : {maxPage}\n"
        )
        print(
            "Crawling operations will resume from Checkpoint as required.\n"
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
        """Extracts all links on multiple Pages"""

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
        """Commit data from PageHunter caches to WebHunter caches"""

        self.db.update(
            {
                self.page_num: dict(
                    zip(range(len(self.pageHunter.links)), self.pageHunter.links)
                )
            }
        )
        return

    def persist(self, mode="w"):
        """Persist data in WebHunter caches to persistent memory"""

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
        """Clear out WebHunter cache"""

        self.db.clear()
        return

