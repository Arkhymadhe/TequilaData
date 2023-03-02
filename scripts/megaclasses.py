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
    def __init__(
            self,
            index=0,
            page_num=0,
            urls=None
    ):
        if urls is None:
            urls = list()
            self.webHunter = WebHunter()
            self.webHunter.run()

            end = False


        self.index = index
        self.urls = urls
        self.obtainers = list()
        self.obtainer = Obtainer()

        return

    def getChildren(self):

        return children

    def commit(self, path=None, mode="w", review_fname="tequila-reviews.json",
               tequila_fname="tequila-details.json"):
        # Dump reviewer data

        if path is not None:
            review_fname = os.path.join(path, review_fname)
            tequila_fname = os.path.join(path, tequila_fname)

        with open(review_fname, mode) as f, open(tequila_fname, mode) as g:
            json.dump(self.obtainer.reviewerData, f)
            json.dump(self.obtainer.communityData, g)

            f.close()
            g.close()

        return


class WebHunter(PageHunter):
    index = 0
    allLinks = dict()
    page_num = 0

    def __init__(self, idx=None, maxPage=None):
        self.pattern = "https://www.tequilamatchmaker.com/tequilas?q=&hPP=30&idx=BaseProduct&p={}"

        try:
            with open("params.json", 'r') as f:
                params = json.load(f)

                self.index = params['idx']
                maxPage = params['maxPage']

                f.close()

            print(
                "Proceeding from checkpoint..."
            )
        except json.JSONDecodeError:
            print(
                "Begin fresh site crawl..."
            )
            if idx is not None:
                self.index = idx

        print(f"  Index    : {self.index}\n  Page Number: {self.index+1}\n  Max Page : {maxPage}\n")

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

        for i in range(self.index+1, self.pageHunter.maxPage):

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

            print(
                f"Sleeping for {sleep_duration} seconds...\n"
            )

            time.sleep(sleep_duration)

            print(
                "Awakening...\n"
            )

        self.index = i
        self.persist()
        return

    def run(self):

        while True:
            self.getSite()

            if self.index + 1 == self.pageHunter.maxPage:
                break

        print(
            "Entire site scraped!"
        )
        return

    def commit(self):
        self.allLinks.update(
            {
                self.page_num:
                dict(
                    zip(
                        range(len(self.pageHunter.links)),
                        self.pageHunter.links
                    )
                )
            }
        )
        return

    def persist(self, fname="links.json", mode="w"):
        # Retrieve and update previously persisted data, if any
        with open(fname, 'r') as f:

            try:
                loaded_dict = json.load(f)
                loaded_dict.update(self.allLinks)

                print(
                    "Updating previous dumps..."
                )

            except json.JSONDecodeError:
                loaded_dict = self.allLinks
                print(
                    "Start dump..."
                )

            f.close()

        # Persist recent data
        with open(fname, mode) as f:
            json.dump(loaded_dict, f, indent=4)

        # Persist recent hyperparams
        with open("params.json", "w") as g:
            json.dump(
                {
                    "idx": self.index,
                    "maxPage": self.pageHunter.maxPage
                },
                g, indent=4
            )

        self.clear()

        self.pageHunter.clear()

        print(
            "All links updated! ",
            "PageHunter instance cleared!\n"
        )
        return

    def clear(self):
        self.allLinks.clear()
        return


hunter = WebHunter()

hunter.run()

