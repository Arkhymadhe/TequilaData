import json
import os
import time

import pandas as pd
import numpy as np

import requests
from requests_html import HTMLSession
from bs4 import BeautifulSoup

from pprint import pprint


class Obtainer:
    def __init__(self, base_url, idx):
        self.idx = idx if idx else 0
        self.base_url = base_url

        self.base_html = BeautifulSoup(
            requests.get(self.base_url).content,
            "html.parser"
        )

        self.tequilaName = self.base_html.findAll("h1", itemprop="name")[0].text.replace("\n", "")

    def run(self):
        self.reviewerData = self.tequilaReviews()
        self.tequilaData = self.getTequilaDetails()
        self.communityData = self.getCommunityDetails()

        print(
            f"Site for {self.tequilaName} scraped!\n"
        )

        return

    def getReviewerData(self, review_html, name="Tequila"):
        """Pick out the needed data fields from a review."""
        # Get reviwer name and level of seniority

        reviewer_name = review_html.find("div", itemprop="author").text.replace("\n", "")
        reviewer_level = review_html.find("div", class_="comment__user-level").text.replace("\n", "")

        # How many ratings has reviwer provided on site? This is a signal of trustworthiness and expertise
        num_ratings_given = review_html.find("div", class_="comment__user-ratings").text.replace("\n", " ").strip()

        if type(num_ratings_given) == str:
            num_ratings_given = num_ratings_given.split(" ")[0]
            num_ratings_given = int(num_ratings_given) if num_ratings_given.isdigit() else num_ratings_given

        # What rating is provided for this tequila?
        rating = review_html.find("li").text.replace("\n", " ").strip().split(" ")[0]
        rating = int(rating) if rating.isdigit() else rating

        # The review attached to the rating
        review_text = review_html.find('p', class_="card-text", itemprop="description").text.replace("\n", "")
        review_text = review_text[:-1] if review_text[-1] == " " else review_text

        # Does reviewer recommend this tequila?
        recom = review_html.find("span", class_="label").text.replace("\n", "")
        recom = 1 if recom.lower() == "i recommend this" else 0

        reviewer_data = {
            "Reviewer": reviewer_name,
            "Reviewer_Level": reviewer_level,
            "Ratings_Given": num_ratings_given,
            "Tequila": name,
            "Rating": rating,
            "Review": review_text,
            "Recommend": recom,

        }
        return reviewer_data

    def allReviews(self, html, name="Tequila"):
        """Pick out required data fields for all reviews on a page."""

        review_html = html.find_all("div", itemprop="review")

        # print(review_html)

        review_dicts = [self.getReviewerData(el, name) for el in review_html]

        data = dict(
            zip(
                range(self.idx, self.idx + len(review_dicts)),
                review_dicts
            )
        )

        return data, self.idx + len(review_dicts)

    def tequilaReviews(self, ):
        """Pick out required data fields for all reviews, for all review pages."""

        p = 1
        final_reviews = dict()
        soup = 0

        while soup is not None:
            url = self.base_url.split("-")[0] + f"/reviews?page={p}"

            print(f"URL: {url}\n")
            sleep_duration = np.random.randint(low=2, high=8)

            soup = BeautifulSoup(
                requests.get(url).content,
                "html.parser"
            )

            if soup.text == "\n":
                break

            scraped, self.idx = self.allReviews(soup, self.tequilaName)

            print(
                f"Extracted review page {p}...\n"
            )
            final_reviews.update(scraped)

            print(
                f"Sleeping for {sleep_duration} seconds...\n"
            )

            time.sleep(sleep_duration)

            p += 1

        return final_reviews

    def getTequilaDetails(self):
        """Pick out tequila bottle chracteristics."""

        table = self.base_html.findAll("div", itemprop="description")[0]

        ids = table.find_all("th", scope="row")
        ids = list(map(lambda x: x.text.replace("\n", "").replace(":", ""), ids))
        ids = ["Tequila"] + ids

        vals = table.find_all("td")
        vals = list(map(lambda x: x.text.replace("\n", ""), vals))
        vals = list(map(lambda x: x[:-1] if x[-1] == "," else x, vals))

        vals[0] = int(vals[0]) if vals[0].isdigit() else vals[0]
        vals = [self.tequilaName] + vals

        return dict(zip(ids, vals))

    def getCommunityDetails(self, kind="aromas"):

        tequila_data = self.base_html.find_all("div", class_=f"{kind} section")

        flavours = list(
            map(
                lambda x: kind.capitalize() + "_" + x.text,
                tequila_data[0].find_all("div", class_="name")
            )
        )

        flavours_vals = list(
            map(
                lambda x: x.text.replace("\n", ""),
                tequila_data[0].find_all("div", class_="taste")
            )
        )

        flavours_vals = list(map(lambda x: int(x) if x.isdigit() else x, flavours_vals))

        taste_profile = dict(zip(flavours, flavours_vals))

        return taste_profile

    def clear(self):
        self.reviewerData.clear()
        self.tequilaData.clear()

        self.communityData.clear()

        print(
            "All cached data cleared!\n"
        )
        return


class PageHunter:
    def __init__(self, url, maxPage=None):
        self.links = list()
        self.url=url
        self.maxPage = maxPage
        self.session = HTMLSession()
        return

    def getLinks(self):
        response = self.session.get(self.url)
        response.html.render(sleep=1, timeout=10)

        links = response.html.xpath('//*[@id="hits"]/div', first=True)
        links = links.absolute_links

        links = list(links)

        return links, response

    def getMaxPage(self, response):
        num_list = response.html.xpath('//*[@id="pagination"]/div/ul/li')
        num_list = list(map(lambda x: x.text, num_list))
        num_list = list(filter(lambda x: x.isdigit(), num_list))
        num_list = [int(x) for x in num_list]

        self.maxPage = max(num_list)

        return

    def run(self, set_max=False):
        self.links, r = self.getLinks()

        if len(self.links) == 0:
            print(
                "Dry run! Links exhausted!\n"
            )
            return

        print(
            f"Extracted {len(self.links)} links!\n"
        )

        if set_max:
            self.getMaxPage(r)
            print(
                f"New max page number found: Page {self.maxPage}!\n"
            )
        return

    def rerun(self, url, set_max=False):
        self.setUrl(url)
        self.run(set_max)
        return

    def setUrl(self, url):
        self.url = url
        return

    def clear(self):
        self.links.clear()
        return

    def cache_empty(self):
        return True if len(self.links) == 0 else False




