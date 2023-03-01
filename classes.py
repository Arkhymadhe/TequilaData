import json
import time
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

from pprint import pprint


class Obtainer:
    def __init__(self, base_url, idx):
        self.base_url = base_url
        self.base_html = BeautifulSoup(
            requests.get(self.base_url).content,
            "html.parser"
        )

        self.tequilaName = self.base_html.findAll("h1", itemprop="name")[0].text.replace("\n", "")

        self.idx = idx if idx else 0

        self.reviewerData = self.tequilaReviews()
        self.tequilaData = self.getTequilaDetails()
        self.communityData = self.getCommunityDetails()

    def getReviewerData(self, review_html, name="Tequila"):
        """Pick out the needed data fields from a review."""
        # Get reviewer name and level of seniority

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

        name = self.base_html.findAll("h1", itemprop="name")[0].text
        name = name.replace("\n", "")

        d = self.base_html.findAll("div", itemprop="description")[0]

        ids = d.find_all("th", scope="row")
        ids = list(map(lambda x: x.text.replace("\n", "").replace(":", ""), ids))
        ids = ["Tequila"] + ids

        vals = d.find_all("td")
        vals = list(map(lambda x: x.text.replace("\n", ""), vals))
        vals = list(map(lambda x: x[:-1] if x[-1] == "," else x, vals))
        vals[0] = int(vals[0]) if vals[0].isdigit() else vals[0]
        vals = [name] + vals

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