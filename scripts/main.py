from classes import PageHunter, Obtainer
from megaclasses import MegaObtainer, WebHunter
from argparse import ArgumentParser

import os
import json

def parse_args(args):

    args.add_argument(
        "--update",
        action="store_true",
        help="Check for updated links?"
    )

    return args


def main():

    args = ArgumentParser()
    args = parse_args(args).parse_args()

    print(f"Parsed args are: {args}")

    if args.update:
        SECRET = os.path.join(os.getcwd().replace("scripts", "secrets"), "webHunter.json")

        with open (SECRET, "r") as f:
            secret = json.load(f)

        max_page, max_index = secret['maxPage'], secret['maxPage'] - 1
        print(max_index)

        webHunter = WebHunter(
            maxPage=max_page
        )

        webHunter.setUrl(page_index=max_index)
        webHunter.pageHunter.setUrl(webHunter.url)

        webHunter.pageHunter.run(set_max=True)

        if max_page < webHunter.pageHunter.maxPage:
            print("New tequila reviews to be had!", "Please run extraction script!", sep='\n')
            webHunter.run()
        else:
            print("No new reviews to be had!")

    else:
        url = "https://www.tequilamatchmaker.com/tequilas/6548-derechito-blanco"
        #obt = Obtainer(url, 0)
        obt = MegaObtainer()

        obt.run()
        #obt.commit()

    return


if __name__ == "__main__":
    main()
