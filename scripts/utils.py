import random
import os
import json


def getRandomHeaders():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0",
        "Mozilla/5.0 (Windows NT 10.0; rv:78.0) Gecko/20100101 Firefox/78.0",
        "Mozilla/5.0 (X11; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0",
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36 RuxitSynthetic/1.0 v21927832117 t3426260252853334511 athfa3c3975 altpub cvcv=2 smf=0"
    ]

    random_user_agent = random.choice(user_agents)

    headers = {
        'User-Agent': random_user_agent
    }

    return headers


def getArchiveSize():

    PATH = os.path.join(
        os.getcwd().replace("scripts", "secrets"), "megaObtainerArchive.json"
    )

    with open(PATH, "r") as f:
        d = json.load(f)

    size = sum(
        [
            len(v) for (k, v) in d.items()
        ]
    )

    print(
        f"Archive contains {size} links..."
    )

    return size
