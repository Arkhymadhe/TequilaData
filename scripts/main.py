from classes import PageHunter, Obtainer
from megaclasses import MegaObtainer


def main():
    url = "https://www.tequilamatchmaker.com/tequilas/6548-derechito-blanco"
    #obt = Obtainer(url, 0)
    obt = MegaObtainer()

    obt.run()
    #obt.commit()
    return


if __name__ == "__main__":
    main()
