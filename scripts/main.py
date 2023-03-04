from classes import PageHunter, Obtainer


def main():
    url = "https://www.tequilamatchmaker.com/tequilas/2328-fortaleza-anejo"
    obt = Obtainer(url, 0)

    obt.run()
    obt.commit()
    return


if __name__ == "__main__":
    main()
