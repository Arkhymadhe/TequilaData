from requests_html import HTMLSession, AsyncHTMLSession
import pandas as pd, json

SITE = "https://www.tequilamatchmaker.com/tequilas?q=&hPP=30&idx=BaseProduct&p=1"

sess = HTMLSession()

r = sess.get(SITE)

r.html.render(sleep=1)

print(r.status_code)

products = r.html.xpath('//*[@id="hits"]/div', first=True)

def getPageNums(r):
    l_nums = r.html.xpath('//*[@id="pagination"]/div/ul/li')
    return list(map(lambda x: x.text, l_nums))


def to_numeric(r):
    num_list = getPageNums(r)
    num_list = list(filter(lambda x: x.isdigit(), num_list))
    num_list = [int(x) for x in num_list]
    return num_list


#print(to_numeric(r))
#print(products.absolute_links)

df = pd.read_json('links.json').T

print(df.head())

