import scrapy
import re
from bs4 import BeautifulSoup


class EDUSpider(scrapy.Spider):
    name = 'Wiki'

    start_urls = ['https://en.wikipedia.org/wiki/Marvel_Comics']

    def parse(self, response):
        # follow links to author pages
        soup = BeautifulSoup(response.body, 'html.parser')

        title = soup.h1.get_text()
        date = response.xpath('//li[@id="footer-info-lastmod"]/text()').get()
        table = soup.table

        if table == None:
            return

        facts = soup.table.get_text()

        websiteList = response.css('tr span.url a::text').getall()
        website = ""
        for part in websiteList:
            website = website + part


        p = table.find_all_next("p")
        psoup = BeautifulSoup(str(p),features="lxml")
        yield {
            'title': title,
            'lastEdit': date,
            'facts': facts,
            'website': website,
            'description' : psoup.get_text()
        }

        for link in psoup.find_all('a'):
            url = str(link.get('href'))
            if re.search(r'\/wiki\/[0-9A-Za-z_-]+$',url):
                yield response.follow(url,self.parse)
