import scrapy
import re
from bs4 import BeautifulSoup


incomingLinks = {}

class EDUSpider(scrapy.Spider):
    name = 'Wiki'

    start_urls = ['https://en.wikipedia.org/wiki/Marvel_Comics']

    def parse(self, response):
        #Change response information to soup context
        soup = BeautifulSoup(response.body, 'html.parser')

        #Get the title and last edit date of the current page
        title = soup.h1.get_text()
        date = response.xpath('//li[@id="footer-info-lastmod"]/text()').get()

        #Get the right table facts of the page
        table = soup.table
        if table == None:
            return
        facts = soup.table.get_text()

        #Get the external website of the page
        websiteList = response.css('tr span.url a::text').getall()
        website = ""
        for part in websiteList:
            website = website + part

        #Get all the paragraphs in the page
        p = table.find_all_next("p")
        psoup = BeautifulSoup(str(p),features="lxml")

        #count the number of outgoing links
        outGoingLinksList = []
        outGoingLinks = 0
        for link in psoup.find_all('a'):
            url = str(link.get('href'))
            if re.search(r'\/wiki\/[0-9A-Za-z_-]+$',url):
                outGoingLinksList.append(url)
                outGoingLinks = outGoingLinks + 1
                if url in incomingLinks:
                    incomingLinks[url] = incomingLinks[url] + 1
                else:
                    incomingLinks.update({url:1})

        url = response.request.url
        short_url = url.replace('https://en.wikipedia.org','')
        if short_url in incomingLinks:
            incomingLinks[short_url] = incomingLinks[short_url] + 1
        else:
            incomingLinks.update({short_url:1})
        #yield (store) all the information
        yield {
            'title': title,
            'lastEdit': date,
            'url': response.request.url,
            'facts': facts,
            'website': website,
            'outGoingLinks': outGoingLinks,
            'outGoingLinksList': outGoingLinksList,
            'incomingLinks': incomingLinks[short_url],
            'description' : psoup.get_text()
        }

        #Get all links within the paragraphs and schedule the correct incomingLinks
        # with the following format "http://en.wikipedia.org/wiki/<page here>"
        for link in psoup.find_all('a'):
            url = str(link.get('href'))
            if re.search(r'\/wiki\/[0-9A-Za-z_-]+$',url):
                yield response.follow(url,self.parse)
