# import needed libraries
import re
import requests
from techpartners.spiders.base_spider import BaseSpider
import scrapy
from bs4 import BeautifulSoup as BS
from urllib.parse import urlparse
import tldextract

class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'cynetpartner'
    site_name = 'Cynet Partner Finder'
    page_link = 'https://www.cynet.com/partners/find-a-partner/'

    item_fields = ['partner_program_link', 'partner_directory', 'partner_program_name', 'partner_company_name',
                   'company_domain_name', 'partner_type', 'partner_tier', 'company_description', 'headquarters_street',
                   'headquarters_city', 'headquarters_state', 'headquarters_zipcode', 'headquarters_country',
                   'locations_street', 'locations_city', 'locations_state', 'locations_zipcode', 'locations_country',
                   'regions', 'languages', 'services', 'solutions', 'products', 'pricing', 'specializations',
                   'categories', 'year_founded', 'general_phone_number', 'general_email_address', 'linkedin_link',
                   'twitter_link', 'facebook_link', 'primary_contact_name', 'integrations', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'certified?', 'industries', 'partner_clients', 'notes']

    def start_requests(self):
        """
        landing function of spider class
        will get search result length and create the search pages and pass it to parse function
        :return:
        """

        try:
            url = "https://www.cynet.com/partners/find-a-partner/"
            response = requests.get(url)
            content = BS(response.text, "html.parser")
            countries = content.find("select", {"id": "partnercountry"}).find_all("option")
            # iterate through search results until length
            for option in countries[1:]:
                url = "https://www.cynet.com/partners/find-a-partner/?region=&country=%s&search=" % option['value']
                yield scrapy.Request(url, callback=self.parse, meta={"country": option.text}, dont_filter=True)

        except Exception as e:
            print(e)

    def parse(self, response):
        """
        parse search page and get results json data
        :param response:
        :return:
        """

        partners = response.xpath("//div[@id='partners']//a[@class='partner']").extract()
        if partners and len(partners) > 0:
            for partner in partners:

                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                soup = BS(partner, "html.parser").find("a")
                link = soup['href']

                # return if the partner don't have a href link.
                if not link or link == '':
                    return
                link = link.replace('%20', '').replace(' ', '').strip()
                url_obj = urlparse(link)

                name = soup.find("img")['alt']
                if name:
                    # delete logo size if exist
                    while re.search(r"\d+\*\d+", name):
                        name = name.replace(re.search(r"\d+\*\d+", name).group(), '')
                    while re.search(r"\d+x\d+", name, re.IGNORECASE):
                        name = name.replace(re.search(r"\d+x\d+", name, re.IGNORECASE).group(), '')
                    # delete digits in paranthes or after hashtag
                    while re.search(r"\(\d*\)|#\d+", name):
                        name = name.replace(re.search(r"\(\d*\)|#\d+", name).group(), '')
                    while re.search(r"\d+\s*pix|\d+\s*pixel|\d+\s*pixels|\d+\s*dpi", name, re.IGNORECASE):
                        name = name.replace(re.search(r"\d+\s*pix|\d+\s*pixel|\d+\s*pixels|\d+\s*dpi", name, re.IGNORECASE).group(), '')
                    # delete some words if exist
                    while re.search(r"unknown|logos|logo|rgb|transparent|background|jpg|jpeg|png|pixel|pixels", name, re.IGNORECASE):
                        name = name.replace(re.search(r"unknown|logos|logo|rgb|transparent|background|jpg|jpeg|png|pixel|pixels", name, re.IGNORECASE).group(), '')

                    name = name.strip().replace('_', ' ').replace('-', ' ').replace('  ', ' ').strip().capitalize()

                    if name == "" or name.isdigit() or 'screen shot' in name.lower():
                        link_tld = tldextract.extract(link)
                        name = link_tld.domain.strip().capitalize()

                else:
                    link_tld = tldextract.extract(link)
                    name = link_tld.domain.strip().capitalize()

                item['partner_program_link'] = 'https://www.cynet.com/partners/find-a-partner/'
                item['partner_directory'] = 'Cynet Partners'
                # item['partner_program_name'] = ''
                item['locations_country'] = response.meta["country"]
                item['partner_company_name'] = name
                item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path

                yield item
