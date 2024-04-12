# import needed libraries
from techpartners.spiders.base_spider import BaseSpider
import scrapy
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'klipfoliopartner'
    partner_program_link = 'https://www.klipfolio.com/partners/directory'
    partner_directory = 'Klipfolio Partner Directory'
    partner_program_name = ''
    crawl_id = 1347

    start_urls = [partner_program_link]

    item_fields = ['partner_program_link', 'partner_directory', 'partner_program_name', 'partner_company_name',
                   'company_domain_name', 'partner_type', 'partner_tier', 'company_description', 'headquarters_street',
                   'headquarters_city', 'headquarters_state', 'headquarters_zipcode', 'headquarters_country',
                   'locations_street', 'locations_city', 'locations_state', 'locations_zipcode', 'locations_country',
                   'regions', 'languages', 'services', 'solutions', 'products', 'pricing', 'specializations',
                   'categories', 'year_founded', 'partnership_timespan', 'general_phone_number', 'general_email_address',
                   'use_case', 'integrations', 'contract_vehicles', 'company_characteristics', 'linkedin_link',
                   'twitter_link', 'facebook_link', 'primary_contact_name', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'certified?', 'industries', 'partner_clients', 'notes']

    def parse(self, response):
        """
        parse item api and get results json data
        :param response:
        :return:
        """
        links = response.xpath('//div[@class = "partner-directory"]//a/@href').getall()
        for link in links:
            if link.startswith('/partner'):
                prod_link = 'https://www.klipfolio.com' + link
                yield scrapy.Request(prod_link, callback=self.parse_partner)

    def parse_partner(self, response):
        """
        parse partner page and get result json data
        :param response:
        :return:
        """

        # Initialize item
        item = dict()
        for k in self.item_fields:
            item[k] = ''

        item['partner_program_link'] = self.partner_program_link
        item['partner_directory'] = self.partner_directory
        item['partner_program_name'] = self.partner_program_name

        item['partner_company_name'] = response.xpath('//meta[@property="og:title"]/@content').get() if response.xpath('//meta[@property="og:title"]/@content') else ''

        item['company_description'] = re.sub('.kpi-list\s*\{.+}', '', cleanhtml(response.xpath('//meta[@property="og:description"]/@content').get())) if response.xpath('//meta[@property="og:description"]/@content') else ''

        data = response.xpath('//div[@class="region region-content"]').get()
        soup = BS(data, "html.parser")

        items = soup.find_all('h4')
        for elem in items:
            if elem.text == 'Country':
                item['headquarters_country'] = elem.next.next.text.strip() if elem.next.next and elem.next.next.text else ''
            elif elem.text == 'Certified':
                item['certified?'] = elem.next.next.text.strip() if elem.next.next and elem.next.next.text else ''
            elif elem.text == 'Global':
                item['regions'] = 'Global' if elem.next.next and elem.next.next.text.strip() == 'Yes' else ''
            elif elem.text == 'Phone':
                item['general_phone_number'] = elem.next.next.text.strip() if elem.next.next and elem.next.next.text != '' else ''
            elif elem.text == 'Email':
                item['general_email_address'] = elem.findNext('a', href=True).text
            elif elem.text == 'Website':
                # item['company_domain_name'] = elem.findNext('a', href=True).text
                url_obj = urllib.parse.urlparse(elem.findNext('a', href=True).text)
                item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                if x:
                    item['company_domain_name'] = x[-1]

                if '/' in item['company_domain_name']:
                    item['company_domain_name'] = item['company_domain_name'][:item['company_domain_name'].find('/')]

        for a in soup.find_all('a', href=True):
            link = a['href']
            if 'twitter.com' in link:
                item['twitter_link'] = link
            elif 'linkedin.com' in link:
                item['linkedin_link'] = link
            elif 'facebook.com' in link:
                item['facebook_link'] = link

        while item['twitter_link'].startswith('/'):
            item['twitter_link'] = item['twitter_link'].lstrip('/')
        while item['linkedin_link'].startswith('/'):
            item['linkedin_link'] = item['linkedin_link'].lstrip('/')
        while item['facebook_link'].startswith('/'):
            item['facebook_link'] = item['facebook_link'].lstrip('/')

        yield item
