# import needed libraries
import datetime
import json
import math
import re

import requests
import scrapy
from techpartners.spiders.base_spider import BaseSpider
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'itronpartner'
    partner_program_link = 'https://partner.itron.com/English/directory/'
    partner_directory = 'ITron Partner'
    partner_program_name = ''
    crawl_id = None

    item_fields = ['partner_program_link', 'partner_directory', 'partner_program_name', 'partner_company_name', 'product_service_name',
                   'company_domain_name', 'partner_type', 'partner_tier', 'company_description', 'product_service_description',
                   'headquarters_address',
                   'headquarters_street', 'headquarters_city', 'headquarters_state', 'headquarters_zipcode', 'headquarters_country',
                   'locations_address',
                   'locations_street', 'locations_city', 'locations_state', 'locations_zipcode', 'locations_country',
                   'regions', 'languages', 'products', 'services', 'solutions',
                   'pricing_plan', 'pricing_model', 'pricing_plan_description',
                   'pricing', 'specializations', 'categories',
                   'features', 'account_requirements', 'product_package_name', 'year_founded', 'latest_update', 'publisher',
                   'partnership_timespan', 'partnership_founding_date', 'product_version', 'product_requirements',
                   'general_phone_number', 'general_email_address',
                   'support_phone_number', 'support_email_address', 'support_link', 'help_link', 'terms_and_conditions',
                   'license_agreement_link', 'privacy_policy_link',
                   'linkedin_link', 'twitter_link', 'facebook_link', 'youtube_link', 'instagram_link', 'xing_link',
                   'primary_contact_name', 'primary_contact_phone_number', 'primary_contact_email',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts', 'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    def start_requests(self):
        partner_types = {'Technology Partner': 'https://partner.itron.com/English/directory/search?f0=Partner+Type&f0v0=Technology+Partners',
                         'Sales Channel Partner': 'https://partner.itron.com/English/directory/search?f0=Partner+Type&f0v0=Sales+Channel+Partners'}
        for partner_type, type_link in partner_types.items():
            yield scrapy.Request(type_link, callback=self.parse,
                                 dont_filter=True,
                                 meta={'partner_type': partner_type, 'type_link': type_link, 'page': 0})

    def parse(self, response):
        page = response.meta['page']
        partner_type = response.meta['partner_type']
        type_link = response.meta['type_link']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            partners = soup.find('div', {'class': 'row-results'}).find_all('div', recursive=False)
            self.logger.info(f'Scrape {partner_type}, page= {page}, partners= {len(partners)}')

            for partner in partners:

                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = ''

                item['partner_company_name'] = partner.find('h3').text
                item['partner_type'] = partner_type
                item['partner_tier'] = partner.find('small').text if partner.find('small') else ''

                partner_link = 'https://partner.itron.com' + partner.find('a', {'href': True})['href']

                yield scrapy.Request(partner_link, callback=self.parse_partner,
                                     dont_filter=True,
                                     meta={'item': item})

            # follow next page
            if len(partners) > 0:
                page += 1
                page_link = type_link + f'&p={page}'
                yield scrapy.Request(page_link, callback=self.parse,
                                     dont_filter=True,
                                     meta={'partner_type': partner_type, 'type_link': type_link, 'page': page})

    def parse_partner(self, response):
        item = response.meta['item']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")

            item['headquarters_street'] = soup.find('span', {'itemprop': 'streetAddress'}).text if soup.find('span', {'itemprop': 'streetAddress'}) else ''
            item['headquarters_city'] = soup.find('span', {'itemprop': 'addressLocality'}).text if soup.find('span', {'itemprop': 'addressLocality'}) else ''
            item['headquarters_state'] = soup.find('span', {'itemprop': 'addressRegion'}).text if soup.find('span', {'itemprop': 'addressRegion'}) else ''
            item['headquarters_zipcode'] = soup.find('span', {'itemprop': 'postalCode'}).text if soup.find('span', {'itemprop': 'postalCode'}) else ''
            item['headquarters_country'] = soup.find('span', {'itemprop': 'addressCountry'}).text if soup.find('span', {'itemprop': 'addressCountry'}) else ''

            item['company_domain_name'] = get_domain_from_url(soup.find('div', {'class': 'user-website'}).find('a', {'href': True})['href']) if soup.find('div', {'class': 'user-website'}) and soup.find('div', {'class': 'user-website'}).find('a', {'href': True}) else ''

            item['company_description'] = cleanhtml(soup.find('p', id='Locator_BodyContent_MarketplaceLongDescription').text) if soup.find('p', id='Locator_BodyContent_MarketplaceLongDescription') else ''

            item['general_phone_number'] = soup.find('a', {'href': re.compile(r"tel:")})['href'].replace('tel:', '').strip() if soup.find('a', {'href': re.compile(r'tel:')}) else ''

            info_divs = soup.find_all('div', {'class': 'overview-detail'})
            for div in info_divs:
                if div.find('h5') and div.find('h5').text.strip() == 'Industries':
                    item['industries'] = [li.text.strip() for li in div.find_all('li')]

                if div.find('h5') and div.find('h5').text.strip() == 'Products & Solutions':
                    item['products'] = [li.text.strip() for li in div.find_all('li')]

                if div.find('h5') and div.find('h5').text.strip() == 'Technologies & Solutions':
                    item['services'] = [li.text.strip() for li in div.find_all('li')]

                if div.find('h5') and div.find('h5').text.strip() == 'Regions Served':
                    item['regions'] = [[li.text.strip() for li in ul.find_all('li')] for ul in div.find_all('ul', {'class': 'list-unstyled'})]

            yield item
