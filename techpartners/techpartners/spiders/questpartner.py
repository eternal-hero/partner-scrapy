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
    name = 'questpartner'
    partner_program_link = 'https://partners.quest.com/en-US/directory/'
    partner_directory = 'Quest Partner'
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

    headers = {
        'authority': 'partners.quest.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',
        'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
    }

    start_urls = [partner_program_link]

    type_dict = dict()
    region_dict = dict()

    def start_requests(self):
        response = requests.get(self.partner_program_link, headers=self.headers)
        soup = BS(response.text, "html.parser")

        type_list_options = {optn['value']: optn.text.strip() for optn in soup.find('select', id='typeList').find_all('option', {'value': True}) if optn['value'] != 'all'}
        region_list_options = {optn['href']: optn['href'][optn['href'].rfind('=') + 1:].strip() for optn in soup.find_all('a', {'href': re.compile(r"^search\?f0=Region&f0v0=")})}

        # get list of type partners
        for type_value, type_text in type_list_options.items():
            self.type_dict[type_text] = set()
            page = 0
            while True:
                type_link = f'https://partners.quest.com/en-US/directory/search?p={page}&type={type_value}'
                response = requests.get(type_link, headers=self.headers)
                soup = BS(response.text, "html.parser")
                partners = soup.find('div', {'class': 'row-results'}).find_all('div', recursive=False)
                self.logger.info(f'QUEST Type {type_text}, PAGE {page+1}, has {len(partners)} PARTNERS')
                for partner in partners:
                    partner_link = partner.find('a', {'role': "button", 'href': True})['href']
                    self.type_dict[type_text].add(partner_link)

                if len(partners) > 0 and soup.find('ul', {'class': 'pagination'}) and soup.find('ul', {'class': 'pagination'}).find('a', {'aria-label': 'Next', 'href': True}):
                    page += 1
                    continue
                else:
                    break

        # get list of region partners
        for region_query, region_text in region_list_options.items():
            self.region_dict[region_text] = set()
            page = 0
            while True:
                region_link = f'https://partners.quest.com/en-US/directory/' + region_query + f'&p={page}'
                response = requests.get(region_link, headers=self.headers)
                soup = BS(response.text, "html.parser")
                partners = soup.find('div', {'class': 'row-results'}).find_all('div', recursive=False)
                self.logger.info(f'QUEST Region {region_text}, PAGE {page+1}, has {len(partners)} PARTNERS')
                for partner in partners:
                    partner_link = partner.find('a', {'role': "button", 'href': True})['href']
                    self.region_dict[region_text].add(partner_link)

                if len(partners) > 0 and soup.find('ul', {'class': 'pagination'}) and soup.find('ul', {'class': 'pagination'}).find('a', {'aria-label': 'Next', 'href': True}):
                    page += 1
                    continue
                else:
                    break

        yield scrapy.Request(url='https://partners.quest.com/en-US/directory/search?p=0',
                             callback=self.parse, meta={'page': 0})

    def parse(self, response):
        page = response.meta['page']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            partners = soup.find('div', {'class': 'row-results'}).find_all('div', recursive=False)
            self.logger.info(f'QUEST, PAGE {page + 1}, has {len(partners)} PARTNERS')

            for partner in partners:
                partner_link = partner.find('a', {'role': "button", 'href': True})['href']
                yield scrapy.Request(url='https://partners.quest.com' + partner_link, callback=self.parse_partner,
                                     headers=self.headers, meta={'partner_link': partner_link})

            # follow next page
            if len(partners) > 0 and soup.find('ul', {'class': 'pagination'}) and soup.find('ul', {'class': 'pagination'}).find('a', {'aria-label': 'Next', 'href': True}):
                page += 1
                page_link = f'https://partners.quest.com/en-US/directory/search?p={page}'
                yield scrapy.Request(url=page_link, callback=self.parse, meta={'page': page})

    def parse_partner(self, response):
        partner_link = response.meta['partner_link']

        if response.status != 200:
            self.logger.info(f'ERROR PARTNER REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            partner = BS(response.text, "html.parser")

            # Initialize item
            item = dict()
            for k in self.item_fields:
                item[k] = ''

            item['partner_program_link'] = self.partner_program_link
            item['partner_directory'] = self.partner_directory
            item['partner_program_name'] = ''

            item['partner_company_name'] = partner.find('h1').text
            item['partner_tier'] = partner.find('span', {'itemprop': 'gradCap'}).text.strip().replace('Partner Tier:', '').strip() if partner.find('span', {'itemprop': 'gradCap'}) else ''
            item['general_phone_number'] = partner.find('span', {'itemprop': 'telephone'}).text.strip() if partner.find('span', {'itemprop': 'telephone'}) else ''
            item['company_domain_name'] = get_domain_from_url(partner.find('div', {'class': 'row'}).find('a', {'href': True, 'target': True}).text.strip() if partner.find('div', {'class': 'row'}) and partner.find('div', {'class': 'row'}).find('a', {'href': True, 'target': True}) else '')

            item['headquarters_country'] = partner.find('span', {'itemprop': 'addressCountry'}).text.strip() if partner.find('span', {'itemprop': 'addressCountry'}) else ''
            item['headquarters_zipcode'] = partner.find('span', {'itemprop': 'postalCode'}).text.strip() if partner.find('span', {'itemprop': 'postalCode'}) else ''
            item['headquarters_state'] = partner.find('span', {'itemprop': 'addressRegion'}).text.strip() if partner.find('span', {'itemprop': 'addressRegion'}) else ''
            item['headquarters_city'] = partner.find('span', {'itemprop': 'addressLocality'}).text.strip() if partner.find('span', {'itemprop': 'addressLocality'}) else ''
            item['headquarters_street'] = partner.find('span', {'itemprop': 'streetAddress'}).text.strip() if partner.find('span', {'itemprop': 'streetAddress'}) else ''

            if partner.find('section', id='solutions'):
                item['solutions'] = partner.find('section', id='solutions').find('p').text.strip() if partner.find('section', id='solutions').find('p') else ''

            if partner.find('section', id='services'):
                item['services'] = partner.find('section', id='services').find('p').text.strip() if partner.find('section', id='services').find('p') else ''

            item['partner_type'] = list()
            for type_text in self.type_dict:
                if partner_link in self.type_dict[type_text]:
                    item['partner_type'].append(type_text)

            item['regions'] = list()
            for region_text in self.region_dict:
                if partner_link in self.region_dict[region_text]:
                    item['regions'].append(region_text)

            yield item
