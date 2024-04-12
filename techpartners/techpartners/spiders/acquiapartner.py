# import needed libraries
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
    name = 'acquiapartner'
    partner_program_link = 'https://www.acquia.com/partners/finder'
    partner_directory = 'acquia Partner'
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
        'authority': 'www.acquia.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',
        'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
    }

    def start_requests(self):
        page_number = 0
        yield scrapy.Request(method='GET', url=f'https://www.acquia.com/partners/finder?page={page_number}',
                             callback=self.parse,
                             headers=self.headers,
                             meta={'page_number': page_number}
                             )

    def parse(self, response):
        page_number = response.meta['page_number']
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            partners = soup.find_all('div', {'class': 'views-row'})
            self.logger.info(f'Acquia Page: {page_number}, has partners = {len(partners)}')

            for partner in partners:

                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = self.partner_program_name

                item['partner_company_name'] = partner.find('span', {'class': 'ct-partner__content__title'}).text.strip()
                item['partner_tier'] = partner.find('span', {'class': 'ct-partner__content__level'}).text.strip() if partner.find('span', {'class': 'ct-partner__content__level'}) else ''
                item['certifications'] = [div.text.strip() for div in (partner.find('span', {'class': 'ct-partner__content__badge'}).find_all('div') if partner.find('span', {'class': 'ct-partner__content__badge'}) else list()) if not div.find('div')]

                partner_url = 'https://www.acquia.com' + partner.find('a', {'href': True})['href']
                yield scrapy.Request(url=partner_url, headers=self.headers, callback=self.parse_partner,
                                     meta={'item': item})

            # follow next page
            if soup.find('nav', {'class': 'pager'}) and soup.find('nav', {'class': 'pager'}).find('a', {'rel': 'next'}):
                page_number += 1
                yield scrapy.Request(method='GET', url=f'https://www.acquia.com/partners/finder?page={page_number}',
                                     callback=self.parse,
                                     headers=self.headers,
                                     meta={'page_number': page_number}
                                     )

    def parse_partner(self, response):
        item = response.meta['item']
        if response.status != 200:
            self.logger.info(f'ERROR PARSE_PARTNER STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")

            item['company_description'] = cleanhtml(soup.find('div', {'class': 'ct-partner__content'}).text) if soup.find('div', {'class': 'ct-partner__content'}) else ''
            item['company_domain_name'] = get_domain_from_url(soup.find('div', {'class': 'ct-partner__website'}).find('a', {'href': True})['href'] if soup.find('div', {'class': 'ct-partner__website'}) else '')
            item['regions'] = [itm.text.strip() for itm in (soup.find('div', {'class': 'ct-partner__highlights__regions'}).find_all('div', {'class': 'value'}) if soup.find('div', {'class': 'ct-partner__highlights__regions'}) else list())]

        yield item
