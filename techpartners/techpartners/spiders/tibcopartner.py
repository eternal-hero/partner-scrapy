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
    name = 'tibcopartner'
    partner_program_link = 'https://www.tibco.com/partners/directory'
    partner_directory = 'Tibco Partners Directory'
    partner_program_name = ''
    crawl_id = None

    item_fields = ['partner_program_link', 'partner_directory', 'partner_program_name', 'partner_company_name',
                   'product_service_name',
                   'company_domain_name', 'partner_type', 'partner_tier', 'company_description',
                   'product_service_description',
                   'headquarters_address',
                   'headquarters_street', 'headquarters_city', 'headquarters_state', 'headquarters_zipcode',
                   'headquarters_country',
                   'locations_address',
                   'locations_street', 'locations_city', 'locations_state', 'locations_zipcode', 'locations_country',
                   'regions', 'languages', 'products', 'services', 'solutions',
                   'pricing_plan', 'pricing_model', 'pricing_plan_description',
                   'pricing', 'specializations', 'categories',
                   'features', 'account_requirements', 'product_package_name', 'year_founded', 'latest_update',
                   'publisher',
                   'partnership_timespan', 'partnership_founding_date', 'product_version', 'product_requirements',
                   'general_phone_number', 'general_email_address',
                   'support_phone_number', 'support_email_address', 'support_link', 'help_link', 'terms_and_conditions',
                   'license_agreement_link', 'privacy_policy_link',
                   'linkedin_link', 'twitter_link', 'facebook_link', 'youtube_link', 'instagram_link', 'xing_link',
                   'primary_contact_name', 'primary_contact_phone_number', 'primary_contact_email',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts',
                   'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Connection': 'keep-alive',
        'Authority': 'www.tibco.com',
        'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'upgrade-insecure-requests': '1',
    }

    def start_requests(self):
        page_number = 0
        yield scrapy.Request(method='GET', url=self.partner_program_link + f'?page={page_number}', callback=self.parse,
                             headers=self.headers, dont_filter=True, meta={'page_number': page_number})

    def parse(self, response):
        page_number = response.meta['page_number']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, RESPONSE: {response.text}')
            return
        soup = BS(response.text, "html.parser")
        try:
            partners = soup.find('div', {'class': 'item-list'}).find('ul', recursive=False).find_all('li', recursive=False)
        except:
            partners = None

        if partners:
            self.logger.info(f'Page: {page_number}, Partners = {len(partners)}')
            for partner in partners:

                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = ''

                item['partner_company_name'] = cleanhtml(partner.find('h4', {'class': 'node-title'}).text)
                item['partner_tier'] = partner.find('div', {'class': 'partner-level'}).text if partner.find('div', {'class': 'partner-level'}) else ''
                item['company_domain_name'] = get_domain_from_url(partner.find('div', {'class': 'field-name-field-website'}).find('a', {'href': True})['href'] if partner.find('div', {'class': 'field-name-field-website'}) and partner.find('div', {'class': 'field-name-field-website'}).find('a', {'href': True}) else '')

                link = partner.find('a', {'class': 'learn-more', 'href': True})['href'] if partner.find('a', {'class': 'learn-more', 'href': True}) else None
                if link:
                    prod_link = 'https://www.tibco.com' + link
                    yield scrapy.Request(prod_link, callback=self.parse_partner, dont_filter=False,
                                         meta={'item': item})
                else:
                    yield item

            # follow next pages
            if soup.find('nav', {'class': 'pager'}) and soup.find('nav', {'class': 'pager'}).find('li', {'class': 'pagination-next'}):
                page_number += 1
                yield scrapy.Request(method='GET', url=self.partner_program_link + f'?page={page_number}',
                                     callback=self.parse,
                                     headers=self.headers, dont_filter=True, meta={'page_number': page_number})

    def parse_partner(self, response):
        item = response.meta['item']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, RESPONSE: {response.text}')
            return
        soup = BS(response.text, "html.parser")
        item['company_description'] = cleanhtml(soup.find('div', {'class': 'field-name-field-description'}).text) if soup.find('div', {'class': 'field-name-field-description'}) else ''
        item['regions'] = [field.text for field in soup.find('div', {'class': 'field-name-field-region'}).find_all('div', {'class': 'field-item'})] if soup.find('div', {'class': 'field-name-field-region'}) else ''
        item['industries'] = [field.text for field in soup.find('div', {'class': 'field-name-field-industries'}).find_all('div', {'class': 'field-item'})] if soup.find('div', {'class': 'field-name-field-industries'}) else ''
        item['products'] = [field.text for field in soup.find('div', {'class': 'field-name-field-products'}).find_all('div', {'class': 'field-item'})] if soup.find('div', {'class': 'field-name-field-products'}) else ''

        yield item
