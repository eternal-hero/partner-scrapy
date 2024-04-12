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
    name = 'relativitypartner'
    partner_program_link = 'https://www.relativity.com/partners/'
    partner_directory = 'Relativity Partners Directory'
    partner_program_name = ''
    crawl_id = None

    start_urls = [partner_program_link]

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

    def parse(self, response):
        countries_dict = dict()
        type_dict = dict()
        category_dict = dict()
        certification_dict = dict()

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            soup = BS(response.text, "html.parser")
            countries = [{'key': country['value'], 'value': country.text} for country in soup.find('select', {'title': 'Country'}).find_all('option')]
            for country in countries:
                countries_dict[country['key']] = country['value']

            divs = soup.find_all('select', {'title': True})
            for div in divs:
                if 'Select Type' in div['title']:
                    partner_types = div.find_all('option', {'value': True})
                    for partner_type in partner_types:
                        if partner_type['value'] == '':
                            continue
                        else:
                            type_dict[partner_type['value']] = partner_type.text

                elif 'Select Offerings' in div['title']:
                    category_types = div.find_all('option', {'value': True})
                    for category_type in category_types:
                        if category_type['value'] == '':
                            continue
                        else:
                            category_dict[category_type['value']] = category_type.text

                elif 'Select Certifications' in div['title']:
                    certification_types = div.find_all('option', {'value': True})
                    for certification_type in certification_types:
                        if certification_type['value'] == '':
                            continue
                        else:
                            certification_dict[certification_type['value']] = certification_type.text.replace(' on Staff', '')

            partners = soup.find_all('div', {'class': 'grid-item'})
            print(f'Partners: {len(partners)}')

            for partner in partners:
                partner = partner.find('div', {'data-name': True})
                partner_name = partner['data-name']

                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = ''

                item['partner_company_name'] = partner_name

                item['locations_country'] = list()
                for i in partner['data-country'].split(','):
                    if i in countries_dict.keys():
                        item['locations_country'].append(countries_dict[i])

                item['partner_tier'] = list()
                for i in partner['data-partner'].split(','):
                    if i in type_dict.keys():
                        item['partner_tier'].append(type_dict[i])

                item['integrations'] = list()
                for i in partner['data-offerings'].split(','):
                    if i in category_dict.keys():
                        item['integrations'].append(category_dict[i])

                item['certifications'] = list()
                for i in partner['data-individual'].split(','):
                    if i in certification_dict.keys():
                        item['certifications'].append(certification_dict[i])

                item['company_description'] = partner.find('div', {'class': 'card-details'}).text if partner.find('div', {'class': 'card-details'}) else ''

                partner_link = partner.find('a', {'href': True})['href'] if partner.find('a', {'href': True}) else ''
                if partner_link:
                    if partner_link.startswith('/partners/'):
                        yield scrapy.Request('https://www.relativity.com' + partner_link, callback=self.parse_partner,
                                             dont_filter=True, meta={'item': item})
                    else:
                        item['company_domain_name'] = get_domain_from_url(partner_link)
                        yield item

    def parse_partner(self, response):
        item = response.meta['item']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
            yield item
        else:
            soup = BS(response.text, "html.parser")

            headers = soup.find_all('h3')
            for header in headers:
                if header.text == 'Services':
                    item['services'] = [li.text for li in header.find_next('ul').find_all('li')]

                elif header.text.startswith('Partner Since '):
                    item['partnership_founding_date'] = header.text.replace('Partner Since ', '').strip()

            item['company_domain_name'] = get_domain_from_url(soup.find('a', {'class': 'nobg', 'href': True})['href']) if soup.find('a', {'class': 'nobg', 'href': True}) else ''
            item['company_description'] = ' '.join([cleanhtml(p.text) for p in soup.find('div', {'class': 'mura-body'}).find_all('p')] if soup.find('div', {'class': 'mura-body'}) else list())

            locations = soup.find_all('div', {'class': 'datacenter'})
            if len(locations) > 0:
                for location in locations:
                    item['primary_contact_name'] = ''
                    item['primary_contact_email'] = ''

                    div = location.find('div', {'class': 'inner'})
                    country = div.find('h4').text
                    country = country[country.rfind('-')+1:]
                    # if ',' in country:
                    #     item['locations_city'] = country[:country.rfind(',')]
                    #     item['locations_country'] = country[country.rfind(',')+1:]
                    # else:
                    #     item['locations_country'] = country
                    item['locations_address'] = country

                    lines = div.find('p').find_all(text=True, recursive=False) if div.find('p') else list()
                    for txt in lines:
                        if item['primary_contact_name'] == '' and txt.strip() != '' and txt.strip()[0].isalpha():
                            item['primary_contact_name'] = txt.strip()
                            break

                    links = div.find_all('a')
                    for link in links:
                        if item['primary_contact_email'] == '' and '@' in link['href']:
                            item['primary_contact_email'] = link['href'].replace('mailto:', '')
                            break
                    yield item
            else:
                yield item
