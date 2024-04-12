# import needed libraries
import json
import math
import re
import time

import requests
import scrapy
from techpartners.spiders.base_spider import BaseSpider
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse

partners = dict()


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'splunkpartner'
    partner_program_link = 'https://partners.splunk.com/solutionscatalog'
    partner_directory = 'Splunk Partners'
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

    start_urls = [partner_program_link]

    def parse(self, response):
        """
        landing function of spider class
        will get search result length and create the search pages and pass it to parse function
        :return:
        """

        def parse_data(data):
            fltr_type = data['type']
            fltr_label = data['label']
            fltr_value = data['value']
            page = data['page']

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36',
                'Accept': 'text/html, */*; q=0.01',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Referer': 'https://partners.splunk.com/solutionscatalog/',
                'authority': 'partners.splunk.com',
                'Connection': 'keep-alive',
                'sec-ch-ua': '"Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
                'sec-ch-ua-platform': 'Windows',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'x-newrelic-id': 'VQMAUlRTCBAFUFFRDgMCVw==',
                'x-requested-with': 'XMLHttpRequest',
                }

            url = 'https://partners.splunk.com/solutionscatalog' + f'/search?p={page}'
            if fltr_type != '':
                url += f'&f0={fltr_type}&f0v0={fltr_value}'
            for i in range(3):
                try:
                    response = requests.get(url=url, headers=headers)
                    if response.status_code != 200:
                        self.logger.info(f'ERROR REQUEST STATUS: {response.status_code}, URL: {response.url}')
                        raise Exception
                    else:
                        break
                except:
                    self.logger.info('ERROR REQUEST: WILL SLEEP FOR 2 SECONDS')
                    time.sleep(2)
                    continue
            else:
                response = None

            if response:
                soup = BS(response.text, "html.parser")
                page_partners = soup.find_all('a', {'class': 'partnertcarda', 'href': True})

                self.logger.info(f"Page number: {page}{f', {fltr_type}: {fltr_label}' if fltr_type != '' else ''}, Number of results = {len(page_partners)}")
                for partner in page_partners:
                    partner_data = partner.find_next('div', {'class': 'tooltiptext'})
                    partner_name = partner_data.find('p', {'class': 'titleNametp'}).text
                    partner_link = partner['href']

                    if partner_link in partners.keys():
                        item = partners[partner_link]
                    else:
                        item = None
                        for i in range(3):
                            try:
                                r = requests.get(url='https://partners.splunk.com' + partner_link, headers=headers)
                                if r.status_code != 200:
                                    self.logger.info(f'ERROR REQUEST STATUS: {response.status_code}, URL: {response.url}')
                                    raise Exception
                                else:
                                    # Initialize item
                                    item = dict()
                                    for k in self.item_fields:
                                        item[k] = ''

                                    item['partner_program_link'] = self.partner_program_link
                                    item['partner_directory'] = self.partner_directory
                                    item['partner_program_name'] = self.partner_program_name

                                    item['partner_company_name'] = partner_name
                                    partners[partner_link] = item
                                    break
                            except:
                                self.logger.info(f'ERROR PARTNER REQUEST {i}: {url}')
                                time.sleep(2)
                                continue
                        else:
                            r = None

                        if r and item:
                            partner_soup = BS(r.text, "html.parser")

                            item['company_description'] = cleanhtml(partner_soup.find('p', {'id': 'Locator_BodyContent_MarketplaceLongDescription'}).text) if partner_soup.find('p', {'id': 'Locator_BodyContent_MarketplaceLongDescription'}) else ''

                            item['company_domain_name'] = partner_soup.find('p', {'class': 'websitestyle'}).find('a', {'href': True})['href'] if partner_soup.find('p', {'class': 'websitestyle'}) and partner_soup.find('p', {'class': 'websitestyle'}).find('a', {'href': True}) else ''
                            try:
                                url_obj = urllib.parse.urlparse(item['company_domain_name'])
                                item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                                x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                                if x:
                                    item['company_domain_name'] = x[-1]
                                if '/' in item['company_domain_name']:
                                    item['company_domain_name'] = item['company_domain_name'][:item['company_domain_name'].find('/')]
                            except Exception as e:
                                print('DOMAIN ERROR: ', e)

                            social_div = partner_soup.find('div', {'id': 'socialMedia'})
                            if social_div:
                                socials = social_div.find_all('a', {'href': True})
                                for social in socials:
                                    if 'linkedin' in social['href'].lower():
                                        item['linkedin_link'] = social['href']

                                    if 'twitter' in social['href'].lower():
                                        item['twitter_link'] = social['href']

                                    if 'facebook' in social['href'].lower():
                                        item['facebook_link'] = social['href']

                                    if 'instagram' in social['href'].lower():
                                        item['instagram_link'] = social['href']

                                    if 'youtube' in social['href'].lower():
                                        item['youtube_link'] = social['href']

                                    if 'xing' in social['href'].lower():
                                        item['xing_link'] = social['href']

                            item['specializations'] = [tmp.text.replace('• ', '').strip() for tmp in partner_soup.find_all('span', id=re.compile(r'^Locator_BodyContent_IndustryCap\d+$'))]

                            item['partner_type'] = list()
                            item['partner_tier'] = list()

                            location_div = partner_soup.find('section', id='Locator_BodyContent_LocationsSection')

                            if location_div:
                                headquarter = location_div.find('div', {'id': 'locations'}).find('div', {'class': 'details-address'})
                                if headquarter:
                                    address_lines = list()
                                    for line in headquarter.text.splitlines():
                                        if line.strip() != '':
                                            address_lines.append(line.strip())
                                    if len(address_lines) > 0:
                                        item['headquarters_address'] = cleanhtml(' '.join(address_lines[:-1]))
                                        item['headquarters_country'] = address_lines[-1]
                                        item['regions'] = headquarter.find('p', {'class': re.compile(r'^Regionp.*')}).text if headquarter.find('p', {'class': re.compile(r'^Regionp.*')}) else ''

                                locations = location_div.find('div', {'id': 'locations2'}).find_all('div', {'class': 'details-address'})
                                if len(locations) > 0:
                                    item['locations_country'] = [{'address': cleanhtml(location.find('p', {'class': 'Locationp'}).text if location.find('p', {'class': 'Locationp'}) else ''), 'country': location.find('p', {'class': re.compile(r'^Regionp.*')}).text if location.find('p', {'class': re.compile(r'^Regionp.*')}) else ''} for location in locations]

                            partners[partner_link] = item

                    if item and fltr_type != '' and fltr_type == 'Partner+Type':
                        item['partner_type'].append(fltr_label)

                    elif item and fltr_type != '' and fltr_type == 'Tier':
                        item['partner_tier'].append(fltr_label)

                # follow next pages
                if soup.find('nav', {'aria-label': 'Solutions'}) and soup.find('nav', {'aria-label': 'Solutions'}).find('a', {'aria-label': 'Next'}):
                    data['page'] += 1
                    parse_data(data)

        try:
            filters = [{'type': '', 'label': '', 'value': '', 'page': 0}]

            soup = BS(response.text, "html.parser")
            inpts = soup.find_all('div', {'class': 'checkbox'})
            for inpt in inpts:
                fltr = inpt.find('input', {'type': 'checkbox', 'data-category': True, 'value': True})
                if not fltr:
                    continue
                if fltr['data-category'] == 'Partner Type':
                    filters.append({'type': 'Partner+Type', 'label': inpt.text.strip(), 'value': fltr['value'], 'page': 0})
                elif fltr['data-category'] == 'Tier':
                    filters.append({'type': 'Tier', 'label': inpt.text.strip(), 'value': fltr['value'], 'page': 0})

            for fltr in filters:
                parse_data(fltr)

        except Exception as e:
            print(e)

        for item in partners.values():
            locations = item['locations_country']
            if type(locations) is list and len(locations) > 0:
                for location in locations:
                    item['locations_country'] = location['country']
                    item['locations_address'] = location['address']
                    yield item
            else:
                yield item
