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
    name = 'veeampartner'
    partner_program_link = 'https://www.veeam.com/find-partner.html'
    partner_directory = 'Veeam Partners'
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

    certificates = {
        "VMSP": "Veeam Sales Professional",
        "VMTSP": "Veeam Technical Sales Professional",
        "VMCE": "Veeam Certified Engineer",
        "VMCE-A": "Veeam Certified Architect"}

    start_urls = [partner_program_link]

    def parse(self, response):
        """
        landing function of spider class
        will get search result length and create the search pages and pass it to parse function
        :return:
        """

        def parse_data(data):
            country_code = data['country_code']
            country_name = data['country_name']
            type_code = data['type_code']
            type_name = data['type_name']
            size_code = data['size_code']
            size_name = data['size_name']
            competency_code = data['competency_code']
            competency_name = data['competency_name']
            offset = data['offset']
            page_limit = data['page_limit']

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Referer': 'https://www.veeam.com/find-partner.html?country=AUS',
                'authority': 'www.veeam.com',
                'Connection': 'keep-alive',
                'sec-ch-ua': '"Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
                'sec-ch-ua-platform': 'Windows',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                }

            url = f"https://www.veeam.com/services/unified-partners?country={country_code}&state=&type={type_code}&business-size={size_code}&competency={competency_code}&partner-level=&search=&offset={offset}&limit={page_limit}"
            for i in range(3):
                try:
                    response = requests.get(url=url, headers=headers)
                    if response.status_code != 200:
                        self.logger.info(f'ERROR REQUEST STATUS: {response.status_code}, URL: {response.url}')
                        raise Exception
                    else:
                        break
                except:
                    time.sleep(2)
                    self.logger.info('ERROR REQUEST: WILL SLEEP FOR 2 SECONDS')
                    continue
            else:
                response = None

            if response:
                try:
                    jdata = json.loads(response.text)
                except:
                    jdata = None

                if jdata and 'items' in jdata:
                    page_partners = jdata['items']
                    self.logger.info(f"Country: {country_name}, Page number: {int(offset / page_limit)}{(', Type: ' + type_name) if type_name != '' else ''}{(', Company_size: ' + size_name) if size_name != '' else ''}{(', Competency: ' + competency_name) if competency_name != '' else ''}, Number of results = {len(page_partners)}")

                    for partner in page_partners:
                        partner_id = partner['propartnerId']
                        if partner_id in partners.keys():
                            item = partners[partner_id]
                        else:
                            # Initialize item
                            item = dict()
                            for k in self.item_fields:
                                item[k] = ''

                            item['partner_program_link'] = self.partner_program_link
                            item['partner_directory'] = self.partner_directory
                            item['partner_program_name'] = self.partner_program_name

                            item['partner_company_name'] = partner['name'] if 'name' in partner and partner['name'] else ''
                            item['partner_tier'] = partner['partnership'] if 'partnership' in partner and partner['partnership'] else ''
                            item['company_description'] = cleanhtml(partner['description']) if 'description' in partner and partner['description'] else ''

                            item['general_email_address'] = partner['generalEmail'] if 'generalEmail' in partner and partner['generalEmail'] else ''
                            item['company_domain_name'] = item['general_email_address'][item['general_email_address'].find('@') + 1:] if item['general_email_address'] != '' else ''
                            try:
                                url_obj = urllib.parse.urlparse(item['company_domain_name'])
                                item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                                x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                                if x:
                                    item['company_domain_name'] = x[-1]
                                if '/' in item['company_domain_name']:
                                    item['company_domain_name'] = item['company_domain_name'][
                                                                  :item['company_domain_name'].find('/')]
                            except Exception as e:
                                print('DOMAIN ERROR: ', e)

                            item['staffing_specializations'] = [' '.join([str(certificate['count']), self.certificates[certificate['type']]]) for certificate in (partner['certificates'] if 'certificates' in partner and partner['certificates'] else list())]

                            item['partner_type'] = list()
                            item['company_size'] = list()
                            item['competencies'] = list()

                            if 'coveredArea' in partner and len(partner['coveredArea']) >= 1:
                                item['headquarters_country'] = [{'country': c['country'], 'states': c['states']} for c in partner['coveredArea']]

                            partners[partner_id] = item

                        if type_name != '':
                            item['partner_type'].append(type_name)

                        if size_name != '':
                            item['company_size'].append(size_name)

                        if competency_name != '':
                            item['competencies'].append(competency_name)

                    # follow next pages
                    if offset == 0 and 'totalSize' in jdata and jdata['totalSize'] > page_limit:
                        pages = math.ceil(jdata['totalSize'] / page_limit)
                        for page in range(1, pages + 1):
                            data['offset'] = page * page_limit
                            parse_data(data)

                    if offset == 0 and jdata['totalSize'] > 0:
                        return True
                    else:
                        return False


        try:
            type_filters = dict()
            size_filters = dict()
            competency_filters = dict()

            soup = BS(response.text, "html.parser")
            inpts = soup.find_all('div', {'class': 'partner-listing__filter-content'})
            for inpt in inpts:
                fltr = inpt.find('input', {'type': 'checkbox', 'value': True})
                if not fltr:
                    continue
                for lbl in inpt.find_all('label', {'class': 'form-checkbox'}, recursive=False):
                    if fltr['name'] == 'type':
                        type_filters[lbl.text.strip()] = lbl.find('input', {'type': 'checkbox', 'value': True})['value']
                    elif fltr['name'] == 'business-size':
                        size_filters[lbl.text.strip()] = lbl.find('input', {'type': 'checkbox', 'value': True})['value']
                    elif fltr['name'] == 'competency':
                        competency_filters[lbl.text.strip()] = lbl.find('input', {'type': 'checkbox', 'value': True})['value']

            countries = list()
            response = requests.get('https://www.veeam.com/services/countries')
            if response.status_code == 200:
                try:
                    j = json.loads(response.text)
                    if len(j) > 0:
                        for country in j:
                            countries.append({'country': country['countryName'].title(), 'code': country['code3']})
                except:
                    self.logger.info(f"ERROR RESPONSE STATUS {response.text}")
            # iterate through countries
            for i in countries:
                data = {"country_code": i['code'], "country_name": i['country'],
                        "type_code": '', "type_name": '',
                        "size_code": '', "size_name": '',
                        "competency_code": '', "competency_name": '',
                        "offset": 0, "page_limit": 50}

                if parse_data(data):
                    for type_name, type_code in type_filters.items():
                        data['offset'] = 0
                        data['type_code'] = type_code
                        data['type_name'] = type_name
                        parse_data(data)

                    data['type_code'] = ''
                    data['type_name'] = ''

                    for size_name, size_code in size_filters.items():
                        data['offset'] = 0
                        data['size_code'] = size_code
                        data['size_name'] = size_name
                        parse_data(data)

                    data['size_code'] = ''
                    data['size_name'] = ''

                    for competency_name, competency_code in competency_filters.items():
                        data['offset'] = 0
                        data['competency_code'] = competency_code
                        data['competency_name'] = competency_name
                        parse_data(data)

        except Exception as e:
            print(e)

        for item in partners.values():
            locations = item['headquarters_country']
            if type(locations) is list and len(locations) > 0:
                for location in locations:
                    item['headquarters_country'] = location['country']
                    item['headquarters_state'] = location['states']
                    yield item
            else:
                yield item
