# import needed libraries
import datetime
import math
import re
import json
import time
import scrapy
from techpartners.spiders.base_spider import BaseSpider
from techpartners.functions import *
from bs4 import BeautifulSoup as BS
import urllib.parse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'ibmpartner'
    site_name = 'IBM Partner Directory'
    page_link = 'https://www.ibm.com/partnerworld/bpdirectory/'

    item_fields = ['partner_program_link', 'partner_directory', 'partner_program_name', 'partner_company_name', 'product_service_name',
                   'company_domain_name', 'partner_type', 'partner_tier', 'company_description', 'product_service_description',
                   'headquarters_street', 'headquarters_city', 'headquarters_state', 'headquarters_zipcode', 'headquarters_country',
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
                   'primary_contact_name', 'primary_contact_phone_number',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts', 'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    def start_requests(self):
        """
        landing function of spider class
        will get search result length and create the search pages and pass it to parse function
        :return:
        """
        self.geography = dict()
        classification_link = 'https://www.ibm.com/partnerworld/bpdirectory/api/catalog/businessPartners/all'
        response = requests.get(classification_link, headers={'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"})
        if response.status_code == 200:
            try:
                data = json.loads(response.text)
                if 'geography' in data:
                    for region in data['geography']:
                        for country in region['catalogs']:
                            self.geography[country['id']] = {'region': region['label'], 'country': country['label']}
            except Exception as e:
                print('Region Exception: ', e)

        startindex = 1
        numresults = 100
        totalresults = None

        while not totalresults or startindex < totalresults:
            data = None
            api_link = f'https://www.ibm.com/partnerworld/bpdirectory/api/businessPartner/retrieveBusinessPartners?search=&startindex={startindex}&numresults={numresults}&sort=relevance&partner_level=PLT+++,PRE+++,ADV+++,MEM+++'
            print(startindex)
            response = requests.get(api_link, headers={'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"})
            if response.status_code == 200:
                startindex += numresults
                try:
                    data = json.loads(response.text)
                    if 'totalresults' in data:
                        totalresults = data['totalresults']
                except Exception as e:
                    print('EXCEPTION: ', e)

            else:
                print(response.status_code)
                print(response.text)

            if data and 'businessPartners' in data:
                for partner in data['businessPartners']:
                    companyId = partner['companyId']
                    partner_link = 'https://www.ibm.com/partnerworld/bpdirectory/api/businessPartner/retrieveBusinessPartnerDetailed/' + companyId
                    yield scrapy.Request(partner_link, callback=self.parse_partner)

    def parse_partner(self, response):
        """
        parse partner page and get result json data
        :param response:
        :return:
        """
        if response.status == 200:
            pass
        elif response.status == 403:
            self.logger.info('*'*60)
            self.logger.info('ERROR PARSE PARTNER')
            self.logger.info(response.status)
            self.logger.info('*' * 60)
            yield scrapy.Request(response.request.url, callback=self.parse_partner, dont_filter=True,
                                 meta={'trial': response.meta['trial']})
            return
        else:
            self.logger.info('*'*60)
            self.logger.info('ERROR PARSE PARTNER')
            self.logger.info(response.status)
            self.logger.info('*' * 60)
            if response.meta['trial'] == 1:
                yield scrapy.Request(response.request.url, callback=self.parse_partner, dont_filter=True,
                                     meta={'trial': 2})
            return

        # Initialize item
        item = dict()
        for k in self.item_fields:
            item[k] = ''

        partner = response.json()

        item['partner_program_link'] = self.page_link
        item['partner_directory'] = self.site_name

        item['partner_company_name'] = partner['bpName'] if 'bpName' in partner else ''
        item['company_domain_name'] = partner['websiteURL'] if 'websiteURL' in partner else ''
        if item['company_domain_name'] and item['company_domain_name'] != '':
            url_obj = urllib.parse.urlparse(item['company_domain_name'])
            item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
            x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
            if x:
                item['company_domain_name'] = x[-1]

        item['partner_tier'] = partner['bpLevel'] if 'bpLevel' in partner else ''
        item['company_description'] = partner['description'] if 'description' in partner else ''
        if 'partnerTypes' in partner:
            item['partner_type'] = list()
            for itm in partner['partnerTypes']:
                item['partner_type'].append(itm['ptDescription'])

        if 'address' in partner:
            item['headquarters_street'] = partner['address']['street'] if 'street' in partner['address'] else ''
            item['headquarters_city'] = partner['address']['city'] if 'city' in partner['address'] else ''
            item['headquarters_state'] = partner['address']['state'] if 'state' in partner['address'] else ''
            item['headquarters_zipcode'] = partner['address']['zipCode'] if 'zipCode' in partner['address'] else ''
            item['headquarters_country'] = partner['address']['country'] if 'country' in partner['address'] else ''

        if item['headquarters_country'] in self.geography.keys():
            item['regions'] = self.geography[item['headquarters_country']]['region']
            item['headquarters_country'] = self.geography[item['headquarters_country']]['country']

        item['general_phone_number'] = partner['phoneNumber'] if 'phoneNumber' in partner else ''

        if 'resaleAuths' in partner:
            item['categories'] = set()
            for itm in partner['resaleAuths']:
                item['categories'].add(itm['techDescription'])
                if 'subTechDescription' in itm:
                    item['categories'].add(itm['subTechDescription'])
            item['categories'] = list(item['categories'])

        if 'industries' in partner:
            item['industries'] = list()
            for itm in partner['industries']:
                item['industries'].append(itm['industryName'])

        if 'serviceTypes' in partner:
            item['services'] = list()
            for itm in partner['serviceTypes']:
                item['services'].append(itm['name'])

        if 'solutions' in partner:
            item['solutions'] = list()
            for itm in partner['solutions']:
                item['solutions'].append(itm['solutionName'])

        if 'diversities' in partner:
            item['company_characteristics'] = list()
            for itm in partner['diversities']:
                item['company_characteristics'].append(itm['description'])

        if 'competencies' in partner:
            item['competencies'] = list()
            for itm in partner['competencies']:
                item['competencies'].append({'Category': itm['category'], 'Competency': itm['areaSpecDesc'], 'Competency level': itm['competencyLevel']})

        try:
            certifications_link = 'https://www.ibm.com/partnerworld/bpdirectory/api/businessPartner/retrieveCredentials/' + partner['id']
            response = requests.get(certifications_link, headers={'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"})
            if response.status_code == 200:
                data = json.loads(response.text)
                if len(data) > 0:
                    item['certifications'] = list()
                    for itm in data:
                        item['certifications'].append({'Competency': itm['subCompetency'],
                                                       'Credential': itm['certDescription'],
                                                       'Number of credentials': itm['credNumber']})
        except Exception as e:
            print('Failed to obtain credentials')
            print(e)
        yield item
