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
    name = 'alenterprisepartner'
    partner_program_link = 'https://www.al-enterprise.com/en/partner-locator'
    partner_directory = 'Business Partners'
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
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
        'cookie': 'renderCtx={"pageId":"6a6c2bee-7406-4b07-a2a1-b1ff083d220e","schema":"Published","viewType":"Published","brandingSetId":"99b37247-05d1-40c5-bea3-4d5a71bd7c4c","audienceIds":""}; CookieConsentPolicy=0:1; LSKey-c$CookieConsentPolicy=0:1; OptanonConsent=isIABGlobal=false&datestamp=Sun+Aug+28+2022+13:45:45+GMT+0200+(Eastern+European+Standard+Time)&version=6.31.0&hosts=&consentId=79778a18-8e5d-4af5-bf5d-69eea2f53378&interactionCount=0&landingPath=https://www.al-enterprise.com/en/partner-locator&groups=C0002:1,C0004:1,C0005:1,C0001:1,C0003:1; _gcl_au=1.1.1370119932.1661687145; _gid=GA1.2.1406409177.1661687146; _gat_UA-92320976-1=1; _ga_BN553ZC0DM=GS1.1.1661687145.1.0.1661687145.0.0.0; _ga=GA1.1.1614201740.1661687146; _fbp=fb.1.1661687145981.826658523; _hjSessionUser_791604=eyJpZCI6IjYwMWNlY2Q4LThkYjQtNTE3MS1iNzEzLTA3YTMxMmViNTI2NyIsImNyZWF0ZWQiOjE2NjE2ODcxNDYwOTAsImV4aXN0aW5nIjpmYWxzZX0=; _hjFirstSeen=1; _hjSession_791604=eyJpZCI6IjEzZjMwNmZjLWUxMGItNDQxMy04ODYzLTg5ZDEzYjM3NzlkZSIsImNyZWF0ZWQiOjE2NjE2ODcxNDYxMDQsImluU2FtcGxlIjp0cnVlfQ==; _hjAbsoluteSessionInProgress=1; sfdc-stream=!ZHFFjlAhmBpvpxVEHjQuqVfnJ7+GKecuY4RiiDkDlxgUXIwB5vIk0jUEuxDnbnZc0KRcU/VBLzS40T8=; pctrk=690453b9-fafe-40eb-9665-35584be11257',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Host': 'customer.al-enterprise.com',
        'Origin': 'https://customer.al-enterprise.com',
        'Referer': 'https://customer.al-enterprise.com/s/partner-locator?language=en_US&https://www.al-enterprise.com/en/partner-locator=',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'Connection': 'keep-alive',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'X-SFDC-Page-Scope-Id': 'a2ae2fd3-778a-4a98-8af1-eab015c0c518',
        'X-SFDC-Request-Id': '18394700000070b9c8'
    }

    def start_requests(self):
        api_link = 'https://customer.al-enterprise.com/s/sfsites/aura?r=1&other.PartnerLocatorCtrl.getSalesTerritory=1&other.PartnerLocatorCtrl.getSolution=1&other.PartnerLocatorCtrl.getTranslateComponent=1'
        data = 'message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22173%3Ba%22%2C%22descriptor%22%3A%22apex%3A%2F%2FPartnerLocatorCtrl%2FACTION%24getTranslateComponent%22%2C%22callingDescriptor%22%3A%22markup%3A%2F%2Fc%3APartnerLocatorSearch%22%2C%22params%22%3A%7B%22Component_name%22%3A%22PartnerLocatorSearch%22%2C%22language_code%22%3A%22en_US%22%7D%2C%22storable%22%3Atrue%7D%2C%7B%22id%22%3A%22174%3Ba%22%2C%22descriptor%22%3A%22apex%3A%2F%2FPartnerLocatorCtrl%2FACTION%24getSalesTerritory%22%2C%22callingDescriptor%22%3A%22markup%3A%2F%2Fc%3APartnerLocatorSearch%22%2C%22params%22%3A%7B%7D%2C%22storable%22%3Atrue%7D%2C%7B%22id%22%3A%22175%3Ba%22%2C%22descriptor%22%3A%22apex%3A%2F%2FPartnerLocatorCtrl%2FACTION%24getSolution%22%2C%22callingDescriptor%22%3A%22markup%3A%2F%2Fc%3APartnerLocatorSearch%22%2C%22params%22%3A%7B%7D%2C%22storable%22%3Atrue%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22QPQi8lbYE8YujG6og6Dqgw%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22Zj1VcUXqZfCDWZ-Q5LxXcA%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11yCoreCollector%22%3A%228089lZkrpgraL8-V8KZXNw%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fs%2Fpartner-locator%3Flanguage%3Den_US%26https%253A%252F%252Fwww.al-enterprise.com%252Fen%252Fpartner-locator%3D&aura.token=null'
        r = requests.request(method='POST', url=api_link, data=data, headers=self.headers)

        if r and r.status_code == 200:
            try:
                j = json.loads(r.text)
            except:
                j = None

            if j and 'actions' in j:
                for ls in j['actions']:
                    if 'returnValue' in ls and 'label' in ls['returnValue'] and ls['returnValue']['label'] == 'Solution':
                        categories = ls['returnValue']['options']
                        if len(categories) > 0:
                            api_link = 'https://customer.al-enterprise.com/s/sfsites/aura?r=1&other.PartnerLocatorCtrl.getAccounts=1'
                            for category in categories:
                                data = f'message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22296%3Ba%22%2C%22descriptor%22%3A%22apex%3A%2F%2FPartnerLocatorCtrl%2FACTION%24getAccounts%22%2C%22callingDescriptor%22%3A%22markup%3A%2F%2Fc%3APartnerLocatorSearch%22%2C%22params%22%3A%7B%22country%22%3A%22%22%2C%22solution%22%3A%22{urllib.parse.quote(category)}%22%7D%2C%22storable%22%3Atrue%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22QPQi8lbYE8YujG6og6Dqgw%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22Zj1VcUXqZfCDWZ-Q5LxXcA%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11yCoreCollector%22%3A%228089lZkrpgraL8-V8KZXNw%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fs%2Fpartner-locator%3Flanguage%3Den_US%26https%253A%252F%252Fwww.al-enterprise.com%252Fen%252Fpartner-locator%3D&aura.token=null'
                                yield scrapy.Request(method='POST', url=api_link, body=data,
                                                     callback=self.parse, headers=self.headers,
                                                     meta={'category': category})

        else:
            self.logger.info(f'ERROR Request: Status {r.status_code}, Response: {r.text}')

    def parse(self, response):
        category = response.meta['category']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            try:
                j = json.loads(response.text)
            except:
                j = None

            if j and 'actions' in j:
                for ls in j['actions']:
                    if 'returnValue' in ls and 'accs' in ls['returnValue'] and ls['returnValue']['accs']:
                        partners = ls['returnValue']['accs']
                        if len(partners) > 0:
                            for partner in partners:
                                if 'CRD_Name__c' not in partner:
                                    continue

                                # Initialize item
                                item = dict()
                                for k in self.item_fields:
                                    item[k] = ''

                                item['partner_program_link'] = self.partner_program_link
                                item['partner_directory'] = self.partner_directory
                                item['partner_program_name'] = ''

                                partner_id = partner['CRD_Id__c']

                                item['partner_company_name'] = partner['CRD_Name__c']
                                item['headquarters_country'] = partner['Country__c'] if 'Country__c' in partner and partner['Country__c'] else ''
                                item['general_phone_number'] = partner['Phone__c'] if 'Phone__c' in partner and partner['Phone__c'] else ''
                                item['categories'] = category

                                item['company_domain_name'] = partner['Website__c'] if 'Website__c' in partner and partner['Website__c'] else ''
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

                                partner_link = 'https://customer.al-enterprise.com/s/sfsites/aura?r=10&other.PartnerLocatorDetailCtrl.getPlDetail=1&other.PartnerLocatorDetailCtrl.getTranslateComponent=1'
                                data = f'message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22319%3Ba%22%2C%22descriptor%22%3A%22apex%3A%2F%2FPartnerLocatorDetailCtrl%2FACTION%24getTranslateComponent%22%2C%22callingDescriptor%22%3A%22markup%3A%2F%2Fc%3APartnerLocatorDetail%22%2C%22params%22%3A%7B%22Component_name%22%3A%22PartnerLocatorDetail%22%2C%22language_code%22%3A%22en_US%22%7D%2C%22storable%22%3Atrue%7D%2C%7B%22id%22%3A%22320%3Ba%22%2C%22descriptor%22%3A%22apex%3A%2F%2FPartnerLocatorDetailCtrl%2FACTION%24getPlDetail%22%2C%22callingDescriptor%22%3A%22markup%3A%2F%2Fc%3APartnerLocatorDetail%22%2C%22params%22%3A%7B%22crdId%22%3A%22{partner_id}%22%7D%2C%22storable%22%3Atrue%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22QPQi8lbYE8YujG6og6Dqgw%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22Zj1VcUXqZfCDWZ-Q5LxXcA%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11yCoreCollector%22%3A%228089lZkrpgraL8-V8KZXNw%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fs%2Fpartner-locator%3Flanguage%3Den_US%26https%253A%252F%252Fwww.al-enterprise.com%252Fen%252Fpartner-locator%3D&aura.token=null'
                                yield scrapy.Request(method='POST', url=partner_link, body=data,
                                                     callback=self.parse_partner, headers=self.headers,
                                                     meta={'partner_id': partner_id, 'item': item})

    def parse_partner(self, response):
        partner_id = response.meta['partner_id']
        item = response.meta['item']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            try:
                j = json.loads(response.text)
            except:
                j = None

            if j and 'actions' in j:
                for ls in j['actions']:
                    if 'returnValue' in ls and 'crdInfo' in ls['returnValue'] and ls['returnValue']['crdInfo'] and partner_id in ls['returnValue']['crdInfo']:
                        info = ls['returnValue']['crdInfo'][partner_id]
                        specialization_data = ls['returnValue']['crdSpecialization'][partner_id]
                        segment_data = ls['returnValue']['crdSegment'][partner_id]

                        item['headquarters_city'] = info['City__c'] if 'City__c' in info and info['City__c'] else ''
                        item['headquarters_street'] = info['Street__c'] if 'Street__c' in info and info['Street__c'] else ''
                        item['headquarters_zipcode'] = info['PostalCode__c'] if 'PostalCode__c' in info and info['PostalCode__c'] else ''
                        item['company_description'] = cleanhtml(info['Company_Description__c']) if 'Company_Description__c' in info and info['Company_Description__c'] else ''
                        item['regions'] = info['Sales_territory__c'] if 'Sales_territory__c' in info and info['Sales_territory__c'] else ''

                        item['solutions'] = specialization_data
                        item['categories'] = segment_data

                        if len(ls['returnValue']['crdInfo'].keys()) > 1:
                            for key in ls['returnValue']['crdInfo'].keys():
                                if key == partner_id:
                                    continue
                                location = ls['returnValue']['crdInfo'][key]

                                item['locations_country'] = location['Country__c'] if 'Country__c' in location else ''
                                item['locations_city'] = location['City__c'] if 'City__c' in location else ''
                                item['locations_street'] = location['Street__c'] if 'Street__c' in location else ''
                                item['locations_zipcode'] = location['PostalCode__c'] if 'PostalCode__c' in location else ''

                                yield item
                        else:
                            yield item
