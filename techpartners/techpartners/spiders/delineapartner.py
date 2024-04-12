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
    name = 'delineapartner'
    partner_program_link = 'https://delinea.com/partners/find-a-partner'
    partner_directory = 'Delinea Partner'
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

        api_link = 'https://thycotic.force.com/support/aura?r=1&other.PartnerDirectoryHandler.grabDirectoryList=1'
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                   'Accept': '*/*',
                   'Accept-Encoding': 'gzip, deflate, br',
                   'Accept-Language': 'en-US,en;q=0.9',
                   'Connection': 'keep-alive',
                   'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                   'Host': 'thycotic.force.com',
                   'Origin': 'https://delinea.com',
                   'Referer': 'https://delinea.com/partners/find-a-partner',
                   'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
                   'sec-ch-ua-mobile': '?0',
                   'sec-ch-ua-platform': '"Windows"',
                   'Sec-Fetch-Dest': 'empty',
                   'Sec-Fetch-Mode': 'cors',
                   'Sec-Fetch-Site': 'cross-site',
                   }

        payload = 'message=%7B%22actions%22%3A%5B%7B%22id%22%3A%229%3Ba%22%2C%22descriptor%22%3A%22apex%3A%2F%2FPartnerDirectoryHandler%2FACTION%24grabDirectoryList%22%2C%22callingDescriptor%22%3A%22markup%3A%2F%2Fc%3ApartnerDirectoryCmp%22%2C%22params%22%3A%7B%22partnerType%22%3A%22all%22%2C%22country%22%3A%22all%22%7D%2C%22version%22%3Anull%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22tr2UlkrAHzi37ijzEeD2UA%22%2C%22app%22%3A%22c%3APartnerDirectoryApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fc%3APartnerDirectoryApp%22%3A%22gfQJlzN2HISOvoj4XE8HWw%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Atrue%7D&aura.pageURI=%2Fpartners%2Ffind-a-partner&aura.token=null'
        yield scrapy.Request(method='POST', url=api_link, headers=headers, body=payload, callback=self.parse)

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            if 'actions' in response.text:
                partners = json.loads(json.loads(response.text)['actions'][0]['returnValue'])['entryList'][0]['entries']
                for partner in partners:

                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = self.partner_program_link
                    item['partner_directory'] = self.partner_directory
                    item['partner_program_name'] = ''
                    item['partner_company_name'] = partner.get('Name__c', '')
                    item['partner_type'] = partner.get('Partnership_Type__c', '')
                    item['company_domain_name'] = get_domain_from_url(partner.get('Website__c', ''))
                    item['company_description'] = cleanhtml(partner.get('Description__c', ''))
                    item['general_phone_number'] = partner.get('Phone__c', '')
                    item['headquarters_state'] = partner.get('State_Province__c', '')
                    item['headquarters_country'] = partner.get('Country__c', '')

                    yield item
