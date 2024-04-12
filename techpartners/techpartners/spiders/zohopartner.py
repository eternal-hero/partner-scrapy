import time

import requests
import scrapy
import json
import datetime
import os
from techpartners.spiders.base_spider import BaseSpider
from techpartners.functions import *
from urllib.parse import urlparse


class Spider(BaseSpider):
    name = 'zohopartner'
    partner_program_link = 'https://www.zoho.com/partners/find-partner.html'
    partner_directory = 'Zoho Partner'
    partner_program_name = ''
    crawl_id = 1261

    item_fields = ['partner_program_link', 'partner_directory', 'partner_program_name', 'partner_company_name',
                   'company_domain_name', 'partner_type', 'partner_tier', 'company_description', 'headquarters_street',
                   'headquarters_city', 'headquarters_state', 'headquarters_zipcode', 'headquarters_country',
                   'locations_street', 'locations_city', 'locations_state', 'locations_zipcode', 'locations_country',
                   'regions', 'languages', 'services', 'solutions', 'products', 'pricing', 'specializations',
                   'categories', 'year_founded', 'partnership_timespan', 'general_phone_number', 'general_email_address',
                   'use_case', 'integrations', 'contract_vehicles', 'company_characteristics', 'linkedin_link',
                   'twitter_link', 'facebook_link', 'primary_contact_name', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'certified?', 'industries', 'partner_clients', 'notes']

    allowed_domains = ['www.zoho.com']
    start_urls = ['https://store.zoho.com/partnerlisting/toppartners?callback=getAllDetails']

    def parse(self, response):
        data = json.loads(response.text.strip('getAllDetails(')[:-1])

        for partner in data:
            # Initialize item
            item = dict()
            for k in self.item_fields:
                item[k] = ''

            item['partner_program_link'] = self.partner_program_link
            item['partner_directory'] = self.partner_directory
            item['partner_program_name'] = self.partner_program_name

            item['partner_company_name'] = partner['companyname'] if 'companyname' in partner else ''
            item['partner_tier'] = partner['tier'] if 'tier' in partner else ''
            item['partner_type'] = partner['category'] if 'category' in partner else ''
            # year = datetime.datetime.now().year
            item['partnership_timespan'] = partner['years'] if 'years' in partner else ''
            item['languages'] = partner['language'] if 'language' in partner else ''
            item['specializations'] = partner['practice'] if 'practice' in partner else ''
            item['solutions'] = partner['topzohoapps'] if 'topzohoapps' in partner else ''
            # item['company_domain_name'] = partner['website'] if 'website' in partner else ''
            url_obj = urlparse(partner['website'] if 'website' in partner else '')
            item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
            x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
            if x:
                item['company_domain_name'] = x[-1]
            item['regions'] = partner['country'] if 'country' in partner else ''

            partnerid = partner['encryptedpid'] if 'encryptedpid' in partner else ''
            if partnerid != '':
                url = 'https://store.zoho.com/restapi/portal/v1/json/partnerreview?JSONString={"partnerid":"%s"}&callback=onGetCustomerReviewDateSuccess'%partnerid
                for i in range(3):
                    try:
                        x = requests.get(url=url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"})
                        if x.status_code == 200:
                            res = json.loads(x.text.strip('onGetCustomerReviewDateSuccess(')[:-1])
                            if 'partnerDetails' in res:
                                details = res['partnerDetails']
                                item['linkedin_link'] = details['linkedinurl'] if 'linkedinurl' in details else ''
                                item['notes'] = details['notes'] if 'notes' in details else ''
                                item['company_description'] = cleanhtml(details['partner_description']) if 'partner_description' in details else ''
                                break
                    except:
                        continue

            if 'alladdressobj' in partner and len(partner['alladdressobj']) > 0:
                for location in partner['alladdressobj']:
                    if 'primarylocation' in location and location['primarylocation']:
                        item['general_email_address'] = location['email'] if 'email' in location else ''
                        # item['primary_contact_name'] = location['firstname'] if 'firstname' in location else '' + location['lastname'] if 'lastname' in location else ''
                        item['headquarters_street'] = location['address_details'] if 'address_details' in location else ''
                        item['headquarters_city'] = location['city'] if 'city' in location else ''
                        item['headquarters_state'] = location['state']  if 'state' in location else ''
                        item['headquarters_zipcode'] = location['zipcode']  if 'zipcode' in location else ''
                        item['headquarters_country'] = location['country']  if 'country' in location else ''
                        item['general_phone_number'] = location['phone']  if 'phone' in location else ''

                for location in partner['alladdressobj']:
                    item['general_email_address'] = location['email'] if 'email' in location else item['general_email_address']
                    item['primary_contact_name'] = ' '.join([location['firstname'] if 'firstname' in location else '',
                                                   location['lastname'] if 'lastname' in location else '']).strip()
                    item['locations_street'] = location['address_details'] if 'address_details' in location else ''
                    item['locations_city'] = location['city'] if 'city' in location else ''
                    item['locations_state'] = location['state']  if 'state' in location else ''
                    item['locations_zipcode'] = location['zipcode']  if 'zipcode' in location else ''
                    item['locations_country'] = location['country']  if 'country' in location else ''
                    item['general_phone_number'] = location['phone']  if 'phone' in location else item['general_phone_number']
                    yield item
            else:
                yield item
