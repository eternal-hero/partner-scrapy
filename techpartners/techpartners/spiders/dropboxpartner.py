# import needed libraries
import json
import re
import math
from techpartners.spiders.base_spider import BaseSpider
import scrapy
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse
from scrapy.http import HtmlResponse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'dropboxpartner'
    partner_program_link = 'https://portal.dropboxpartners.com/s/locator?language=en_US'
    partner_directory = 'Dropbox Partner Directory'
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
                   'primary_contact_name', 'primary_contact_phone_number',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts', 'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
               'cookie': 'renderCtx={"pageId":"d9231973-9dae-41af-8d68-d868df23f61a","schema":"Published","viewType":"Published","brandingSetId":"ba1bdc82-2431-48f8-a8d0-5d3e12a902b3","audienceIds":"6Au1J0000004Cbu"}; CookieConsentPolicy=0:1; LSKey-c$CookieConsentPolicy=0:1; CookieConsentPolicy=0:0; LSKey-c$CookieConsentPolicy=0:0; pctrk=5c77d156-7301-44f3-a880-f26c9dac6331; sfdc-stream=!nBeqH6WgqRVxZgKk+ARJcZJfBAngAK/0UfTyktTTSCb04BwW3+tU6mgRwe/FX0ut8oruNNFvFfNQXj4=',
               'Host': 'portal.dropboxpartners.com',
               'Accept': '*/*',
               'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
               'Accept-Encoding': 'gzip, deflate, br',
               'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
               'Origin': 'https://portal.dropboxpartners.com',
               'Referer': 'https://portal.dropboxpartners.com/s/locator?language=en_US',
               'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
               'Connection': 'keep-alive',
               'sec-ch-ua-mobile': '?0',
               'sec-ch-ua-platform': 'Windows',
               'Sec-Fetch-Dest': 'empty',
               'Sec-Fetch-Mode': 'cors',
               'Sec-Fetch-Site': 'same-origin',
               'X-SFDC-Page-Cache': '41d6462ad73b3311',
               'X-SFDC-Page-Scope-Id': '788a5405-aea0-4845-a843-101417f71ba2',
               'X-SFDC-Request-Id': '27754000006386aa08'
               }

    def start_requests(self):

        data = 'message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22118%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PartnerLocatorController%22%2C%22method%22%3A%22getPartners%22%2C%22params%22%3A%7B%22searchText%22%3A%22%22%7D%2C%22cacheable%22%3Atrue%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%222yRFfs4WfGnFrNGn9C_dGg%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22PAjEh9HEIZmsDpK-Y8SVTg%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fs%2Flocator%3Flanguage%3Den_US&aura.token=null'
        yield scrapy.Request(method='POST', url='https://portal.dropboxpartners.com/s/sfsites/aura?r=1&aura.ApexAction.execute=1',
                             body=data, callback=self.parse, headers=self.headers)

    def parse(self, response):
        if response.status != 200:
            print(response.status)
            print(response.text)
            return

        data = json.loads(response.text)
        if 'actions' in data:
            for d in data['actions']:
                if 'id' in d and (len(data['actions']) == 1 or d['id'] == "118;a"):
                    profiles = d['returnValue']['returnValue']
                    for profile in profiles:
                        # Initialize item
                        item = dict()
                        for k in self.item_fields:
                            item[k] = ''

                        item['partner_program_link'] = self.partner_program_link
                        item['partner_directory'] = self.partner_directory
                        item['partner_program_name'] = self.partner_program_name

                        item['partner_company_name'] = profile['record']['Account_Name__c']
                        item['partner_tier'] = profile['record']['Partner_Tier__c'] if 'Partner_Tier__c' in profile['record'] else ''
                        item['company_description'] = cleanhtml(profile['record']['Company_Description__c']) if 'Company_Description__c' in profile['record'] else ''
                        item['general_phone_number'] = profile['record']['Phone_Number__c'] if 'Phone_Number__c' in profile['record'] else ''
                        item['regions'] = profile['record']['Region__c'] if 'Region__c' in profile['record'] else ''
                        item['industries'] = profile['record']['Industry__c'] if 'Industry__c' in profile['record'] else ''
                        item['certifications'] = profile['formattedCerts'] if 'formattedCerts' in profile else (profile['record']['Employee_Certifications__c'] if 'Employee_Certifications__c' in profile['record'] else '')

                        item['locations_street'] = profile['record']['Street__c'] if 'Street__c' in profile['record'] else ''
                        item['locations_state'] = profile['record']['State__c'] if 'State__c' in profile['record'] else ''
                        item['locations_zipcode'] = profile['record']['Postal_Code__c'] if 'Postal_Code__c' in profile['record'] else ''
                        item['locations_country'] = profile['record']['Country__c'] if 'Country__c' in profile['record'] else ''

                        item['company_domain_name'] = profile['record']['Company_Website__c'] if 'Company_Website__c' in profile['record'] and profile['record']['Company_Website__c'] else ''
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

                        yield item
