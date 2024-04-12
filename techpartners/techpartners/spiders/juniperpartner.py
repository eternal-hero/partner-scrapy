# import needed libraries
import json
import re
import math
import requests

from techpartners.spiders.base_spider import BaseSpider
import scrapy
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse
from scrapy.http import HtmlResponse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'juniperpartner'
    partner_program_link = 'https://junipercommunity.force.com/prm/s/partnerlocator'
    partner_directory = 'Juniper community Partner'
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
                   'primary_contact_name', 'primary_contact_phone_number', 'primary_contact_email_address',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts', 'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes', 'diversity_type']
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
               'Accept': '*/*',
               'Accept-Encoding': 'gzip, deflate, br',
               'Accept-Language': 'en-US,en;q=0.9',
               'Connection': 'keep-alive',
               'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
               'cookie': 'renderCtx={"pageId":"4f1224c5-7a0d-46aa-8186-c02bc9622b67","schema":"Published","viewType":"Published","brandingSetId":"e13482dc-f6dc-4d61-b968-b2eeea54a2a0","audienceIds":"6Au3c000000fzdb"}; CookieConsentPolicy=0:1; LSKey-c$CookieConsentPolicy=0:1; sfdc-stream=!FgKL4eXkuzGQs0OiCXfG13Hm/GpyCoJQTONtq6TE3ZHh5j5qMM3NOmnqwzj0yHIcSISd8+kdy6m2DhE=; force-proxy-stream=!5SYebm8cb8DARfk2tgpxx7QiiB0rhdnIbimCSsVmo1kWY2R3P2g3kz9KuIDbwtVtX6DcmHl5srC1hw==; force-stream=!FgKL4eXkuzGQs0OiCXfG13Hm/GpyCoJQTONtq6TE3ZHh5j5qMM3NOmnqwzj0yHIcSISd8+kdy6m2DhE=; BrowserId=NsNRpmo-Ee2oeRuRj4sAxA; BrowserId_sec=NsNRpmo-Ee2oeRuRj4sAxA; pctrk=6aa2d570-c734-4e05-bce9-f70d2a935407',
               'Host': 'junipercommunity.force.com',
               'Origin': 'https://junipercommunity.force.com',
               'Referer': 'https://junipercommunity.force.com/prm/s/partnerlocator',
               'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
               'sec-ch-ua-mobile': '?0',
               'sec-ch-ua-platform': 'Windows',
               'Sec-Fetch-Dest': 'empty',
               'Sec-Fetch-Mode': 'cors',
               'Sec-Fetch-Site': 'same-origin',
               'X-SFDC-Page-Cache': '94607f0c0a8e030c',
               'X-SFDC-Page-Scope-Id': 'f25b537c-6e0b-4c44-b179-6fbd53664d5a',
               'X-SFDC-Request-Id': '370269000064ed59a7'
               }
    specializations = list()

    def start_requests(self):
        # acquire specializations list
        specialization_link = 'https://junipercommunity.force.com/prm/s/sfsites/aura?r=5&aura.ApexAction.execute=1'
        data = 'message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22107%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_FieldController%22%2C%22method%22%3A%22getSpecializationsAsOptions%22%2C%22cacheable%22%3Atrue%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22tr2UlkrAHzi37ijzEeD2UA%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22wlLnZvhAshR7p-QD7LB6JQ%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11yCoreCollector%22%3A%228giBLfYbOC17LwOopJh9VQ%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fprm%2Fs%2Fpartnerlocator&aura.token=null'
        response = requests.post(url=specialization_link,
                                 headers=self.headers,
                                 data=data)
        if response.status_code == 200:
            data = response.json()
            if 'actions' in data and data['actions'][0]['state'] == 'SUCCESS':
                self.specializations = [{'value': specialization['value'], 'label': specialization['label']} for specialization in data['actions'][0]['returnValue']['returnValue']]

        # acquire countries list
        countries_link = 'https://junipercommunity.force.com/prm/s/sfsites/aura?r=1&aura.RecordUi.getPicklistValuesByRecordType=1'
        data = 'message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22111%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FRecordUiController%2FACTION%24getPicklistValuesByRecordType%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22objectApiName%22%3A%22Account%22%2C%22recordTypeId%22%3A%22012C0000000GJgwIAG%22%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22tr2UlkrAHzi37ijzEeD2UA%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22wlLnZvhAshR7p-QD7LB6JQ%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11yCoreCollector%22%3A%228giBLfYbOC17LwOopJh9VQ%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fprm%2Fs%2Fpartnerlocator&aura.token=null'
        response = requests.post(url=countries_link,
                                 headers=self.headers,
                                 data=data)
        if response.status_code == 200:
            data = response.json()
            if 'actions' in data and data['actions'][0]['state'] == 'SUCCESS':
                countries = [country['label'] for country in data['actions'][0]['returnValue']['picklistFieldValues']['Partner_Coverage_Area__c']['values']]

                # Start scraping for each country
                for country in countries:
                    data = f'message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22146%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_LocatorSearch%22%2C%22method%22%3A%22searchAccount%22%2C%22params%22%3A%7B%22filter%22%3A%7B%22Reseller__c%22%3Atrue%7D%2C%22locationFilter%22%3A%5B%7B%22country%22%3A%22{urllib.parse.quote_plus(country)}%22%2C%22state%22%3A%22%22%2C%22city%22%3A%22%22%2C%22type%22%3A%22country%22%7D%5D%2C%22limitNum%22%3A100000%2C%22rowOffset%22%3A0%2C%22sortedBy%22%3A%22%22%2C%22sortDir%22%3A%22%22%2C%22selectedDiversitiestoSearch%22%3A%5B%5D%2C%22IncludeOnlyDiversePartners%22%3Afalse%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22tr2UlkrAHzi37ijzEeD2UA%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22wlLnZvhAshR7p-QD7LB6JQ%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11yCoreCollector%22%3A%228giBLfYbOC17LwOopJh9VQ%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fprm%2Fs%2Fpartnerlocator&aura.token=null'
                    yield scrapy.Request(method='POST', url='https://junipercommunity.force.com/prm/s/sfsites/aura?r=2&aura.ApexAction.execute=1',
                                         body=data, callback=self.parse, headers=self.headers,
                                         meta={'country': country})
        else:
            self.logger.info(f'Failed to obtain Countries data, with status {response.status_code}')

    def parse(self, response):
        country = response.meta['country']
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST COUNTRY: {country}, STATUS: <{response.status}> RESPONSE: {response.text}')
            return

        data = json.loads(response.text)
        if 'actions' in data:
            try:
                partners = data['actions'][0]['returnValue']['returnValue']
            except:
                partners = list()
            self.logger.info(f'Country {country}, Result Partners: {len(partners)}')
            for partner in partners:
                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = self.partner_program_name

                item['partner_company_name'] = partner['Name']
                item['company_domain_name'] = get_domain_from_url(partner['URL__c']) if 'URL__c' in partner and partner['URL__c'] else ''
                item['company_description'] = cleanhtml(partner['Partner_Description__c']) if 'Partner_Description__c' in partner and partner['Partner_Description__c'] else ''
                item['partner_tier'] = partner['Partner_Level__c'] if 'Partner_Level__c' in partner and partner['Partner_Level__c'] else ''

                item['diversity_type'] = partner['Select_all_socio_economic_designations__c'] if 'Select_all_socio_economic_designations__c' in partner and partner['Select_all_socio_economic_designations__c'] else ''

                contact_info = partner['Contact__c'] if 'Contact__c' in partner and partner['Contact__c'] else None
                if contact_info:
                    # extract email
                    if re.search(r"\w+@\w+\.\w+", contact_info.strip()):
                        item['primary_contact_email_address'] = re.search(r"\w+@\w+\.\w+", contact_info.strip()).group()
                        contact_info = contact_info.replace(item['primary_contact_email_address'], '').strip()
                    # extract phone number
                    if re.search(r"\+?[(\s\d\-)]{4,}e?x?t?:?\s?[(\d\-)]+", contact_info.strip(), re.IGNORECASE):
                        item['primary_contact_phone_number'] = re.search(r"\+?[(\s\d\-)]{4,}e?x?t?:?\s?[(\d\-)]+", contact_info.strip(), re.IGNORECASE).group()
                        contact_info = contact_info.replace(item['primary_contact_phone_number'], '').strip()

                    item['primary_contact_name'] = contact_info.strip(',.;')

                item['headquarters_street'] = partner['BillingAddress_AddressLine1__c'] if 'BillingAddress_AddressLine1__c' in partner and partner['BillingAddress_AddressLine1__c'] else None
                item['headquarters_city'] = partner['BillingAddress_city__c'] if 'BillingAddress_city__c' in partner and partner['BillingAddress_city__c'] else None
                item['headquarters_state'] = partner['BillingAddressState__c'] if 'BillingAddressState__c' in partner and partner['BillingAddressState__c'] else None
                item['headquarters_zipcode'] = partner['BillingAddressPostalCode__c'] if 'BillingAddressPostalCode__c' in partner and partner['BillingAddressPostalCode__c'] else None
                item['headquarters_country'] = partner['BillingAddress_country__c'] if 'BillingAddress_country__c' in partner and partner['BillingAddress_country__c'] else None

                item['specializations'] = list()
                for specialization in self.specializations:
                    if specialization['value'] in partner and partner[specialization['value']]:
                        item['specializations'].append(specialization['label'])

                yield item
