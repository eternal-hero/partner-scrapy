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
    name = 'inteldistributor'
    partner_program_link = 'https://marketplace.intel.com/s/pmp-partner-program/a723b0000008PICAA2/distributor?language=en_US'
    partner_directory = 'Intel Distributor Partner'
    partner_program_name = ''
    crawl_id = None

    custom_settings = {
        # 'DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 8,
    }

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
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
        'Authority': 'marketplace.intel.com',
        'Accept': '*/*',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'no-cache',
        'cookie': 'renderCtx={"pageId":"faab079b-da30-4950-9474-83b3e47e8ec6","schema":"Published","viewType":"Published","brandingSetId":"a0c9d2f5-91c6-42ab-8ccd-016fbde06821","audienceIds":"6Au3b000000k9xR"}; CookieConsentPolicy=1:1; LSKey-c$CookieConsentPolicy=1:1; MP_URL_Tracking=https://marketplace.intel.com/s/pmp-partner-program/a723b0000008PICAA2/distributor?language=en_US; RT="z=1&dm=intel.com&si=upkmk3pqt1&ss=lbbzogys&sl=0&tt=0"; sfdc-stream=!vcpX9dWdDgMlJPCrjYa4BdtkXauK49aEbeS7W6IshZHFLj73m65nP7OXqY4es7qQBztlUnp4zpodlQ==; force-proxy-stream=!PtC3C7NOoVXsh4MlP09NAjm7TN33QP02p5l9cda8EgVYFsB2e/XKG2zcp/Blk3BIw8Ecr4I7QBuicWw=; detected_bandwidth=HIGH; src_countrycode=EG; force-stream=!brm5MnfscBzQ7RUlP09NAjm7TN33QM+RrzhQTArZLr5IvFqYUcHrGyah5hAqnxxclm7G9yHTOfhUBIo=; _abck=CC805F7F360AE8CB7538B7C2A9A98F93~-1~YAAQsfptaPg8z9KEAQAAtiaq5gkt9Y1ymRXuZ5he2y23BjRsFLADdwwD2cJkgGED4JBN8agSkUvdLuElCZUzpzwoBQeZyg6Rt2NnCNIVs20Vlp82xtyBGf5Dw9Vlx151rND7ANOVCA0Y6JyrCowWT1o4yPeXR/xRxJaA73vmP1ZT8vJmoMhOrxfvq7wNdPP89dwmXKgZGmCBwij53clXiEChuzfkwjstJ4aCC4CQibOtOBAGuKeTzWVSENDDcgDFoUtXA+DwKaGDE+3k7UNuGoPQ0BGtxjk4Zjwl9QwPdHaxkNShWvfdA1JmMvbhdfsfZMOw3USFfcf5mhp1MwrqOSusJSihcfSqzCEq32Z9Rb+0lzS0UeKo4jctvg==~-1~-1~-1; ak_bmsc=7189D31556377272EDD321A04E992910~000000000000000000000000000000~YAAQsfptaPk8z9KEAQAAtiaq5hLv7sq8heg+O8QoZV6Z1Z47iSWvHu9AnvjU2jbI5Ck8gZUdY8MGvKJ4HTmHpTNPRvD8x3RwvZuv6sGnsSTd/LY+rnPNbP1qeO8FHYdDwKuu0BPhhmTA7nNg5ygGDPCnvHCaOS/+5txUFKf4Ra8i6+53WkBwdbF6T//LY9M6DmDz4JpEXlUVAlBQzl/SNmz6ruh57vlPtZF0QNPIb71nuZBsLcmH7Ux/CjUqVCIx/TJDffcAN8NuhoVSZiALgjyb8Zj442mXCprLDkFmPalXs8x53kvEw/eoLLfIh07nelpcEzqhO4CGOdhb+SOwsO5un4AwZCRUdRC2bEU+jKeTLaqdUoM0Ll+mnybIgK7MdUsQSoHcrWbp; bm_sz=F924CA896557F0A1D998D5336B3EF4D7~YAAQsfptaPo8z9KEAQAAtiaq5hLZUGG3QWRO1GkUFpUkn7ehxfDtbup4y4+j+lGC0VjStLujREREtitYNYThiRykhRVq/CBD0fRMoafRY1X8H1/E7h2cOsruDpT2cHY9Vc2dzptycBqnuqZ0Yayudyoknmzp42E0j3NM2WeP/00TKmGgqxiiF37cwKBZjn7rX+0NYQLaDw4f8gLPVvzeUkzssNCKVnIEBsgzIGJHJ3U7tlPoGtiOH4ODkxCtge0k61tVGwwMREq1ZJXMS5UII3dRI37CjZoZ6j+hpoPFBnkJZw==~4536388~3748164',
        'Origin': 'https://marketplace.intel.com',
        'Referer': 'https://marketplace.intel.com/s/pmp-partner-program/a723b0000008PICAA2/distributor?language=en_US',
        'Sec-Ch-Ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'Sec-Ch-Ua-mobile': '?0',
        'Sec-Ch-Ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'x-sfdc-page-cache': 'c27ad6d3c1e2054b',
        'x-sfdc-page-scope-id': 'ff77f05b-2882-435f-94d6-a03ab3944e1c',
        'x-sfdc-request-id': '90842900005c885539',
    }
    api_link = 'https://marketplace.intel.com/s/sfsites/aura?r=17&aura.ApexAction.execute=1'

    def start_requests(self):
        countries = None
        r = requests.post(self.api_link,
                          data='message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22256%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PMP_TaxonomyStore%22%2C%22method%22%3A%22fetchTaxonomy%22%2C%22params%22%3A%7B%22requestJSON%22%3A%22%7B%5C%22baseTaxonomy%5C%22%3Afalse%2C%5C%22langCode%5C%22%3A%5C%22en-US%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22tr2UlkrAHzi37ijzEeD2UA%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22wlLnZvhAshR7p-QD7LB6JQ%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11yCoreCollector%22%3A%228giBLfYbOC17LwOopJh9VQ%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fs%2Fpmp-partner-program%2Fa723b0000008PICAA2%2Fdistributor%3Flanguage%3Den_US&aura.token=null',
                          headers=self.headers)

        if r.status_code != 200:
            self.logger.info(f'ERROR REQUEST COUNTRIES: {r.status_code}, RESPONSE: {r.text}')
        else:
            try:
                jsn_data = json.loads(r.text)
            except:
                jsn_data = None

            if jsn_data and 'actions' in jsn_data and len(jsn_data['actions']) > 0 and 'returnValue' in jsn_data['actions'][0] and 'returnValue' in jsn_data['actions'][0]['returnValue'] and 'Country' in jsn_data['actions'][0]['returnValue']['returnValue'] and 'records' in jsn_data['actions'][0]['returnValue']['returnValue']['Country']:
                countries = jsn_data['actions'][0]['returnValue']['returnValue']['Country']['records']
            else:
                countries = None

        if countries:
            for country in countries:
                country = country.strip()
                payload = f'message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22552%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PMP_SearchUtilityNew%22%2C%22method%22%3A%22newSearch%22%2C%22params%22%3A%7B%22requestJSON%22%3A%22%7B%5C%22category%5C%22%3A%5C%22a723b0000008PICAA2%5C%22%2C%5C%22keyword%5C%22%3A%5C%22%5C%22%2C%5C%22page%5C%22%3A1%2C%5C%22sortType%5C%22%3A%5C%22%5C%22%2C%5C%22filters%5C%22%3A%7B%5C%22Country%5C%22%3A%5B%5C%22{urllib.parse.quote_plus(country)}%5C%22%5D%7D%2C%5C%22searchType%5C%22%3A%5C%22pmp-partner-program%5C%22%2C%5C%22langCode%5C%22%3A%5C%22en-US%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22tr2UlkrAHzi37ijzEeD2UA%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22wlLnZvhAshR7p-QD7LB6JQ%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11yCoreCollector%22%3A%228giBLfYbOC17LwOopJh9VQ%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3ArecordDetail%22%3A%22wPFVpZB_W4msvEbsp4rY4g%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3AforceCommunityFeed%22%3A%221HH7K53dNQsb5NNDobHtcA%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3AfeedPublisher%22%3A%22oi0RxomnI2XIwaO6mWGejg%22%2C%22COMPONENT%40markup%3A%2F%2Fforce%3AoutputField%22%3A%22gBPfAfDNBQarQOsYi9u3dQ%22%2C%22COMPONENT%40markup%3A%2F%2FforceChatter%3AfeedQbProxy%22%3A%22_oHZaul6rjPu1puZzWDkQg%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fs%2Fpmp-partner-program%2Fa723b0000008PICAA2%2Fdistributor%3Flanguage%3Den_US&aura.token=null'
                yield scrapy.Request(method='POST', url=self.api_link,
                                     callback=self.parse,
                                     headers=self.headers,
                                     body=payload,
                                     dont_filter=True,
                                     meta={'country': country})

    def parse(self, response):
        country = response.meta['country']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            try:
                jsn_data = json.loads(response.text)
            except:
                jsn_data = None

            if jsn_data and 'actions' in jsn_data and len(jsn_data['actions']) > 0 and 'returnValue' in jsn_data['actions'][0] and 'returnValue' in jsn_data['actions'][0]['returnValue']:
                try:
                    jsn_data2 = json.loads(jsn_data['actions'][0]['returnValue']['returnValue'])
                    if jsn_data2 and 'results' in jsn_data2:
                        partners = jsn_data2['results']
                    else:
                        raise Exception
                except:
                    jsn_data2 = None
                    partners = list()

                if jsn_data2 and len(partners) > 0:
                    self.logger.info(f'Country: {country}, partners = {len(partners)}')
                    for partner in partners:
                        # Initialize item
                        item = dict()
                        for k in self.item_fields:
                            item[k] = ''

                        item['partner_program_link'] = self.partner_program_link
                        item['partner_directory'] = self.partner_directory

                        item['partner_company_name'] = partner['name']
                        item['company_domain_name'] = get_domain_from_url(partner['website']) if 'website' in partner and partner['website'] else ''
                        item['partner_type'] = partner['searchGrouping'] if 'searchGrouping' in partner and partner['searchGrouping'] else ''
                        partner_id = partner['id']

                        item['headquarters_country'] = country

                        payload = f'message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22764%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PMP_MarketplaceNewPartnerDetail%22%2C%22method%22%3A%22getPartnerDetails%22%2C%22params%22%3A%7B%22partnerId%22%3A%22{partner_id}%22%2C%22langCode%22%3A%22en_US%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22tr2UlkrAHzi37ijzEeD2UA%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22wlLnZvhAshR7p-QD7LB6JQ%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11yCoreCollector%22%3A%228giBLfYbOC17LwOopJh9VQ%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3ArecordDetail%22%3A%22wPFVpZB_W4msvEbsp4rY4g%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3AforceCommunityFeed%22%3A%221HH7K53dNQsb5NNDobHtcA%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3AfeedPublisher%22%3A%22oi0RxomnI2XIwaO6mWGejg%22%2C%22COMPONENT%40markup%3A%2F%2Fforce%3AoutputField%22%3A%22gBPfAfDNBQarQOsYi9u3dQ%22%2C%22COMPONENT%40markup%3A%2F%2FforceChatter%3AfeedQbProxy%22%3A%22_oHZaul6rjPu1puZzWDkQg%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fs%2Fpartner%2F{partner_id}%3Flanguage%3Den_US&aura.token=null'
                        yield scrapy.Request(method='POST', url=self.api_link,
                                             callback=self.parse_partner,
                                             headers=self.headers,
                                             body=payload,
                                             meta={'item': item},
                                             dont_filter=True)

    def parse_partner(self, response):
        item = response.meta['item']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {item["partner_company_name"]}')
        else:
            try:
                jsn_data = json.loads(response.text)
            except:
                jsn_data = None

            if jsn_data and 'actions' in jsn_data and len(jsn_data['actions']) > 0 and 'returnValue' in jsn_data['actions'][0] and 'returnValue' in jsn_data['actions'][0]['returnValue']:
                partner = jsn_data['actions'][0]['returnValue']['returnValue']
                item['company_description'] = cleanhtml(partner['partnerDescription']) if 'partnerDescription' in partner and partner['partnerDescription'] else ''
                item['partner_tier'] = partner['tierStatus'] if 'tierStatus' in partner and partner['tierStatus'] else ''

                if 'partnerDetails' in partner and 'attributes' in partner['partnerDetails']:
                    info_lst = partner['partnerDetails']['attributes']
                    for info in info_lst:
                        if info['title'] == 'Industry':
                            item['industries'] = [itm['label'] for itm in info['sections']]
                        elif info['title'] == 'Use Cases':
                            item['categories'] = [itm['label'] for itm in info['sections']]
                        elif info['title'] == 'Distribution Products':
                            item['products'] = {itm['label'].strip(): [i['label'].strip() for i in itm['values']] for itm in info['sections']}
                        elif info['title'] == 'Country/Region Coverage':
                            item['regions'] = ', '.join([', '.join([i['label'].strip() for i in itm['values']]) for itm in info['sections']])
                        elif info['title'] == 'Address' and len(info['sections']) > 0:
                            item['headquarters_address'] = ', '.join([itm['label'] for itm in info['sections'][0]['values']])
        yield item
