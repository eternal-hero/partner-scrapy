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
    name = 'intelprovider'
    partner_program_link = 'https://marketplace.intel.com/s/pmp-partner-program/a723b0000008PIBAA2/solution-software-and-service-providers?language=en_US'
    partner_directory = 'Intel solution, software and service provider'
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
        'cookie': 'renderCtx={"pageId":"faab079b-da30-4950-9474-83b3e47e8ec6","schema":"Published","viewType":"Published","brandingSetId":"a0c9d2f5-91c6-42ab-8ccd-016fbde06821","audienceIds":"6Au3b000000k9xR"}; CookieConsentPolicy=1:1; LSKey-c$CookieConsentPolicy=1:1; MP_URL_Tracking=https://marketplace.intel.com/s/pmp-partner-program/a723b0000008PIBAA2/solution-software-and-service-providers?language=en_US; RT="z=1&dm=intel.com&si=se2eniykj8&ss=lbh2o97q&sl=0&tt=0"; detected_bandwidth=HIGH; src_countrycode=EG; utag_main=v_id:0184f8fa81c7006a401ed4f36e340506f007d06700978$_sn:1$_se:2$_ss:0$_st:1670626280044$ses_id:1670624477646;exp-session$_pn:1;exp-session$ad_blocker:0;exp-session; AMCVS_AD2A1C8B53308E600A490D4D@AdobeOrg=1; pctrk=c451c287-3f45-4c98-8774-8c30032cd171; at_check=true; s_cc=true; adcloud={"_les_v":"y,intel.com,1670626282"}; mbox=session#942bc1643e424c1a990604ee0699e949#1670626344|PC#942bc1643e424c1a990604ee0699e949.37_0#1733869284; intelresearchUID=3005325167850M1670624589392; OptanonConsent=isGpcEnabled=0&datestamp=Sat+Dec+10+2022+00:21:24+GMT+0200+(Eastern+European+Standard+Time)&version=202209.2.0&isIABGlobal=false&hosts=&consentId=344aa9c5-1a8c-4bd9-83cd-c05c8da7fbc3&interactionCount=0&landingPath=https://marketplace.intel.com/s/pmp-partner-program/a723b0000008PIBAA2/solution-software-and-service-providers?language=en_US&groups=C0001:1,C0003:1,C0004:1,C0002:1; ELOQUA=GUID=046F0299DEC047449E7A8278C0960D3D; ELQSTATUS=OK; AMCV_AD2A1C8B53308E600A490D4D@AdobeOrg=1585540135|MCIDTS|19336|MCMID|75635820255915842100822139382507121084|MCAAMLH-1671229281|6|MCAAMB-1671229281|6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y|MCOPTOUT-1670631681s|NONE|MCSYNCSOP|411-19343|vVersion|4.4.0; _cs_c=0; _abck=6AA5AF9BC108D46FA31A68C99392C2C3~0~YAAQxDndWLmUSeqEAQAATE38+AlKvLBJ+N4/TMoE9KtORZO6zbFTBdPgEHn7RC6shNiqW76DmWeZK8C/d2GsOpCIdKdYoWcdKiaJclaem3OJifmKdC/qfbSoAFuWpZiMmRD9XelY9LJqo9VlUSzhPWbpqN/p5Y3ljr/NJ/bjtdGAsAFCrS5dbXx4jyNfFrCBLEtElFXbYeN5lVOqxfjRI/wRUmHQlgvda104+YNFtTxdzHEbTXHbCsugUvtU20XwvPM9UaCuzyvORgPPk9aOAoacFGM6WvVZ7TlhwYmvtpxo/I1PYO3VYwkyAZ0m4rnja+w4+iL7wd09RVSLuOfQdJ3XvszvDwUygyfopHGl89bZxKMM3J6+zbuBGlEy8KAsfF3lp6aa+hN1OihxbS8pPpyar6cjyXs=~-1~||-1||~-1; s_sq=[[B]]; force-stream=!i21vZi6grEGVBnQ/OqZ90fEnrLfr+A0g/qMHncd6v+b0CyregedRP6CposGaIXUc7yuIpcFozxFARn8=; _cs_id=48825e55-2e87-aeeb-deb2-2b5977a6ca4c.1670624485.3.1670660459.1670660459.1589385054.1704788485755; _cs_s=1.5.0.1670662260479',
        'Origin': 'https://marketplace.intel.com',
        'Referer': 'https://marketplace.intel.com/s/pmp-partner-program/a723b0000008PIBAA2/solution-software-and-service-providers?language=en_US',
        'Sec-Ch-Ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'Sec-Ch-Ua-mobile': '?0',
        'Sec-Ch-Ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'x-sfdc-page-cache': 'ce0db2a67b1b76bc',
        'x-sfdc-page-scope-id': '35fabfae-8e4d-49dd-a203-fe16258b5310',
        'x-sfdc-request-id': '141822000004d35813',
    }
    api_link = 'https://marketplace.intel.com/s/sfsites/aura?r=17&aura.ApexAction.execute=1'

    def start_requests(self):
        page = 1
        payload = f'message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22331%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PMP_SearchUtilityNew%22%2C%22method%22%3A%22newSearch%22%2C%22params%22%3A%7B%22requestJSON%22%3A%22%7B%5C%22category%5C%22%3A%5C%22a723b0000008PIBAA2%5C%22%2C%5C%22keyword%5C%22%3A%5C%22%5C%22%2C%5C%22page%5C%22%3A{page}%2C%5C%22sortType%5C%22%3A%5C%22%5C%22%2C%5C%22filters%5C%22%3A%7B%7D%2C%5C%22searchType%5C%22%3A%5C%22pmp-partner-program%5C%22%2C%5C%22langCode%5C%22%3A%5C%22en-US%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22tr2UlkrAHzi37ijzEeD2UA%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22wlLnZvhAshR7p-QD7LB6JQ%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11yCoreCollector%22%3A%228giBLfYbOC17LwOopJh9VQ%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3ArecordDetail%22%3A%22wPFVpZB_W4msvEbsp4rY4g%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3AforceCommunityFeed%22%3A%221HH7K53dNQsb5NNDobHtcA%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3AfeedPublisher%22%3A%22oi0RxomnI2XIwaO6mWGejg%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fs%2Fpmp-partner-program%2Fa723b0000008PIBAA2%2Fsolution-software-and-service-providers%3Flanguage%3Den_US&aura.token=null'
        yield scrapy.Request(method='POST', url=self.api_link,
                             callback=self.parse,
                             headers=self.headers,
                             body=payload,
                             dont_filter=True,
                             meta={'page': page})

    def parse(self, response):
        page = response.meta['page']

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
                        totalPages = jsn_data2['totalPages']
                    else:
                        raise Exception
                except:
                    jsn_data2 = None
                    partners = list()
                    totalPages = None

                if jsn_data2 and len(partners) > 0:
                    self.logger.info(f'Intel Service Provider Page: {page}, partners = {len(partners)}')
                    for partner in partners:
                        # Initialize item
                        item = dict()
                        for k in self.item_fields:
                            item[k] = ''

                        item['partner_program_link'] = self.partner_program_link
                        item['partner_directory'] = self.partner_directory

                        item['partner_company_name'] = partner['name']
                        item['partner_tier'] = partner['status'] if 'status' in partner and partner['status'] else ''
                        item['company_domain_name'] = get_domain_from_url(partner['website']) if 'website' in partner and partner['website'] else ''
                        item['partner_type'] = partner['role'] if 'role' in partner and partner['role'] else ''
                        partner_id = partner['id']

                        payload = f'message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22543%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PMP_MarketplaceNewPartnerDetail%22%2C%22method%22%3A%22getPartnerDetails%22%2C%22params%22%3A%7B%22partnerId%22%3A%22{partner_id}%22%2C%22langCode%22%3A%22en_US%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22tr2UlkrAHzi37ijzEeD2UA%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22wlLnZvhAshR7p-QD7LB6JQ%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11yCoreCollector%22%3A%228giBLfYbOC17LwOopJh9VQ%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3ArecordDetail%22%3A%22wPFVpZB_W4msvEbsp4rY4g%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3AforceCommunityFeed%22%3A%221HH7K53dNQsb5NNDobHtcA%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3AfeedPublisher%22%3A%22oi0RxomnI2XIwaO6mWGejg%22%2C%22COMPONENT%40markup%3A%2F%2Fforce%3AoutputField%22%3A%22gBPfAfDNBQarQOsYi9u3dQ%22%2C%22COMPONENT%40markup%3A%2F%2FforceChatter%3AfeedQbProxy%22%3A%22_oHZaul6rjPu1puZzWDkQg%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fs%2Fpartner%2F{partner_id}%3Flanguage%3Den_US&aura.token=null'
                        yield scrapy.Request(method='POST', url=self.api_link,
                                             callback=self.parse_partner,
                                             headers=self.headers,
                                             body=payload,
                                             meta={'item': item},
                                             dont_filter=True)

                    # follow next page
                    if totalPages and page == 1:
                        for page in range(2, totalPages+1):
                            payload = f'message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22702%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PMP_SearchUtilityNew%22%2C%22method%22%3A%22newSearch%22%2C%22params%22%3A%7B%22requestJSON%22%3A%22%7B%5C%22category%5C%22%3A%5C%22a723b0000008PIBAA2%5C%22%2C%5C%22keyword%5C%22%3A%5C%22%5C%22%2C%5C%22page%5C%22%3A{page}%2C%5C%22sortType%5C%22%3A%5C%22%5C%22%2C%5C%22filters%5C%22%3A%7B%7D%2C%5C%22searchType%5C%22%3A%5C%22pmp-partner-program%5C%22%2C%5C%22langCode%5C%22%3A%5C%22en-US%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22tr2UlkrAHzi37ijzEeD2UA%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22wlLnZvhAshR7p-QD7LB6JQ%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11yCoreCollector%22%3A%228giBLfYbOC17LwOopJh9VQ%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3ArecordDetail%22%3A%22wPFVpZB_W4msvEbsp4rY4g%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3AforceCommunityFeed%22%3A%221HH7K53dNQsb5NNDobHtcA%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3AfeedPublisher%22%3A%22oi0RxomnI2XIwaO6mWGejg%22%2C%22COMPONENT%40markup%3A%2F%2Fforce%3AoutputField%22%3A%22gBPfAfDNBQarQOsYi9u3dQ%22%2C%22COMPONENT%40markup%3A%2F%2FforceChatter%3AfeedQbProxy%22%3A%22_oHZaul6rjPu1puZzWDkQg%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fs%2Fpmp-partner-program%2Fa723b0000008PIBAA2%2Fsolution-software-and-service-providers%3Flanguage%3Den_US&aura.token=null'
                            yield scrapy.Request(method='POST', url=self.api_link,
                                                 callback=self.parse,
                                                 headers=self.headers,
                                                 body=payload,
                                                 dont_filter=True,
                                                 meta={'page': page})

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
                item['specializations'] = [itm['name'] for itm in partner['specialties']]

                if 'partnerDetails' in partner and 'attributes' in partner['partnerDetails']:
                    info_lst = partner['partnerDetails']['attributes']
                    for info in info_lst:
                        if info['title'] == 'Industry':
                            item['industries'] = {itm['label'].strip(): [i['label'].strip() for i in itm['values']] for itm in info['sections']}

                        elif info['title'] == 'Use Cases':
                            item['categories'] = [itm['label'] for itm in info['sections']]

                        elif info['title'] == "Regional Coverage":
                            item['regions'] = {itm['label'].strip(): [i['label'].strip() for i in itm['values']] for itm in info['sections']}

                        elif info['title'] == 'Address' and len(info['sections']) > 0:
                            item['headquarters_address'] = ', '.join([itm['label'] for itm in info['sections'][0]['values']])
                            item['headquarters_country'] = info['sections'][0]['values'][-1]['label']

        yield item
