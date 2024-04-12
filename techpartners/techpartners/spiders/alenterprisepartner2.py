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


def clean_lst(lst):
    result = list()
    for field in lst:
        if '/' in field:
            for sub_field in field.split('/'):
                if sub_field not in result:
                    result.append(sub_field)
        elif field not in result:
            result.append(field)

    return result


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'alenterprisepartner2'
    partner_program_link = 'https://www.al-enterprise.com/en/partners/dspp/partnerships#sort=%40header%20ascending'
    partner_directory = 'DSPP Partners'
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
        'Connection': 'keep-alive',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Host': 'www.al-enterprise.com',
        'Origin': 'https://www.al-enterprise.com',
        'Referer': 'https://www.al-enterprise.com/en/partners/dspp/partnerships',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    }

    def start_requests(self):
        page_number = 0
        api_link = 'https://www.al-enterprise.com/coveo/rest/v2/?sitecoreItemUri=sitecore://web/{28C23767-10FD-4A46-BB9A-9CCE16C70A9A}?lang=en&ver=3&siteName=ALE'
        payload = f'aq=(((%40audience%20NOT%20%22Audience%2FInternal%22%20AND%20%40audience%20NOT%20%22Audience%2FState%22AND%20%40audience%20NOT%20%22Audience%2FPartners%22AND%20%40audience%20NOT%20%22Audience%2FExtranet%22AND%20(%40hidefromsearch%20%3D%3D%20%22false%22)AND%20%40contenttype%20NOT%20%22Content%20Type%2FGraphics%20and%20Banners%2FAdvertisements%22AND%20%40contenttype%20NOT%20%22Content%20Type%2FGraphics%20and%20Banners%2FAnimated%20GIF%22AND%20%40contenttype%20NOT%20%22Content%20Type%2FGraphics%20and%20Banners%2FIcon%22AND%20%40contenttype%20NOT%20%22Content%20Type%2FGraphics%20and%20Banners%2FLogo%22AND%20%40contenttype%20NOT%20%22Content%20Type%2FGraphics%20and%20Banners%2FThumbnail%20images%22AND%20%40contenttype%20NOT%20%22Content%20Type%2FGraphics%20and%20Banners%2FWeb%20banners%22AND%20%40contenttype%20NOT%20%22Content%20Type%2FReferences%2FCustomer%20Logo%22AND%20%40contenttype%20NOT%20%22Content%20Type%2FReferences%2FCustomer%20Support%20Image%22)%20OR%20NOT%20%40audience)%20NOT%20(%40z95xtemplatename%20%3D%3D%20%22Image%20Asset%22)%20NOT%20(%40filetype%20%3D%3D%20%22sitecoreitem%22)%20AND%20%40sitename%20%3D%3D%20%22ale%22%20AND%20%40sitename%20NOT%20%22ale%20intranet%22)%20(%40syssource%3D%3D(%22Coveo_web_index%20-%20ALE-Redesign%22)%20NOT%20%40templateid%3D%3D(%22adb6ca4f-03ef-4f47-b9ac-9ce2ba53ff97%22%2C%22fe5dd826-48c6-436d-b87a-7c4210c7413b%22))&cq=(%40z95xlatestversion%3D%3D%221%22)&searchHub=Partnerships&language=en&pipeline=DSPP-Pipeline&firstResult={page_number}&numberOfResults=5&excerptLength=200&enableDidYouMean=true&sortCriteria=fieldascending&sortField=%40header&queryFunctions=%5B%5D&rankingFunctions=%5B%5D&groupBy=%5B%7B%22field%22%3A%22%40dspppartnersolutions%22%2C%22maximumNumberOfValues%22%3A10001%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A10000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40apis%22%2C%22maximumNumberOfValues%22%3A10001%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A10000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40industry%22%2C%22maximumNumberOfValues%22%3A10001%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A10000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40dspppartnerlevel%22%2C%22maximumNumberOfValues%22%3A10001%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A10000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40region%22%2C%22maximumNumberOfValues%22%3A10001%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A10000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%5D&retrieveFirstSentences=true&timezone=Africa%2FCairo&disableQuerySyntax=false&enableDuplicateFiltering=false&enableCollaborativeRating=false&debug=false&context=%7B%7D'
        yield scrapy.Request(method='POST', url=api_link, body=payload,
                             callback=self.parse, headers=self.headers,
                             meta={'page_number': page_number})

    def parse(self, response):
        page_number = response.meta['page_number']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:

            try:
                jdata = json.loads(response.text)
            except:
                jdata = None

            if jdata and 'results' in jdata and len(jdata['results']) > 0:
                partners = jdata['results']

                self.logger.info(f"page_number: {page_number}, Number of results = {len(partners)}")

                for p in partners:

                    if 'raw' not in p:
                        continue

                    partner = p['raw']

                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = self.partner_program_link
                    item['partner_directory'] = self.partner_directory
                    item['partner_program_name'] = ''

                    item['partner_company_name'] = partner['header'] if 'header' in partner and partner['header'] else ''
                    item['company_description'] = cleanhtml(partner['plainbodytez120xt']) if 'plainbodytez120xt' in partner and partner['plainbodytez120xt'] else ''

                    item['languages'] = partner['language'] if 'language' in partner and partner['language'] else ''
                    item['general_email_address'] = partner['partnerz32xemailz32xid'] if 'partnerz32xemailz32xid' in partner and partner['partnerz32xemailz32xid'] else ''
                    # item['partner_tier'] = partner['dspppartnerlevel'][-1].replace("DSPP Partner Level/Solution Vendor/", '') if 'dspppartnerlevel' in partner and partner['dspppartnerlevel'] else ''
                    item['partner_tier'] = partner['dspppartnerlevel'] if 'dspppartnerlevel' in partner and partner['dspppartnerlevel'] else ''
                    if item['partner_tier'] != '':
                        item['partner_tier'] = clean_lst(item['partner_tier'])
                        if 'DSPP Partner Level' in item['partner_tier']:
                            item['partner_tier'].pop(item['partner_tier'].index('DSPP Partner Level'))

                    item['solutions'] = partner['dspppartnersolutions'] if 'dspppartnersolutions' in partner and partner['dspppartnersolutions'] else ''
                    if item['solutions'] != '':
                        item['solutions'] = clean_lst(item['solutions'])
                        if 'Network' in item['solutions']:
                            item['solutions'].pop(item['solutions'].index('Network'))
                        if 'DSPP Partner Solution' in item['solutions']:
                            item['solutions'].pop(item['solutions'].index('DSPP Partner Solution'))

                    item['industries'] = partner['industry'] if 'industry' in partner and partner['industry'] else ''
                    if item['industries'] != '':
                        item['industries'] = clean_lst(item['industries'])
                        if 'Industry' in item['industries']:
                            item['industries'].pop(item['industries'].index('Industry'))

                    item['products'] = partner['productcategory'] if 'productcategory' in partner and partner['productcategory'] else ''
                    if item['products'] != '':
                        item['products'] = clean_lst(item['products'])
                        if 'Product Category' in item['products']:
                            item['products'].pop(item['products'].index('Product Category'))

                    item['regions'] = partner['region'] if 'region' in partner and partner['region'] else ''
                    if item['regions'] != '':
                        item['regions'] = clean_lst(item['regions'])
                        if 'Region' in item['regions']:
                            item['regions'].pop(item['regions'].index('Region'))

                    updated = partner['updated'] if 'updated' in partner and partner['updated'] else ''
                    if updated != '':
                        item['latest_update'] = datetime.datetime.fromtimestamp(int(updated)/1000).strftime("%d %B %Y")

                    website_tag = partner['website'] if 'website' in partner and partner['website'] else partner['partnerz32xlogoz32xlink'] if 'partnerz32xlogoz32xlink' in partner and partner['partnerz32xlogoz32xlink'] else None
                    if website_tag:
                        try:
                            website_link = BS(website_tag, "html.parser").find('link')['url']
                        except:
                            website_link = ''

                        item['company_domain_name'] = website_link
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

                    yield item

                # follow next pages
                if page_number == 0 and 'totalCount' in jdata:
                    total = jdata['totalCount']
                    last_page = math.ceil(total / 5.0)
                    for i in range(1, last_page+1):
                        page_number = i * 5
                        api_link = 'https://www.al-enterprise.com/coveo/rest/v2/?sitecoreItemUri=sitecore://web/{28C23767-10FD-4A46-BB9A-9CCE16C70A9A}?lang=en&ver=3&siteName=ALE'
                        payload = f'aq=(((%40audience%20NOT%20%22Audience%2FInternal%22%20AND%20%40audience%20NOT%20%22Audience%2FState%22AND%20%40audience%20NOT%20%22Audience%2FPartners%22AND%20%40audience%20NOT%20%22Audience%2FExtranet%22AND%20(%40hidefromsearch%20%3D%3D%20%22false%22)AND%20%40contenttype%20NOT%20%22Content%20Type%2FGraphics%20and%20Banners%2FAdvertisements%22AND%20%40contenttype%20NOT%20%22Content%20Type%2FGraphics%20and%20Banners%2FAnimated%20GIF%22AND%20%40contenttype%20NOT%20%22Content%20Type%2FGraphics%20and%20Banners%2FIcon%22AND%20%40contenttype%20NOT%20%22Content%20Type%2FGraphics%20and%20Banners%2FLogo%22AND%20%40contenttype%20NOT%20%22Content%20Type%2FGraphics%20and%20Banners%2FThumbnail%20images%22AND%20%40contenttype%20NOT%20%22Content%20Type%2FGraphics%20and%20Banners%2FWeb%20banners%22AND%20%40contenttype%20NOT%20%22Content%20Type%2FReferences%2FCustomer%20Logo%22AND%20%40contenttype%20NOT%20%22Content%20Type%2FReferences%2FCustomer%20Support%20Image%22)%20OR%20NOT%20%40audience)%20NOT%20(%40z95xtemplatename%20%3D%3D%20%22Image%20Asset%22)%20NOT%20(%40filetype%20%3D%3D%20%22sitecoreitem%22)%20AND%20%40sitename%20%3D%3D%20%22ale%22%20AND%20%40sitename%20NOT%20%22ale%20intranet%22)%20(%40syssource%3D%3D(%22Coveo_web_index%20-%20ALE-Redesign%22)%20NOT%20%40templateid%3D%3D(%22adb6ca4f-03ef-4f47-b9ac-9ce2ba53ff97%22%2C%22fe5dd826-48c6-436d-b87a-7c4210c7413b%22))&cq=(%40z95xlatestversion%3D%3D%221%22)&searchHub=Partnerships&language=en&pipeline=DSPP-Pipeline&firstResult={page_number}&numberOfResults=5&excerptLength=200&enableDidYouMean=true&sortCriteria=fieldascending&sortField=%40header&queryFunctions=%5B%5D&rankingFunctions=%5B%5D&groupBy=%5B%7B%22field%22%3A%22%40dspppartnersolutions%22%2C%22maximumNumberOfValues%22%3A10001%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A10000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40apis%22%2C%22maximumNumberOfValues%22%3A10001%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A10000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40industry%22%2C%22maximumNumberOfValues%22%3A10001%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A10000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40dspppartnerlevel%22%2C%22maximumNumberOfValues%22%3A10001%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A10000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40region%22%2C%22maximumNumberOfValues%22%3A10001%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A10000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%5D&retrieveFirstSentences=true&timezone=Africa%2FCairo&disableQuerySyntax=false&enableDuplicateFiltering=false&enableCollaborativeRating=false&debug=false&context=%7B%7D'
                        yield scrapy.Request(method='POST', url=api_link, body=payload,
                                             callback=self.parse, headers=self.headers,
                                             meta={'page_number': page_number})
