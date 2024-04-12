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
    name = 'docusignpartner'
    partner_program_link = 'https://partners.docusign.com/s/partnerfinder'
    partner_directory = 'Docusign Partner Directory'
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
               'cookie': 'renderCtx={"pageId":"8b50129c-a3a7-46fb-8b00-bbb9e38dbbc0","schema":"Published","viewType":"Published","brandingSetId":"a2d720e2-6577-46a7-a5ce-b2ff023bda0a","audienceIds":"6Au1W0000004CDy"}; optimizelyEndUserId=oeu1652774805415r0.9747643409178663; ds_a=1043c061-86b8-4574-bab4-62d756ed52c1; _gcl_au=1.1.1831827424.1652774510; _fbp=fb.1.1652774510249.101890221; _gid=GA1.2.1362927357.1652774510; fs_uid=rs.fullstory.com#12BP4E#5612483662319616:6341997640028160/1684310510; nmstat=05c4d56f-3491-e038-1af4-2d52fbca1a9c; _uetsid=aa70a780d5b711ec9d876b0aed1ead07; _uetvid=aa70cde0d5b711ec9466cf753556f310; _rdt_uuid=1652774535731.20ca2598-3f6d-47bc-9b95-6513dd5c6b41; _ga=GA1.2.716787314.1652774510; _clck=8t51vc|1|f1j|0; _clsk=iki0fg|1652774541289|1|1|f.clarity.ms/collect; _ga_1TZ7S9D6BQ=GS1.1.1652774510.1.1.1652774644.60; CookieConsentPolicy=0:1; LSKey-c$CookieConsentPolicy=0:1; CookieConsentPolicy=0:0; LSKey-c$CookieConsentPolicy=0:0; OptanonConsent=isIABGlobal=false&datestamp=Wed+May+18+2022+09:10:46+GMT+0200+(Eastern+European+Standard+Time)&version=6.23.0&hosts=&consentId=b321cecf-540c-42ba-819b-0fdbd7ac8321&interactionCount=1&landingPath=NotLandingPage&groups=C0001:1,C0003:1,C0002:1,C0004:1&AwaitingReconsent=false; _gat=1; pctrk=100d0b99-25c8-478c-b723-1ab006424b4c; sfdc-stream=!lHvnXI6vMuqtVxgrJs/gXPyK2yzb5jzrsEHZ8S3JPUHP7DssiqOExHEjDDS+VfGA0Ihx0l+xxSthMw==',
               'Host': 'partners.docusign.com',
               'Accept': '*/*',
               'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
               'Accept-Encoding': 'gzip, deflate, br',
               'cache-control': 'no-cache',
               'pragma': 'no-cache',
               'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
               'Referer': 'https://partners.docusign.com/s/partnerfinder',
               'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
               'Origin': 'https://partners.docusign.com',
               'Connection': 'keep-alive',
               'sec-ch-ua-mobile': '?0',
               'sec-ch-ua-platform': 'Windows',
               'Sec-Fetch-Dest': 'empty',
               'Sec-Fetch-Mode': 'cors',
               'Sec-Fetch-Site': 'same-origin',
               'X-SFDC-Page-Cache': '7cbb8198994c8e21',
               'X-SFDC-Page-Scope-Id': '0d067240-7642-4d63-98c8-459da12e9dd7',
               'X-SFDC-Request-Id': '3343100000cbdad76f'
               }

    def start_requests(self):

        data = 'message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22105%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_ImageCarouselController%22%2C%22method%22%3A%22retrieveCurrentSpotlights%22%2C%22params%22%3A%7B%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22106%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_SearchResultsController%22%2C%22method%22%3A%22retrieveResults%22%2C%22params%22%3A%7B%22searchRequestJson%22%3A%22%7B%5C%22keywords%5C%22%3A%5B%5D%2C%5C%22allKeywords%5C%22%3Atrue%2C%5C%22filterIds%5C%22%3A%5B%5D%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22107%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_SearchCategoriesController%22%2C%22method%22%3A%22retrieveCategoriesWithFilters%22%2C%22cacheable%22%3Atrue%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%222yRFfs4WfGnFrNGn9C_dGg%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22PAjEh9HEIZmsDpK-Y8SVTg%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fs%2Fpartnerfinder&aura.token=null'
        yield scrapy.Request(method='POST', url='https://partners.docusign.com/s/sfsites/aura?r=1&aura.ApexAction.execute=3',
                             body=data, callback=self.parse, headers=self.headers)

    def parse(self, response):
        if response.status != 200:
            print(response.status)
            print(response.text)
            return

        data = json.loads(response.text)
        if 'actions' in data:
            for d in data['actions']:
                if d['id'] == "106;a":
                    profiles = d['returnValue']['returnValue']
                    for profile in profiles:
                        if 'sobjType' in profile and profile['sobjType'] == 'Partner_Profile__c':
                            partner_id = profile['record']['Id']
                            partner_link = 'https://partners.docusign.com/s/sfsites/aura?r=20&ui-comm-runtime-components-aura-components-siteforce-qb.Quarterback.validateRoute=1&ui-communities-components-aura-components-forceCommunity-seoAssistant.SeoAssistant.getRecordAndTranslationData=1&ui-force-components-controllers-recordGlobalValueProvider.RecordGvp.getRecord=2'
                            data = f'message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22578%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_BreadcrumbsController%22%2C%22method%22%3A%22retrievePartnerProfile%22%2C%22params%22%3A%7B%22recordId%22%3A%22{partner_id}%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22579%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_DynamicFieldController%22%2C%22method%22%3A%22retrieveFieldInfoResultJson%22%2C%22params%22%3A%7B%22fieldInfoReqJson%22%3A%22%7B%5C%22objectApiName%5C%22%3A%5C%22Partner_Profile__c%5C%22%2C%5C%22recordId%5C%22%3A%5C%22{partner_id}%5C%22%2C%5C%22fieldApiName%5C%22%3A%5C%22Company_Logo__c%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22580%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_DynamicFieldController%22%2C%22method%22%3A%22retrieveFieldInfoResultJson%22%2C%22params%22%3A%7B%22fieldInfoReqJson%22%3A%22%7B%5C%22objectApiName%5C%22%3A%5C%22Partner_Profile__c%5C%22%2C%5C%22recordId%5C%22%3A%5C%22{partner_id}%5C%22%2C%5C%22fieldApiName%5C%22%3A%5C%22Name%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22581%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_DynamicFieldController%22%2C%22method%22%3A%22retrieveFieldInfoResultJson%22%2C%22params%22%3A%7B%22fieldInfoReqJson%22%3A%22%7B%5C%22objectApiName%5C%22%3A%5C%22Partner_Profile__c%5C%22%2C%5C%22recordId%5C%22%3A%5C%22{partner_id}%5C%22%2C%5C%22fieldApiName%5C%22%3A%5C%22Company_Tagline__c%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22582%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_DynamicFieldController%22%2C%22method%22%3A%22retrieveFieldInfoResultJson%22%2C%22params%22%3A%7B%22fieldInfoReqJson%22%3A%22%7B%5C%22objectApiName%5C%22%3A%5C%22Partner_Profile__c%5C%22%2C%5C%22recordId%5C%22%3A%5C%22{partner_id}%5C%22%2C%5C%22fieldApiName%5C%22%3A%5C%22PRM_ContactUsLink__c%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22583%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_DynamicFieldController%22%2C%22method%22%3A%22retrieveFieldInfoResultJson%22%2C%22params%22%3A%7B%22fieldInfoReqJson%22%3A%22%7B%5C%22objectApiName%5C%22%3A%5C%22Partner_Profile__c%5C%22%2C%5C%22recordId%5C%22%3A%5C%22{partner_id}%5C%22%2C%5C%22fieldApiName%5C%22%3A%5C%22PRM_PartnerType__c%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22584%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_DynamicFieldController%22%2C%22method%22%3A%22retrieveFieldInfoResultJson%22%2C%22params%22%3A%7B%22fieldInfoReqJson%22%3A%22%7B%5C%22objectApiName%5C%22%3A%5C%22Partner_Profile__c%5C%22%2C%5C%22recordId%5C%22%3A%5C%22{partner_id}%5C%22%2C%5C%22fieldApiName%5C%22%3A%5C%22PRM_Partner_Level__c%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22585%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_DynamicFieldController%22%2C%22method%22%3A%22retrieveFieldInfoResultJson%22%2C%22params%22%3A%7B%22fieldInfoReqJson%22%3A%22%7B%5C%22objectApiName%5C%22%3A%5C%22Partner_Profile__c%5C%22%2C%5C%22recordId%5C%22%3A%5C%22{partner_id}%5C%22%2C%5C%22fieldApiName%5C%22%3A%5C%22Company_and_Partnership_Summary__c%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22586%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_DynamicFieldController%22%2C%22method%22%3A%22retrieveFieldInfoResultJson%22%2C%22params%22%3A%7B%22fieldInfoReqJson%22%3A%22%7B%5C%22objectApiName%5C%22%3A%5C%22Partner_Profile__c%5C%22%2C%5C%22recordId%5C%22%3A%5C%22{partner_id}%5C%22%2C%5C%22fieldApiName%5C%22%3A%5C%22Key_Benefits__c%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22587%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_DynamicFieldController%22%2C%22method%22%3A%22retrieveFieldInfoResultJson%22%2C%22params%22%3A%7B%22fieldInfoReqJson%22%3A%22%7B%5C%22objectApiName%5C%22%3A%5C%22Partner_Profile__c%5C%22%2C%5C%22recordId%5C%22%3A%5C%22{partner_id}%5C%22%2C%5C%22fieldApiName%5C%22%3A%5C%22Customer_Testimonials__c%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22588%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_DynamicFieldController%22%2C%22method%22%3A%22retrieveFieldInfoResultJson%22%2C%22params%22%3A%7B%22fieldInfoReqJson%22%3A%22%7B%5C%22objectApiName%5C%22%3A%5C%22Partner_Profile__c%5C%22%2C%5C%22recordId%5C%22%3A%5C%22{partner_id}%5C%22%2C%5C%22fieldApiName%5C%22%3A%5C%22Company_Website__c%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22589%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_ImageCarouselController%22%2C%22method%22%3A%22retrieveSolutionsByPartner%22%2C%22params%22%3A%7B%22profId%22%3A%22{partner_id}%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22590%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_ImageCarouselController%22%2C%22method%22%3A%22retrieveScreenshots%22%2C%22params%22%3A%7B%22recId%22%3A%22{partner_id}%22%2C%22objName%22%3A%22Partner_Profile__c%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22591%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_DynamicFieldController%22%2C%22method%22%3A%22retrieveFieldInfoResultJson%22%2C%22params%22%3A%7B%22fieldInfoReqJson%22%3A%22%7B%5C%22objectApiName%5C%22%3A%5C%22Partner_Profile__c%5C%22%2C%5C%22recordId%5C%22%3A%5C%22{partner_id}%5C%22%2C%5C%22fieldApiName%5C%22%3A%5C%22PRM_Awards__c%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22592%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_DynamicFieldController%22%2C%22method%22%3A%22retrieveFieldInfoResultJson%22%2C%22params%22%3A%7B%22fieldInfoReqJson%22%3A%22%7B%5C%22objectApiName%5C%22%3A%5C%22Partner_Profile__c%5C%22%2C%5C%22recordId%5C%22%3A%5C%22{partner_id}%5C%22%2C%5C%22fieldApiName%5C%22%3A%5C%22PRM_Practitioners__c%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22593%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_DynamicFieldController%22%2C%22method%22%3A%22retrieveFieldInfoResultJson%22%2C%22params%22%3A%7B%22fieldInfoReqJson%22%3A%22%7B%5C%22objectApiName%5C%22%3A%5C%22Partner_Profile__c%5C%22%2C%5C%22recordId%5C%22%3A%5C%22{partner_id}%5C%22%2C%5C%22fieldApiName%5C%22%3A%5C%22Resources__c%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22594%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_DynamicFieldController%22%2C%22method%22%3A%22retrieveFieldInfoResultJson%22%2C%22params%22%3A%7B%22fieldInfoReqJson%22%3A%22%7B%5C%22objectApiName%5C%22%3A%5C%22Partner_Profile__c%5C%22%2C%5C%22recordId%5C%22%3A%5C%22{partner_id}%5C%22%2C%5C%22fieldApiName%5C%22%3A%5C%22PRM_DocuSignProduct__c%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22595%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_DynamicFieldController%22%2C%22method%22%3A%22retrieveFieldInfoResultJson%22%2C%22params%22%3A%7B%22fieldInfoReqJson%22%3A%22%7B%5C%22objectApiName%5C%22%3A%5C%22Partner_Profile__c%5C%22%2C%5C%22recordId%5C%22%3A%5C%22{partner_id}%5C%22%2C%5C%22fieldApiName%5C%22%3A%5C%22PRM_Industry__c%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22596%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_DynamicFieldController%22%2C%22method%22%3A%22retrieveFieldInfoResultJson%22%2C%22params%22%3A%7B%22fieldInfoReqJson%22%3A%22%7B%5C%22objectApiName%5C%22%3A%5C%22Partner_Profile__c%5C%22%2C%5C%22recordId%5C%22%3A%5C%22{partner_id}%5C%22%2C%5C%22fieldApiName%5C%22%3A%5C%22Platform_Specialization__c%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22597%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_DynamicFieldController%22%2C%22method%22%3A%22retrieveFieldInfoResultJson%22%2C%22params%22%3A%7B%22fieldInfoReqJson%22%3A%22%7B%5C%22objectApiName%5C%22%3A%5C%22Partner_Profile__c%5C%22%2C%5C%22recordId%5C%22%3A%5C%22{partner_id}%5C%22%2C%5C%22fieldApiName%5C%22%3A%5C%22PRM_SupportedLanguages__c%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22598%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_DynamicFieldController%22%2C%22method%22%3A%22retrieveFieldInfoResultJson%22%2C%22params%22%3A%7B%22fieldInfoReqJson%22%3A%22%7B%5C%22objectApiName%5C%22%3A%5C%22Partner_Profile__c%5C%22%2C%5C%22recordId%5C%22%3A%5C%22{partner_id}%5C%22%2C%5C%22fieldApiName%5C%22%3A%5C%22PRM_Regions__c%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%2C%7B%22id%22%3A%22599%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PRM_DynamicFieldController%22%2C%22method%22%3A%22retrieveFieldInfoResultJson%22%2C%22params%22%3A%7B%22fieldInfoReqJson%22%3A%22%7B%5C%22objectApiName%5C%22%3A%5C%22Partner_Profile__c%5C%22%2C%5C%22recordId%5C%22%3A%5C%22{partner_id}%5C%22%2C%5C%22fieldApiName%5C%22%3A%5C%22PRM_Countries__c%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%222yRFfs4WfGnFrNGn9C_dGg%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22PAjEh9HEIZmsDpK-Y8SVTg%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fs%2Fpartner-profile%2F{partner_id}%2Fdetail&aura.token=null'

                            # Initialize item
                            item = dict()
                            for k in self.item_fields:
                                item[k] = ''

                            item['partner_program_link'] = self.partner_program_link
                            item['partner_directory'] = self.partner_directory
                            item['partner_program_name'] = self.partner_program_name
                            item['partner_company_name'] = profile['record']['Name']
                            item['partner_type'] = profile['record']['PRM_PartnerType__c'] if 'PRM_PartnerType__c' in profile['record'] else ''
                            item['partner_tier'] = profile['record']['PRM_Partner_Level__c'] if 'PRM_Partner_Level__c' in profile['record'] else ''

                            self.headers['Referer'] = f'https://partners.docusign.com/s/partner-profile/{partner_id}/detail'
                            yield scrapy.Request(method='POST',
                                                 url=partner_link,
                                                 body=data, callback=self.parse_partner, headers=self.headers,
                                                 meta={'item': item})

    def parse_partner(self, response):
        item = response.meta['item']

        if response.status != 200:
            print(response.status)
            print(response.text)
            return

        data = json.loads(response.text)
        desc_html = None
        for partial_data in data['actions']:
            if type(partial_data['returnValue']['returnValue']) == str:
                dic = json.loads(partial_data['returnValue']['returnValue'])
                if 'record' in dic:
                    record = dic['record']
                    if item['company_domain_name'] == '' and 'Company_Website__c' in record:
                        item['company_domain_name'] = record['Company_Website__c'] if record['Company_Website__c'] else ''
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

                    if item['company_description'] == '' and 'Company_and_Partnership_Summary__c' in record:
                        item['company_description'] = cleanhtml(record['Company_and_Partnership_Summary__c'])
                    if item['regions'] == '' and 'PRM_Regions__c' in record:
                        item['regions'] = record['PRM_Regions__c']

                    if item['industries'] == '' and 'PRM_Industry__c' in record:
                        item['industries'] = record['PRM_Industry__c']

                    if item['specializations'] == '' and 'Platform_Specialization__c' in record:
                        item['specializations'] = record['Platform_Specialization__c']

                    if item['languages'] == '' and 'PRM_SupportedLanguages__c' in record:
                        item['languages'] = record['PRM_SupportedLanguages__c']

                    if item['locations_country'] == '' and 'PRM_Countries__c' in record:
                        item['locations_country'] = record['PRM_Countries__c']

        lines = [x.strip() for x in item['company_description'].splitlines()]
        for txt in lines:
            if 'linkedin.com/' in txt:
                tmp = txt[txt.find('linkedin.com/'):]
                item['linkedin_link'] = tmp[:tmp.find(' ')]
            if 'twitter.com/' in txt:
                tmp = txt[txt.find('twitter.com/'):]
                item['twitter_link'] = tmp[:tmp.find(' ')]

            if 'facebook.com/' in txt:
                tmp = txt[txt.find('facebook.com/'):]
                item['facebook_link'] = tmp[:tmp.find(' ')]
            if 'instagram.com/' in txt:
                tmp = txt[txt.find('instagram.com/'):]
                item['instagram_link'] = tmp[:tmp.find(' ')]
            if 'youtube.com/' in txt:
                tmp = txt[txt.find('youtube.com/'):]
                item['youtube_link'] = tmp[:tmp.find(' ')]

            if ('@' + item['company_domain_name']) in txt:
                tmp = txt[:txt.find('@' + item['company_domain_name'])]
                item['general_email_address'] = tmp[tmp.rfind(' ')+1 if ' ' in tmp else 0:] + ('@' + item['company_domain_name'])

        if item['locations_country'] != '':
            country_lst = item['locations_country'].split(';')
            for c in country_lst:
                item['locations_country'] = c
                yield item
        else:
            yield item

