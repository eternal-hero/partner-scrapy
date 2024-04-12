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
    name = 'intelmanufacturer'
    partner_program_link = 'https://marketplace.intel.com/s/pmp-partner-program/a723b0000008PI6AAM/manufacturer?language=en_US'
    partner_directory = 'Intel Manufacturer Partner'
    partner_program_name = ''
    crawl_id = None

    custom_settings = {
        'DOWNLOAD_DELAY': 0.5,
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
        'cookie': 'renderCtx={"pageId":"faab079b-da30-4950-9474-83b3e47e8ec6","schema":"Published","viewType":"Published","brandingSetId":"a0c9d2f5-91c6-42ab-8ccd-016fbde06821","audienceIds":"6Au3b000000k9xR"}; CookieConsentPolicy=1:1; LSKey-c$CookieConsentPolicy=1:1; MP_URL_Tracking=https://marketplace.intel.com/s/pmp-partner-program/a723b0000008PI6AAM/manufacturer?language=en_US; RT="z=1&dm=intel.com&si=dge6ph0u655&ss=lbeso1wl&sl=0&tt=0"; detected_bandwidth=HIGH; src_countrycode=EG; sfdc-stream=!AhRbZGptLQqUC0cYiIfvrrQC/ce18+hDSi+XWW5tOGAP3tHnlqzLWjxTdhtezPm8QaJlrGtjPdQ3cw==; force-proxy-stream=!M3U8ilDUXXX1VkhEjHBMM0SzCZR834DQ7p1Y29uj0yujMr1CrRpGaGLOwexAqeEeNIoQFcwZ62Gds2s=; force-stream=!AhRbZGptLQqUC0cYiIfvrrQC/ce18+hDSi+XWW5tOGAP3tHnlqzLWjxTdhtezPm8QaJlrGtjPdQ3cw==; bm_sz=5697DC920483BB090EFB510E9D072000~YAAQyHRZaK1Y0N2EAQAAoF3G8BKlWYaal5nj+07TsrlUxW0WNTpNYiNnY32DRX7d2IlJTl34AFMI1TK9Yjmo84rjR0sxVpXo3qsBClBPCGIhQ6yLGsHRqMuTVS8RQe8jdZMEz9q6z4dq83zzj9/d6UNjPTJQRM6ZxxrUeeOY/shL/ANdgYNZp80KaxXBcO8EEj+hVUFmWVTqaWw0DKR4dfjt4LiLCq7sgdAWPgqDcEaisa6sqsvnB7KqaIZ2lU6MCe53U8ITLWj5UL5FUIuThP3tsvyb3ldyH7XuY3+yIWFSpA==~3420977~3684419; bm_mi=7B5C9CEF85C74B3BBE4648CA1F59C24F~YAAQyHRZaG1Z0N2EAQAAHGLG8BLCsDrSsZeNwA5RV2xNXzOhNf6bggS/wgMB5BZRfEBa9Ixx4tgAxA+HKowL2Q8O8pnccyzyRRMn4cJ4LiYfUKi/grSzpLBVLJHJ1eXe5E6VW8AQz3vWY8bNmg7WMaXlEp2SN4hi5tyTvf5cOsIJnI0igAjtSwyIOTmLGkms6TFCF5z1R4BEsOHhmBbIy6E0DWJN5Z33q4BPBJRsD2qM6CdwD89gE2HqSciWkUQI8gAaT+RL9R4DOtahlK7ehb2QmNqR0BaVxu0zfmtRWtkkNwfcgkTHgblzd1zYRzcRaoXYq02fVWRgNiqN87Gm5EczJalj42LhhzRmFhj2MoCJXX24tXG50Ujox1r1Yjm3gVI7~1; bm_sv=B4B3D9F2CE51B55427C734917B1D917E~YAAQyHRZaG5Z0N2EAQAAHGLG8BKgJaZAWq3cUvSVpkByGq8AIdk4uOIwMRqr2nmMtRgZ8kgGl4aqlnhZQ93LupD8iISibXe1ku6WrD5FMxPtjjfD8Zcr+SWDjfRbKMFh0VGcEWwrGJU9CSCWiqhqJSM9txDDBEmRYK2/B+EVlcS4ajhpoo0/TpsAk3cPxwZavLXMb/gwTxa2c6RzAbBKcitlNZWuI18MVqBe+oGAZIssAm60JjKvp0NUixvLMGI=~1; utag_main=v_id:0184f0c4b9810015da0ec1e442430506f006206700bd0$_sn:1$_se:2$_ss:0$_st:1670488535576$ses_id:1670486735234;exp-session$_pn:1;exp-session; _cs_mk=0.027298334838445237_1670486735581; intelresearchREF=NONE; wap_new_session=1; _cs_c=0; _cs_id=594825aa-2e87-af05-bc32-9621eb3af897.1670486736.1.1670486736.1670486736.1589385054.1704650736008; _abck=FCC70F1A72ABAB48563D6832A284E76F~0~YAAQyHRZaCNa0N2EAQAAfGXG8AlBq1OvwxmKVP7TMyecZ/4KM+bpJfdT4QvPKjyjz5aTzOLayOsyETJTTBSrIDV7xFT5lFzKK8PCDruPgEJRiX/35bpdA/SD4Wk3YwBcSM9otJmjaUmZxTEtKfoZe6v2xO+u0lxwJaLHCUwUIGSnIvKT5jQybMVibQg3AOcFKPVIO0WUosSfbKeJzCp7FuZO3nwcctcQPaoO2fRNlpHFJFkGRSA51KmerPt7FnIPs/0SHaz0fMczbCS5Ikx/4mSpf6RvL6OttjDV4TbcJsbI1TDuW+MMh8pGH7RL2IlhA81TXUnUZn1HSmx9qUnvcf281pwRntsy7xaiP6qsR/dQhD9GIii1NoMp3FDXPA3mAlsXUE7UkTdG/cSOSAw2u2H/JSg/f30=~-1~||-1||~-1; AMCVS_AD2A1C8B53308E600A490D4D@AdobeOrg=1; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Dec+08+2022+10:05:36+GMT+0200+(Eastern+European+Standard+Time)&version=202209.2.0&isIABGlobal=false&hosts=&consentId=3bbc14f3-1ff9-4413-be64-484bcb6d627a&interactionCount=0&landingPath=https://marketplace.intel.com/s/pmp-partner-program/a723b0000008PI6AAM/manufacturer?language=en_US&groups=C0001:1,C0003:1,C0004:1,C0002:1; ak_bmsc=1EAB39EC72ACAF2FD4AD60345487D38C~000000000000000000000000000000~YAAQyHRZaDla0N2EAQAAC2bG8BLHrXB3oMGpjX1/NutdGVsBlmPiYYkhS8J0PPrNllkIy+tb/4fuXSsPQKfjnp9Esqfq0Rb1wJLt9/dnIHou25G+QRzg/ghWoyd9SF3LZjjXXKMpkzFWierPFkC2BjwaF2S6+aYvRP2PRxavJiIqIahGTAYOuwNg8tDJJKA3Lxix/GZgSBd3sUE6HNyvQGpCkoTyBk2MOHTas2JGqtQAUR5TWe+70RxLR4TqpjfD8E0CbpT8Hiyg2mNxCNETAaplTMP8JE0Bw7VO3I2hf8/JbD+/+B9Taj7cSv8n3nOdeT/8hc4V1uCAp3bgqlqry83Fb7DWW9RbPRoFNoyrM8P6W78GqMEkLD+8Zp/474p1l3ezYQZX2bk5klxecWLBsxKpn1pZ; ELOQUA=GUID=FABDB928F50A45868E94E6EF81A19A24; ELQSTATUS=OK; _cs_s=1.5.0.1670488536657; AMCV_AD2A1C8B53308E600A490D4D@AdobeOrg=1585540135|MCIDTS|19335|MCMID|83871086431271319143848837066517520115|MCAAMLH-1671091536|6|MCAAMB-1671091536|6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y|MCOPTOUT-1670493936s|NONE|MCSYNCSOP|411-19342|vVersion|4.4.0; kndctr_AD2A1C8B53308E600A490D4D_AdobeOrg_identity=CiY4Mzg3MTA4NjQzMTI3MTMxOTE0Mzg0ODgzNzA2NjUxNzUyMDExNVIPCLnQmYbPMBgBKgRJUkwx8AG50JmGzzA=; kndctr_AD2A1C8B53308E600A490D4D_AdobeOrg_cluster=irl1; intelresearchUID=5469193720673M1670486841118; intelresearchSID=5469193720673M1670486841118; intelresearchSTG=16',
        'Origin': 'https://marketplace.intel.com',
        'Referer': 'https://marketplace.intel.com/s/pmp-partner-program/a723b0000008PI6AAM/manufacturer?language=en_US',
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
        payload = f'message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22403%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PMP_SearchUtilityNew%22%2C%22method%22%3A%22newSearch%22%2C%22params%22%3A%7B%22requestJSON%22%3A%22%7B%5C%22category%5C%22%3A%5C%22a723b0000008PI6AAM%5C%22%2C%5C%22keyword%5C%22%3A%5C%22%5C%22%2C%5C%22page%5C%22%3A{page}%2C%5C%22sortType%5C%22%3A%5C%22%5C%22%2C%5C%22filters%5C%22%3A%7B%7D%2C%5C%22searchType%5C%22%3A%5C%22pmp-partner-program%5C%22%2C%5C%22langCode%5C%22%3A%5C%22en-US%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22tr2UlkrAHzi37ijzEeD2UA%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22wlLnZvhAshR7p-QD7LB6JQ%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11yCoreCollector%22%3A%228giBLfYbOC17LwOopJh9VQ%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3ArecordDetail%22%3A%22wPFVpZB_W4msvEbsp4rY4g%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3AforceCommunityFeed%22%3A%221HH7K53dNQsb5NNDobHtcA%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3AfeedPublisher%22%3A%22oi0RxomnI2XIwaO6mWGejg%22%2C%22COMPONENT%40markup%3A%2F%2Fforce%3AoutputField%22%3A%22gBPfAfDNBQarQOsYi9u3dQ%22%2C%22COMPONENT%40markup%3A%2F%2FforceChatter%3AfeedQbProxy%22%3A%22_oHZaul6rjPu1puZzWDkQg%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fs%2Fpmp-partner-program%2Fa723b0000008PI6AAM%2Fmanufacturer%3Flanguage%3Den_US&aura.token=null'
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
                        results_per_page = jsn_data2['resultsPerPage']
                    else:
                        raise Exception
                except:
                    jsn_data2 = None
                    partners = list()
                    results_per_page = None

                if jsn_data2 and len(partners) > 0:
                    self.logger.info(f'Intel manufacturer Page: {page}, partners = {len(partners)}')
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
                        partner_id = partner['id']

                        payload = f'message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22202%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PMP_MarketplaceNewPartnerDetail%22%2C%22method%22%3A%22getPartnerDetails%22%2C%22params%22%3A%7B%22partnerId%22%3A%22{partner_id}%22%2C%22langCode%22%3A%22en_US%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22tr2UlkrAHzi37ijzEeD2UA%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22wlLnZvhAshR7p-QD7LB6JQ%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11yCoreCollector%22%3A%228giBLfYbOC17LwOopJh9VQ%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fs%2Fpartner%2F{partner_id}%3Flanguage%3Den_US&aura.token=null'
                        yield scrapy.Request(method='POST', url=self.api_link,
                                             callback=self.parse_partner,
                                             headers=self.headers,
                                             body=payload,
                                             meta={'item': item},
                                             dont_filter=True)

                    # follow next page
                    if results_per_page and len(partners) == results_per_page:
                        page += 1
                        payload = f'message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22403%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PMP_SearchUtilityNew%22%2C%22method%22%3A%22newSearch%22%2C%22params%22%3A%7B%22requestJSON%22%3A%22%7B%5C%22category%5C%22%3A%5C%22a723b0000008PI6AAM%5C%22%2C%5C%22keyword%5C%22%3A%5C%22%5C%22%2C%5C%22page%5C%22%3A{page}%2C%5C%22sortType%5C%22%3A%5C%22%5C%22%2C%5C%22filters%5C%22%3A%7B%7D%2C%5C%22searchType%5C%22%3A%5C%22pmp-partner-program%5C%22%2C%5C%22langCode%5C%22%3A%5C%22en-US%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22tr2UlkrAHzi37ijzEeD2UA%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22wlLnZvhAshR7p-QD7LB6JQ%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11yCoreCollector%22%3A%228giBLfYbOC17LwOopJh9VQ%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3ArecordDetail%22%3A%22wPFVpZB_W4msvEbsp4rY4g%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3AforceCommunityFeed%22%3A%221HH7K53dNQsb5NNDobHtcA%22%2C%22COMPONENT%40markup%3A%2F%2FforceCommunity%3AfeedPublisher%22%3A%22oi0RxomnI2XIwaO6mWGejg%22%2C%22COMPONENT%40markup%3A%2F%2Fforce%3AoutputField%22%3A%22gBPfAfDNBQarQOsYi9u3dQ%22%2C%22COMPONENT%40markup%3A%2F%2FforceChatter%3AfeedQbProxy%22%3A%22_oHZaul6rjPu1puZzWDkQg%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fs%2Fpmp-partner-program%2Fa723b0000008PI6AAM%2Fmanufacturer%3Flanguage%3Den_US&aura.token=null'
                        yield scrapy.Request(method='POST', url=self.api_link,
                                             callback=self.parse,
                                             headers=self.headers,
                                             body=payload,
                                             dont_filter=True,
                                             meta={'page': page})

    def parse_partner(self, response):
        item = response.meta['item']

        save_flag = False

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {item["partner_company_name"]}')
        else:
            try:
                jsn_data = json.loads(response.text)
            except:
                jsn_data = None

            if jsn_data and 'actions' in jsn_data and len(jsn_data['actions']) > 0 and 'returnValue' in jsn_data['actions'][0] and 'returnValue' in jsn_data['actions'][0]['returnValue']:
                partner = jsn_data['actions'][0]['returnValue']['returnValue']
                partner_id = partner['id']

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

                # get products data
                try:
                    r = requests.post(url=self.api_link,
                                      data=f'message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22196%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22PMP_MarketplaceNewPartnerDetailFeed%22%2C%22method%22%3A%22getStorefrontFeedDetails%22%2C%22params%22%3A%7B%22partnerId%22%3A%22{partner_id}%22%2C%22langCode%22%3A%22en_US%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22tr2UlkrAHzi37ijzEeD2UA%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22wlLnZvhAshR7p-QD7LB6JQ%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11yCoreCollector%22%3A%228giBLfYbOC17LwOopJh9VQ%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fs%2Fpartner%2F{partner_id}%3Flanguage%3Den_US&aura.token=null',
                                      headers=self.headers
                                      )
                    if r and r.status_code == 200:
                        products = json.loads(r.text)['actions'][0]['returnValue']['returnValue']['cardData']
                        if len(products) > 0:
                            for product in products:
                                item['product_service_name'] = product['title']
                                item['product_service_description'] = cleanhtml(product['description'])
                                if not save_flag:
                                    save_flag = True
                                yield item
                except:
                    pass

        if not save_flag:
            yield item
