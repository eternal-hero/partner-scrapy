import codecs
import re
import urllib.parse
import json
import requests
import scrapy
from techpartners.spiders.base_spider import BaseSpider


class Spider(BaseSpider):
    name = 'citrixpartner'
    page_link = 'https://www.citrix.com/buy/partnerlocator/'

    custom_settings = {
        # 'DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 1,
    }

    done_ids = set()

    headers = {
        'authority': 'www.citrix.com',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
        'cookie': '_evga_cdca=ea10b4b7a88238fa.; _ga=GA1.2.732013460.1651257659; UTMReferralSources=direct|direct; _gcl_au=1.1.369481889.1651257660; insight_session=1115c73c-9c84-41f7-b1f3-a518bf260e50; UTMClientID=732013460.1651257659; _mkto_trk=id:989-BHO-046&token:_mch-citrix.com-1651257659647-76672; _fbp=fb.1.1651257660148.120324570; _rdt_uuid=1651257660305.8d003bdc-b4e4-4473-a0b5-064da42a6bc1; drift_aid=73a0d682-9770-48f2-9ffe-09405e776cf2; driftt_aid=73a0d682-9770-48f2-9ffe-09405e776cf2; UTMSessionCount=2; UTMsessionStart=true; notice_behavior=implied,us; _gid=GA1.2.270361310.1651659283; sa_uuid=310cc09e-c544-48d1-9a68-fe54830c5db1; insight_selfsvc_session=1651659283623; cebs=1; _ce.s=v~f1b4aba53ca628f6a7e7657a1089b68e7a8ecf2a~vpv~1; _clck=bbnpm1|1|f16|0; renderid=rend01; akamaiDataStored=true; drift_campaign_refresh=79b03168-17fd-472a-88a7-ae82af21d536; _clsk=hvd2tx|1651660396408|13|1|i.clarity.ms/collect; _gat_ctxswebmkt=1; insight_referer=aHR0cHM6Ly93d3cuY2l0cml4LmNvbS9idXkvcGFydG5lcmxvY2F0b3Iv; UTMpageviews=14; _uetsid=04a0f4f0cb9311ec8997e9c7afa83f1e; _uetvid=2434fb607bac11ecb2b11f8112303bf8; _gat=1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
        'referer': 'https://www.citrix.com/buy/partnerlocator/results.html',
        'sec-ch-ua': '"Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'x-requested-with': 'XMLHttpRequest',
    }

    item_fields = ['partner_program_link', 'partner_directory', 'partner_program_name', 'partner_company_name', 'product_service_name',
                   'company_domain_name', 'partner_type', 'partner_tier', 'company_description', 'product_service_description',
                   'headquarters_street', 'headquarters_city', 'headquarters_state', 'headquarters_zipcode', 'headquarters_country',
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

    def start_requests(self):
        cat_lst = ['CSP',
                   'System Integrator',
                   'CSN',
                   'ISV',
                   'Distributor',
                   'CSP Distributor',
                   'LAR',
                   ]

        for cat in cat_lst:
            url = f"https://www.citrix.com/buy/partnerlocator/partner-list/*/26.2093234,-80.1890287/?start=0&fq=(and (term field=partner_type '{cat}') )"
            yield scrapy.Request(url, callback=self.parse, headers=self.headers, dont_filter=True,
                                 meta={'cat': cat, 'start': 0})

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR PARSE REQUEST STATUS {response.status}, RESPONSE:')
            self.logger.info(response.text)
        else:
            cat = response.meta['cat']
            start = response.meta['start']
            data = response.json()
            if 'found' in data and 'fields' in data:
                if start == 0:
                    total_result = data['found']
                    self.logger.info(f"CAT: {cat}, Total Results: {total_result}")

                self.logger.info(f"CAT: {cat}, Start: {start}, Results: {len(data['fields'])}")
                for partner in data['fields']:
                    _id = ''
                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = self.page_link
                    item['partner_directory'] = 'Citrix Partner Locator'
                    # item['partner_program_name'] = ''

                    for key in partner.keys():
                        mod_key = key.replace(' ', '')

                        if mod_key == 'org_id':
                            _id = partner[key]

                        elif mod_key == 'company_name':
                            item['partner_company_name'] = partner[key]

                        elif mod_key == 'weblink_url':
                            item['company_domain_name'] = partner[key]
                            url_obj = urllib.parse.urlparse(item['company_domain_name'] if item['company_domain_name'] else '')
                            item['company_domain_name'] = (url_obj.netloc if url_obj.netloc != "" else url_obj.path)
                            if isinstance(item['company_domain_name'], (bytes, bytearray)):
                                item['company_domain_name'] = codecs.decode(item['company_domain_name'], 'UTF-8')
                            x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                            if x:
                                item['company_domain_name'] = x[-1]

                            if '/' in item['company_domain_name']:
                                item['company_domain_name'] = item['company_domain_name'][
                                                              :item['company_domain_name'].find('/')]

                        elif mod_key == 'specialist_in':
                            item['specializations'] = partner[key]

                        elif mod_key == 'partner_type':
                            item['partner_type'] = [itm['name'] for itm in partner[key].values()]

                        elif mod_key == 'auth_level':
                            item['partner_tier'] = partner[key]

                        elif mod_key == 'company_description':
                            if partner[key] != 'Company Description Not Provided':
                                item['company_description'] = partner[key]

                        elif mod_key == 'industries_served':
                            item['industries'] = partner[key]

                        elif mod_key == 'services_offered':
                            item['services'] = partner[key]

                        elif mod_key == 'country_code':
                            item['headquarters_country'] = partner[key]

                        elif mod_key == 'city':
                            item['headquarters_city'] = partner[key]

                        elif mod_key == 'zip':
                            item['headquarters_zipcode'] = partner[key]

                        elif mod_key == 'state':
                            item['headquarters_state'] = partner[key]

                        elif mod_key == 'address':
                            item['headquarters_street'] = partner[key]

                        elif mod_key == 'phone_area':
                            item['general_phone_number'] = partner[key] + ' ' + item['general_phone_number']

                        elif mod_key == 'phone_number':
                            item['general_phone_number'] += partner[key]

                        elif mod_key == 'weblink_email':
                            item['general_email_address'] = partner[key]

                        # elif mod_key == 'established_year':
                        #     item['year_founded'] = partner[key]

                        elif mod_key == 'partner_since':
                            item['partnership_founding_date'] = partner[key]

                        elif mod_key == 'products_certified':
                            item['certifications'] = partner[key]

                    if _id in self.done_ids:
                        continue
                    else:
                        self.done_ids.add(_id)
                        yield item

                # follow next page
                if len(data['fields']) >= 5 and start+5 < 10000:
                    start = start + (10 if start == 0 else 5)
                    url = f"https://www.citrix.com/buy/partnerlocator/partner-list/*/26.2093234,-80.1890287/?start={str(start)}&fq=(and (term field=partner_type '{cat}') )"
                    yield scrapy.Request(url, callback=self.parse, headers=self.headers,
                                         dont_filter=True, meta={'cat': cat, 'start': start})

            else:
                print(f'ERROR: {response.request.url}')
                print(data)
                yield scrapy.Request(response.request.url, callback=self.parse, headers=self.headers,
                                     dont_filter=True, meta=response.meta)
