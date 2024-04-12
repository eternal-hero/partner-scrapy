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
    name = 'procorepartner'
    partner_program_link = 'https://marketplace.procore.com/apps'
    partner_directory = 'Procore Partner Directory'
    partner_program_name = ''
    crawl_id = None

    start_urls = [partner_program_link]

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

    def parse(self, response):
        if response.status == 200:
            partners = list()
            soup = BS(response.text, "html.parser")
            script_divs = soup.find_all("script")
            for div in script_divs:
                txt = div.text
                if 'ReactDOM.render' in txt and 'apps:' in txt:
                    json_data = txt[txt.find('apps:'):txt.find('query:')].strip('apps:').strip().strip(',').strip()
                    try:
                        partners = json.loads(json_data)
                    except:
                        yield scrapy.Request(response.request.url, callback=self.parse, dont_filter=True)
                        return
                    break

            for partner in partners:
                if 'slug' in partner:
                    partner_link = 'https://marketplace.procore.com/apps/' + partner['slug']
                    yield scrapy.Request(partner_link, dont_filter=True, callback=self.parse_partner)
        else:
            self.logger.info(f'ERROR PAGE, STATUS: {response.status}, RESPONSE: {response.text}')
            yield scrapy.Request(response.request.url, callback=self.parse, dont_filter=True)

    def parse_partner(self, response):
        if response.status == 200:
            data = None
            soup = BS(response.text, "html.parser")
            script_divs = soup.find_all("script")
            for div in script_divs:
                txt = div.text
                if 'ReactDOM.render' in txt and 'app:' in txt:
                    json_data = txt[txt.find('app:'):txt.find('installationRequest:')].strip('app:').strip().strip(',').strip()
                    try:
                        data = json.loads(json_data)
                    except:
                        yield scrapy.Request(response.request.url, callback=self.parse, dont_filter=True)
                        return
                    break

            if not data:
                yield scrapy.Request(response.request.url, callback=self.parse, dont_filter=True)
                return

            partner = data

            # Initialize item
            item = dict()
            for k in self.item_fields:
                item[k] = ''

            item['partner_program_link'] = self.partner_program_link
            item['partner_directory'] = self.partner_directory
            item['partner_program_name'] = self.partner_program_name

            # item['partner_company_name'] = partner['built_by'] if 'built_by' in partner else ''
            item['partner_company_name'] = partner['name'] if 'name' in partner else ''
            item['company_domain_name'] = partner['website'] if ('website' in partner and partner['website']) else ''
            url_obj = urllib.parse.urlparse(item['company_domain_name'])
            item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
            x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
            if x:
                item['company_domain_name'] = x[-1]
            if '/' in item['company_domain_name']:
                item['company_domain_name'] = item['company_domain_name'][:item['company_domain_name'].find('/')]

            item['company_description'] = partner['description'] if 'description' in partner and partner['description'] else ''
            if item['company_description'] == '':
                desc_soup = BS(partner['html'] if 'html' in partner else '', "html.parser")
                for txt in desc_soup.find_all('h5'):
                    if txt.text == 'Integration Summary':
                        item['company_description'] = cleanhtml(txt.findNext('p').text)
            item['regions'] = partner['countries'] if 'countries' in partner else ''
            item['support_email_address'] = partner['email'] if 'email' in partner else ''
            item['categories'] = partner['category_names'] if 'category_names' in partner else ''

            yield item

        else:
            self.logger.info(f'ERROR PAGE, STATUS: {response.status}, RESPONSE: {response.text}')
            yield scrapy.Request(response.request.url, callback=self.parse_partner, dont_filter=True)
