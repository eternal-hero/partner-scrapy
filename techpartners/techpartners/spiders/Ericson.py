import json
import urllib.parse
import scrapy
from techpartners.spiders.base_spider import BaseSpider
from techpartners.functions import *


class Spider(BaseSpider):
    name = 'ericsonpartner'
    page_link = 'https://www.ericsson.com/en/partners/channel-partners/find-your-channel-partner'
    start_urls = ['https://www.ericsson.com/api/getChannelPartnerList?productArea=&location=']

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

    def parse(self, response):

        if response.status == 200:

            data = json.loads(response.text)

            if 'ChannelPartnerList' not in data:
                print(response.text)
                return

            for itm in data['ChannelPartnerList']:

                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = 'https://www.ericsson.com/en/partners/channel-partners/find-your-channel-partner'
                item['partner_directory'] = 'Ericsson channel partner'
                item['partner_program_name'] = ''

                item['partner_company_name'] = itm['Name']
                # item['company_domain_name'] = itm['Url']
                url_obj = urllib.parse.urlparse(itm['Url'] if 'Url' in itm and itm['Url'] else '')
                item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                if x:
                    item['company_domain_name'] = x[-1]

                item['headquarters_country'] = itm['Location'] if 'Location' in itm else ''
                item['categories'] = itm['Products'] if 'Products' in itm else ''

                yield item
