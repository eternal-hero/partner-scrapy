# import needed libraries
import json
import re
from techpartners.spiders.base_spider import BaseSpider
import scrapy
from techpartners.functions import *
import urllib.parse
from scrapy.http import HtmlResponse

class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'zoompartner'
    site_name = 'Zoom Partner Directory'
    page_link = 'https://partner.zoom.us/partner-locator/'
    start_urls = ['https://partner.zoom.us/wp-content/themes/zoompartner/partner-locator-js.php?foo=bar&url=https://partner.zoom.us/partner-locator/']

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
            partners = list()
            if 'partnerList' in response.text:
                content = response.text[response.text.find('partnerList'):]
                content = content[content.find('=') + 1:]
                content = content[: content.find('partnerList')].strip()
                content = cleanhtml(content)

                partners = content.split('PartnerType:')
                for partner in partners:
                    if 'PartnerName:' in partner:
                        # Initialize item
                        item = dict()
                        for k in self.item_fields:
                            item[k] = ''

                        item['partner_program_link'] = 'https://partner.zoom.us/partner-locator/'
                        item['partner_directory'] = 'Zoom Partner'
                        item['partner_program_name'] = ''

                        item['partner_type'] = partner[:partner.find('PartnerName:')].strip().rstrip(',').strip()

                        tmp = partner[partner.find('PartnerName:'): partner.find('PartnerURL:')].strip(',').strip()
                        item['partner_company_name'] = tmp[tmp.find(':')+1:].strip().rstrip(',').strip("'").strip()

                        tmp = partner[partner.find('PartnerURL:'): partner.find('PartnerLogo:')].strip(',').strip()
                        # item['company_domain_name'] = tmp[tmp.find(':')+1:].strip().rstrip(',').strip("'").strip()
                        url_obj = urllib.parse.urlparse(tmp[tmp.find(':')+1:].strip().rstrip(',').strip("'").strip())
                        item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                        x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                        if x:
                            item['company_domain_name'] = x[-1]

                        tmp = partner[partner.find('Region:'): partner.find('Country:')].strip(',').strip()
                        item['regions'] = tmp[tmp.find(':')+1:].strip().rstrip(',').strip("'").strip().upper()

                        tmp = partner[partner.find('Country:'): partner.find('State:')].strip(',').strip()
                        item['headquarters_country'] = tmp[tmp.find(':')+1:].strip().rstrip(',').strip("'").strip().capitalize()

                        tmp = partner[partner.find('State:'): partner.find('Desc:')].strip(',').strip()
                        item['headquarters_state'] = tmp[tmp.find(':')+1:].strip().rstrip(',').strip("'").strip()

                        yield item
