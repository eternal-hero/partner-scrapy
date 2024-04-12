import codecs
import re
import urllib.parse

import requests
import scrapy
from techpartners.spiders.base_spider import BaseSpider


class Spider(BaseSpider):
    name = 'nutanixpartner'
    partner_program_link = "https://www.nutanix.com/partners/find-a-partner"
    partner_directory = 'Nutanix Partner'
    partner_program_name = ''
    crawl_id = 1280

    allowed_domains = ['rest.ziftmarcom.com']

    item_fields = ['partner_program_link', 'partner_directory', 'partner_program_name', 'partner_company_name',
                   'company_domain_name', 'partner_type', 'partner_tier', 'company_description', 'headquarters_street',
                   'headquarters_city', 'headquarters_state', 'headquarters_zipcode', 'headquarters_country',
                   'locations_street', 'locations_city', 'locations_state', 'locations_zipcode', 'locations_country',
                   'regions', 'languages', 'services', 'solutions', 'products', 'pricing', 'specializations',
                   'categories', 'year_founded', 'partnership_timespan', 'general_phone_number', 'general_email_address',
                   'use_case', 'integrations', 'contract_vehicles', 'company_characteristics', 'linkedin_link',
                   'twitter_link', 'facebook_link', 'primary_contact_name', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'certified?', 'industries', 'partner_clients', 'notes']

    def start_requests(self):
        headers = {
            'x-zift-partner-locator': '8a99837974b364830174b512f8b33046',
            'Cookie': 'JSESSIONID=95762473BC6BB7DBAF3FFDF2A23A9C3A'
        }
        try:
            link = "https://rest.ziftmarcom.com/locator/partners?size=24&includeFields=true&includeTiers=true&page="
            res = requests.get(link + '1', headers=headers)
            total_pages = res.json()['totalPages']
            start = 0
            while start <= int(total_pages):
                url = link + str(start)
                yield scrapy.Request(url, callback=self.parse, headers=headers, dont_filter=True)
                start += 1

        except Exception as e:
            print(e)

    def parse(self, response):
        partners = response.json()['content']

        print(response.request.url)
        print(len(partners))
        if partners:
            for partner in partners:

                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = self.partner_program_name

                item['partner_company_name'] = partner.get('name') if 'name' in partner else ''
                # item['company_domain_name'] = partner.get('website') if 'website' in partner else ''
                url_obj = urllib.parse.urlparse(partner.get('website') if 'website' in partner else '')
                item['company_domain_name'] = (url_obj.netloc if url_obj.netloc != "" else url_obj.path)
                if isinstance(item['company_domain_name'], (bytes, bytearray)):
                    item['company_domain_name'] = codecs.decode(item['company_domain_name'], 'UTF-8')
                x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                if x:
                    item['company_domain_name'] = x[-1]

                if '/' in item['company_domain_name']:
                    item['company_domain_name'] = item['company_domain_name'][:item['company_domain_name'].find('/')]

                item['partner_type'] = " ".join([" ".join(i['values']) for i in partner['fieldValues'] if i['displayName'] == "Elevate Reseller Level"])
                item['company_description'] = partner.get('profileDescription') if 'profileDescription' in partner else ''
                item['industries'] = partner.get('industries') if 'industries' in partner else ''
                for i in partner['socialLinks']:
                    if i['provider'] == "FACEBOOK":
                        item['facebook_link'] = i['url']
                    elif i['provider'] == "LINKEDIN":
                        item['linkedin_link'] = i['url']
                    elif i['provider'] == "TWITTER":
                        item['twitter_link'] = i['url']

                for address in partner['addresses']:
                    if address['primary']:
                        item['headquarters_street'] = address['address1'] if 'address1' in address else ''
                        item['headquarters_city'] = address['city'] if 'city' in address else ''
                        item['headquarters_state'] = address['state'] if 'state' in address else ''
                        item['headquarters_zipcode'] = address['postalCode'] if 'postalCode' in address else ''
                        item['headquarters_country'] = address['country'] if 'country' in address else ''

                for address in partner['addresses']:
                    item['general_phone_number'] = address['phone'] if 'phone' in address else ''
                    item['locations_street'] = address['address1'] if 'address1' in address else ''
                    item['locations_city'] = address['city'] if 'city' in address else ''
                    item['locations_state'] = address['state'] if 'state' in address else ''
                    item['locations_zipcode'] = address['postalCode'] if 'postalCode' in address else ''
                    item['locations_country'] = address['country'] if 'country' in address else ''

                    yield item
