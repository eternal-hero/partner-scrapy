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
    name = 'bubblepartner'
    partner_program_link = 'https://bubble.io/agencies'
    partner_directory = 'Bubble Partner Directory'
    partner_program_name = ''
    crawl_id = None

    start_urls = [partner_program_link]

    item_fields = ['partner_program_link', 'partner_directory', 'partner_program_name', 'partner_company_name',
                   'product_service_name',
                   'company_domain_name', 'partner_type', 'partner_tier', 'company_description',
                   'product_service_description',
                   'headquarters_address',
                   'headquarters_street', 'headquarters_city', 'headquarters_state', 'headquarters_zipcode',
                   'headquarters_country',
                   'locations_address',
                   'locations_street', 'locations_city', 'locations_state', 'locations_zipcode', 'locations_country',
                   'regions', 'languages', 'products', 'services', 'solutions',
                   'pricing_plan', 'pricing_model', 'pricing_plan_description',
                   'pricing', 'specializations', 'categories',
                   'features', 'account_requirements', 'product_package_name', 'year_founded', 'latest_update',
                   'publisher',
                   'partnership_timespan', 'partnership_founding_date', 'product_version', 'product_requirements',
                   'general_phone_number', 'general_email_address',
                   'support_phone_number', 'support_email_address', 'support_link', 'help_link', 'terms_and_conditions',
                   'license_agreement_link', 'privacy_policy_link',
                   'linkedin_link', 'twitter_link', 'facebook_link', 'youtube_link', 'instagram_link', 'xing_link',
                   'primary_contact_name', 'primary_contact_phone_number',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts',
                   'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
        'Authority': 'bubble.io',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Cache-Control': 'no-cache',
        'Content-Type': 'application/json',
        'Origin': 'https://bubble.io',
        'Referer': 'https://bubble.io/',
        'Sec-Ch-Ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'Sec-Ch-Ua-mobile': '?0',
        'Sec-Ch-Ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'X-Requested-With': 'XMLHttpRequest',
        'X-Bubble-Breaking-Revision': '5',
        'X-Bubble-Fiber-Id': '1653753309492x297494357497674750',
        'X-Bubble-Pl': '1653753307151x4753',
        'X-Bubble-R': 'https://bubble.io/agencies',
        'X-Bubble-Utm-Data': '{}',
        }

    def parse(self, response):
        if response.status == 200:
            data = '{"z":"2su6xuJVSuX0m87xU2DTK54TAqvaUymIAA1cPk6+uX+gddgLr/LN2t3NMP+Rq87Q7MJoFfJSr2r0+Xop2DEBrFA15NkzeyJfNqshpE9JJaDBiECWa+xpFgAYoXoRmzlPyipwyEHv5QqdtfY1X1MVPBdghvMe5QGnb+c4WY8eKpcR47g44JkPnkZTmlI7Q9WFI8gT9WcQ79TrXu1XryEmfHV6WN0cv8pNv+/9t29QW0BPCDSVPc/+0lHtlZxORgfNtpoLsfv3Z7azse4/htP4bSQ9b8RXWPM8rsdDRFmVoiu11CN69bq5ruTAGoLdeKeDpHzD9LCUHTD8MFhcnrRArAzfhG26wik8evuHNoTmkL5z1/r7vY7NMmV51Jr9Ha8pq5IDGvcNmTFDXWp89pPMVk0/hSVWdU9nJqbIAeTgOhcRIceil5p2psPETF5GVo+kV+2TMpkkuduFvJceSrhEkDhVjPXka1ZyU5+7Sj0WXME0VYVFQ6g5NSBFgPbv/5FKlbgZK6xwgesbDreQHd88rHruPqApMrYsK0SdjtFPOM86vDRREcWnZixaotgb2Qf1/GMoflJrYz4F9EAlEF5Z0mieeoyvVShsGFBzH79o9iRNCPijRXCqbQEXzE9KZbfgm5+DHSLx0vTz/DhdfVO+rrfCWqwEYJd0EJTCmyQOI+wEsxQO8GIMbztErNcsEfALGLJv3k9xZmrI3NL1AGNKpMWZKYPoye37fTU0o1rQIKG8HTNyhQmsdxo5hl3wyKv37XftoBWtSLzCaPs/gtjrvtIKxOijUeMjK878nn3xlgqee52OUq9iQ60W2KPj9m4GDNd7PYM1VcPH2tueKoiYqB/vYwAsgfs+F0TgEj28mbG6DKQ7EtPp/PviRQgFyxMrZFC8igmLuU7ZsBrPsx+VwHBJ17xpOJGBticRqtE9HDpnkHYE9V6F+7tnECfNg3IoQABMJpdusVG3cQh9VNth4YcTeqnygwErM9cWV43wOG8x8r2KTJtyElA3JlnJQQnO+ORmGLi0qASYWLPp/owuKQGQt4q0L+Hn5/OzQS3wJ/pGnW33VP9NF0WPwm9/imwnFHFwBe+FJeSmzunNB/YQmMd4ToEPL+rXB/+zlgJx4l4z6cCGq/0lmv8I8MceyO5P","y":"qAOkwsQmFZs7Y1nA5a1uDA==","x":"0p0NqDDZWjxpI9LGzp2X6BCp7W/WkrYvKiEzmcApYaM="}'
            yield scrapy.Request(method='POST', url='https://bubble.io/elasticsearch/search',
                                 body=data, callback=self.parse_category,
                                 headers=self.headers)

    def parse_category(self, response):
        if response.status != 200:
            print(response.status)
            print(response.text)
            return

        # try:
        if True:
            data = json.loads(response.text)
            if 'hits' in data and 'total' in data['hits'] and 'hits' in data['hits'] and len(data['hits']['hits']) > 0:
                for record in data['hits']['hits']:
                    if '_source' not in record:
                        continue
                    else:
                        partner = record['_source']

                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = self.partner_program_link
                    item['partner_directory'] = self.partner_directory
                    item['partner_program_name'] = self.partner_program_name
                    item['partner_company_name'] = partner['name_text']
                    # item['partner_type'] = partner['type_text']

                    item['company_domain_name'] = partner['website_text'] if ('website_text' in partner and partner['website_text']) else ''
                    try:
                        url_obj = urllib.parse.urlparse(item['company_domain_name'])
                        item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                        x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                        if x:
                            item['company_domain_name'] = x[-1]
                    except Exception as e:
                        print('DOMAIN ERROR: ', e)

                    if 'location_geographic_address' in partner:
                        if 'components' in partner['location_geographic_address'] and 'country' in partner['location_geographic_address']['components']:
                            item['headquarters_city'] = partner['location_geographic_address']['components']['city'] if 'city' in partner['location_geographic_address']['components'] else ''
                            item['headquarters_state'] = partner['location_geographic_address']['components']['state'] if 'state' in partner['location_geographic_address']['components'] else ''
                            item['headquarters_country'] = partner['location_geographic_address']['components']['country'] if 'country' in partner['location_geographic_address']['components'] else ''
                        else:
                            item['headquarters_country'] = partner['location_geographic_address']['address'] if 'address' in partner['location_geographic_address'] else ''

                    yield item

        # except Exception as e:
        #     print(e)
