# import needed libraries
import json
from techpartners.spiders.base_spider import BaseSpider
import scrapy
from techpartners.functions import *
import urllib.parse
from bs4 import BeautifulSoup as BS


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'polypartner'
    site_name = 'Poly Partner Directory'
    page_link = 'https://partners.poly.com/English/directory/'
    start_urls = ['https://partners.poly.com/English/directory/search?f0=Solution+Expertise&f0v0=Headset',
                  'https://partners.poly.com/English/directory/search?f0=Solution+Expertise&f0v0=Headset+and+Video',
                  'https://partners.poly.com/English/directory/search?f0=Solution+Expertise&f0v0=Video',
                  'https://partners.poly.com/English/directory/search?f0=Solution+Expertise&f0v0=Headset+and+Voice+and+Video',
                  'https://partners.poly.com/English/directory/'
                  ]

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
                   'primary_contact_name', 'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts', 'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    def parse(self, response):
        if response.status == 200:

            url = response.request.url
            if '=' in url:
                solution = url[url.rfind('=')+1:].replace('+', ' ')
            else:
                solution = ''

            partners = list()
            soup = BS(response.text, "html.parser")
            scripts = soup.find_all('script')
            for script in scripts:
                if 'plSearch.allResults' in script.text:
                    content = script.text[script.text.find('plSearch.allResults'):]
                    content = content[content.find('=') + 1:]
                    content = content[: content.find('plSearch')]
                    content = content[: content.rfind(';')]
                    partners = json.loads(content.strip())
                    break

            for partner in partners:
                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = 'https://partners.poly.com/English/directory/'
                item['partner_directory'] = 'Poly Partner'
                item['partner_company_name'] = partner['Name']
                item['partner_tier'] = partner['ProgramLevel'] if 'ProgramLevel' in partner else ''
                # item['company_domain_name'] = partner['Website'] if 'Website' in partner else ''
                url_obj = urllib.parse.urlparse(partner['Website'] if 'Website' in partner else '')
                item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                if x:
                    item['company_domain_name'] = x[-1]

                # item['locations_street'] = partner['MailingStreet'] if 'MailingStreet' in partner else ''
                # item['locations_country'] = partner['MailingCountry'] if 'MailingCountry' in partner else ''
                item['general_phone_number'] = partner['Phone'] if 'Phone' in partner else ''

                item['solutions'] = solution

                if 'ViewPartnerUrl' in partner:
                    partner_url = 'https://partners.poly.com/' + partner['ViewPartnerUrl']
                    yield scrapy.Request(method='GET', url=partner_url, callback=self.parse_partner,
                                         meta={'item': item})

    def parse_partner(self, response):
        if response.status == 200:
            item = response.meta['item']

            soup = BS(response.text, "html.parser")
            desc = soup.find('p', {'id': "Locator_BodyContent_MarketplaceLongDescription"})
            if desc:
                item['company_description'] = desc.text

            inds = soup.find('p', {'id': "Locator_BodyContent_IndustryExpertise"})
            if inds:
                item['industries'] = inds.find('span').text

            certifications_div = soup.find('div', {'id': "Locator_BodyContent_Images"})
            if certifications_div:
                item['specializations'] = list()
                imgs = certifications_div.find_all('img')
                for img in imgs:
                    if 'solution_certification' in img['src']:
                        if 'headset' not in img['src'] and 'video' not in img['src']:
                            txt = img['src']
                            txt = txt[txt.rfind('/')+1:txt.rfind('.png')].replace('_', ' ').capitalize()
                            item['specializations'].append(txt)

            locs = soup.find('p', {'id': "Locator_BodyContent_CountriesServed"})
            if locs:
                item['locations_country'] = locs.find('span').text

            street = soup.find('span', {'itemprop': "streetAddress"})
            if street:
                item['headquarters_street'] = street.text

            city = soup.find('span', {'itemprop': "addressLocality"})
            if city:
                item['headquarters_city'] = city.text

            state = soup.find('span', {'itemprop': "addressRegion"})
            if state:
                item['headquarters_state'] = state.text

            postal = soup.find('span', {'itemprop': "postalCode"})
            if postal:
                item['headquarters_zipcode'] = postal.text

            country = soup.find('span', {'itemprop': "addressCountry"})
            if country:
                item['headquarters_country'] = country.text

            yield item
        else:
            yield scrapy.Request(method='GET', url=response.request.url, callback=self.parse_partner,
                                 meta=response.meta, dont_filter=True)
