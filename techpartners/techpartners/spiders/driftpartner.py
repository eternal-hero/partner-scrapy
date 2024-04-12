import scrapy
from techpartners.spiders.base_spider import BaseSpider
import pandas as pd
from techpartners.functions import *
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
import urllib.parse


class Spider(BaseSpider):
    name = 'driftpartner'
    partner_program_link = 'https://www.drift.com/partners/partner-directory'
    partner_directory = 'Drift Featured Partner'
    partner_program_name = ''
    crawl_id = 1259

    allowed_domains = ['www.drift.com']
    start_urls = [partner_program_link]

    item_fields = ['partner_program_link', 'partner_directory', 'partner_program_name', 'partner_company_name', 'product/service_name',
                   'company_domain_name', 'partner_type', 'partner_tier', 'company_description', 'product/service_description',
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
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts', 'certified?',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    geolocator = Nominatim(user_agent="MM_Geocoder")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
    reverse = RateLimiter(geolocator.reverse, min_delay_seconds=1)

    def short_url(self, u):
        u = u.split('?')[0]
        if u[-1] == '/':
            u = u[:-1]
        else:
            u = u
        u = u.split('/')[2]
        return u

    def parse(self, response):
        data = []
        divs = response.xpath('//div[@class="modal modal-partner"]')
        for div in divs:

            addreass = div.xpath('.//div[@class="location margin-bottom__xs"]/text()').get()
            address = addreass.replace('Locations throughout the ', '').strip()
            address = address.replace(' serving ', ', ').strip()
            # address = address.replace('&', ' and ')
            # sep_words = [i.strip() for i in address.split(' and ')]
            sep_words = re.split(r'\sand\s|&|/|;|\(.*\)', address)
            for word in sep_words:
                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''
                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = self.partner_program_name

                item["partner_company_name"] = div.xpath(
                    './/div[@class="font-20 bold margin-bottom__xs margin-top__mobile"]/text()').get()
                # item["company_domain_name"] = self.short_url(div.xpath('.//a[@class="link partner-link"]/@href').get())
                url_obj = urllib.parse.urlparse(self.short_url(div.xpath('.//a[@class="link partner-link"]/@href').get()))
                item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                if x:
                    item['company_domain_name'] = x[-1]

                item["company_description"] = cleanhtml(
                    " ".join([t.strip() for t in div.xpath('.//div[@class="margin-bottom__xs"]//text()').extract()]))
                item["specializations"] = " ".join(
                    [s.strip() + '\n' for s in div.xpath('.//ul[@class="font-16"]/li/text()').extract()])

                try:
                    location = self.geocode(word, language='en')
                    if not location:
                        location = self.geocode(word[word.find(','):], language='en')
                    location = self.reverse([location.latitude, location.longitude], language='en')
                    item["locations_city"] = location.raw['address']['city'] if location and 'address' in location.raw and 'city' in location.raw['address'] and location.raw['address']['city'].lower() in word.lower() else ''
                    item["locations_state"] = location.raw['address']['state'] if location and 'address' in location.raw and 'state' in location.raw['address'] else ''
                    item["locations_country"] = location.raw['address']['country'] if location and 'address' in location.raw and 'country' in location.raw['address'] else ''
                    yield item

                except:
                    sep_words2 = re.split(r',', word)
                    for word in sep_words2:
                        try:
                            location = self.geocode(word, language='en')
                            location = self.reverse([location.latitude, location.longitude], language='en')
                            item["locations_city"] = location.raw['address'][
                                'city'] if location and 'address' in location.raw and 'city' in location.raw[
                                'address'] and location.raw['address']['city'].lower() in word.lower() else ''
                            item["locations_state"] = location.raw['address'][
                                'state'] if location and 'address' in location.raw and 'state' in location.raw[
                                'address'] else ''
                            item["locations_country"] = location.raw['address'][
                                'country'] if location and 'address' in location.raw and 'country' in location.raw[
                                'address'] else ''
                            yield item
                        except:
                            continue
