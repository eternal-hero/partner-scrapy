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


def area(x1, y1, x2, y2, x3, y3):
    return abs((x1 * (y2 - y3) + x2 * (y3 - y1)
                + x3 * (y1 - y2)) / 2.0)


def isInside(x1, y1, x2, y2, x3, y3, x, y):
    A = area(x1, y1, x2, y2, x3, y3)
    A1 = area(x, y, x2, y2, x3, y3)
    A2 = area(x1, y1, x, y, x3, y3)
    A3 = area(x1, y1, x2, y2, x, y)
    if A == A1 + A2 + A3:
        return True
    else:
        return False


T1 = -180, 90, -80, 0, 0, 90
T2 = -80, 20, -80, -60, -30, 0
T3 = -70, 90, 20, -50, 180, 80
T4 = 110, -30, 140, 0, 160, -40


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'avayapartner'
    partner_program_link = 'https://www.avaya.com/en/partner-locator/'
    partner_directory = 'Avaya Partner'
    partner_program_name = ''
    crawl_id = None

    custom_settings = {
        'CONCURRENT_REQUESTS': 8,
        'CONCURRENT_REQUESTS_PER_IP': 1,
        'DOWNLOADER_MIDDLEWARES': {
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            'rotating_proxies.middlewares.BanDetectionMiddleware': 620},
        'ROTATING_PROXY_LIST_PATH': 'proxy_25000.txt',
        'ROTATING_PROXY_PAGE_RETRY_TIMES': 5,
    }

    headers = {'AUTHORITY': 'www.avaya.com',
               'ACCEPT': 'application/json',
               'ACCEPT-ENCODING': 'gzip, deflate, br',
               'ACCEPT-LANGUAGE': 'en-US,en;q=0.9',
               'COOKIE': 'cleared-onetrust-cookies=; GeoIPCountry=United States; cta=23CLD-GL-BRANDSTREG; tac=23CLD-GL-BRANDSTREG; mmapi.p.uat={"generationPage":"others","KickFire NAICS":"Other"}; _gcl_au=1.1.858064319.1674737072; ruid=8vrTQ5zjCz; ln_or=eyI5NjEwNiI6ImQifQ==; _gid=GA1.2.409961356.1674737073; _fbp=fb.1.1674737072862.619463550; AKA_A2=A; _abck=3CD4EE2A6185BBC4953E23CA54695D9E~0~YAAQLKosF4gwzeyFAQAAWvr67glcvEqV37dwtaYY85Xr7palpVmonXnJmZ61SxGnIMw1vNPWenCT9Qy8xJNnZHgcN5fTTtz2kkzAe5BihRczqHZBdiTZzedrqbJH5NmhHazoqYaSa9tAPZUqO/37ZMYN6JEHS+EBiG6M38XEYoqIR71jg9C3NRy8VZujDYQosUDLeIoj64XXAxHDFKKqTDH2tZYXTCjaig1nJd0Mk3GoP5TooDLsKozXgk2wsz63aZULAyDLdR6W7DmQ4ijC5eloc4x24lxLJVVZQicoYH+3+LJL4SdEr0sN47YiZC/4wRNYzezg4QE1rUktZ96LNLIcH+0rhR8ZjU7FIJt+sNnOTDuczPTw94ctBg7ZwaA4X5PN3DhkpCHny6ksDKr7Susg2gdnDko=~-1~-1~-1; bm_sz=24D9103FDFCAD9F40AD381905549F067~YAAQLKosF4swzeyFAQAAWvr67hIoLYMDmlWgXNkt+PDB4YF9SFadJv3MAPNS8UQq2LKUtSSLN3/thk0gfEt68dFhuFGwuU1kgaoGPLJzPl3q015xygOcLiuR7GaVY+fxUUOJFCOPRxvHYYnWMkN1Hx1EZFzuYG+qhsD61Yu6IUopexsC2XWR1LK5XK9BDTJGs+zZJpTWbbRafaur5srHePPVlPr6hwlCf1/zpBbqtAu4uWC1tarEuVVHyQ/RoQyTMBujTXtBPA7tbqeCRSlaznE/8+88Z6dLTZq3l/PzonMKlw==~3616825~4600629; mmapi.p.bid="prodiadcgus03"; mmapi.p.srv="prodiadcgus03"; _gcl_aw=GCL.1674751705.EAIaIQobChMIzIWRnNjl_AIVxhXUAR0ZZAmXEAAYAiAAEgLAYfD_BwE; kickfire_api_session_cookie=1; _gat_UA-50549933-1=1; ak_bmsc=6F6BAEFF5F583C4160C56D0390F201A3~000000000000000000000000000000~YAAQLKosFx8xzeyFAQAAIgL77hIh/R2P5ziPb1W5XsCWhIKbuW7axsLKGUt0ExXXKiSjN0o/M2CYyL7h9GLwvGwy44CCGPWAGZKaSRqSUPY/zqbH3ZtdS8s6DBXrZtH81XFhR2hpt9crxe+lXotUbtUgbhzH/bKjcgg0ZXCtKPnB3aE6y+0dhn2Tt8HywkNjgSbtzmKafBp/ib7jVk9R+xLjqELWx+swncDn3UME4XC+G8EVsoVLrImZY0s1IysZ1d6PQOIZl4OJ4TMzDSC8oDg4hwuoN5KWeBPnUlzU9pjxEKs2gNQ6lyNQw+4Jy6sBwWc2VcpsgepNy8OVK5KOHNIP7fD0WtbHKpbmPAYdx0AK6aQThSvD2wPKrmN/b5KQiGeUkFlwMOgVyPPt6tlGwD5Is1xET3W5dCBLU+Agc5KTTqR1y8MSUyKf4RRvG5SWLLjIQpUSnCS2FIecZAPvkkcqLygwOSTYYi1rw+5uarJEPpo3Mx1M8g==; _gac_UA-50549933-1=1.1674751712.EAIaIQobChMIzIWRnNjl_AIVxhXUAR0ZZAmXEAAYAiAAEgLAYfD_BwE; mmapi.p.pd="ts5VUjELTUaqBbo2IU15I2y36-CiKJg9IGwmXhQwqQs=|EAAAAApDH4sIAAAAAAAEAGNhEO5RnfFW7HQSA3NmYgqjEAOjE8PUGi9PJgaT2cs19v6_5VHIclxwNpBmAAJGIP4PBAx85eXleun5-ek5qXrJ-bksb8WYQPJg4HuCiQGEQQqZGLy0GRkM3t36PuetGFiE8a3YWzGQBNgwBkYvoEoRaRYGRpAWkCFf2BkZRObPFHHG1FCU1uTAmJvOxGCzjBlqGaMrAG-bCtLDAAAA"; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Jan+26+2023+16:48:32+GMT+0000+(Coordinated+Universal+Time)&version=202211.2.0&isIABGlobal=false&landingPath=NotLandingPage&groups=C0001:1,C0002:1,C0003:1,C0004:1&hosts=H42:1,H116:1,H64:1,H75:1,H26:1,H140:1,H12:1,H14:1,H63:1,H128:1,H50:1,H55:1,H97:1,H135:1,H106:1,H43:1,H122:1,H81:1,H44:1,H108:1,H45:1,H84:1,H85:1,H19:1,H46:1,H47:1,H86:1,H129:1,H48:1,H87:1,H49:1,H130:1,H141:1,H123:1,H90:1,H51:1,H52:1,H53:1,H9:1,H54:1,H131:1,H34:1,H10:1,H11:1,H93:1,H132:1,H56:1,H124:1,H112:1,H57:1,H13:1,H58:1,H143:1,H125:1,H114:1,H59:1,H60:1,H99:1,H100:1,H61:1,H101:1,H62:1,H126:1,H145:1,H134:1,H117:1,H65:1,H66:1,H67:1,H16:1,H68:1,H69:1,H70:1,H71:1,H72:1,H73:1,H24:1,H102:1,H103:1,H74:1,H76:1,H146:1,H18:1,H120:1,H77:1,H78:1,H35:1,H136:1,H79:1&genVendors=&AwaitingReconsent=false; JSESSIONID=apfu-x6CybHpeLOU5F-LVQM9br5Tjw8m5ICJDbrwwwu5KUIWwzjj!-1620758522!-1709831201; _ga=GA1.2.1582420610.1674737073; RT="z=1&dm=www.avaya.com&si=66fb2d1b-574b-4b19-bca3-e29201c60687&ss=lddbx8rn&sl=3&tt=37p&bcn=//17de4c12.akstat.io/&ld=ezi"; _ga_4FRNP180SQ=GS1.1.1674751705.2.1.1674751724.41.0.0; bm_sv=E956FA0E160589FA3DB6A0F7F3F2EB54~YAAQLKosF5w1zeyFAQAAAVf77hKc1VjetIOP1FufaYXBsJDMw2eO/h8V3/hVMc0MvlFmV46CF9BbamoHWDF+/R3gB0jiKDwcKSSPWlOsrO32h7w1gz2n8f57kidEaqSLA2GjhGbiGqZye2poNGxfLc33KQrksPQw5peWfjDEZFgS9DFAdvw6MKs3P504FVMzDOu+PCwHvx7zjO5JA4ET6RFuOW/rz5s1iCmrO32+I9B4DpfHbEbOH7RJgYR5JBU=~1',
               'REFERER': 'https://www.avaya.com/en/partner-locator/',
               'SEC-CH-UA': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
               'SEC-CH-UA-MOBILE': '?0',
               'SEC-CH-UA-PLATFORM': '"Windows"',
               'SEC-FETCH-DEST': 'empty',
               'SEC-FETCH-MODE': 'cors',
               'SEC-FETCH-SITE': 'same-origin',
               'USER-AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'}

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

    def start_requests(self):
        for lng in range(-180, 180 + 2, 2):
            for lat in range(-90, 90 + 2, 2):
                p = lng, lat
                if isInside(*(T1 + p)) or isInside(*(T2 + p)) or isInside(*(T3 + p)) or isInside(*(T4 + p)):
                    link = f'https://www.avaya.com/cs/Satellite?pagename=Avaya2/Service/PartnerLocator&locale=en_US&lng={lng}&lat={lat}'
                    yield scrapy.Request(method='GET', url=link, headers=self.headers,
                                         callback=self.parse, meta={'lng': lng, 'lat': lat})

    def parse(self, response):
        lng = response.meta['lng']
        lat = response.meta['lat']
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            locations = json.loads(response.text.strip())['data']['locations']

            if len(locations) > 0:
                self.logger.info(f'Found {len(locations)} location @ lng: {lng}, lat: {lat}')

            for location in locations:
                _id = location.get('partnerId')

                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = ''

                item['partner_company_name'] = location.get('name')
                item['company_domain_name'] = get_domain_from_url(location.get('website', ''))
                item['partner_tier'] = location.get('levelDesc', '')

                location_link = 'https://www.avaya.com/cs/Satellite/?pagename=Avaya2/Service/PartnerDetail&locale=en_US&partnerId=' + _id

                yield scrapy.Request(location_link, callback=self.parse_location, meta={'item': item})

    def parse_location(self, response):
        item = response.meta['item']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, URL: {response.url}')
        else:
            location_data = json.loads(response.text.strip())['data']['partner']
            item['general_phone_number'] = location_data.get('phone', '')
            item['headquarters_street'] = location_data.get('address1', '')
            if location_data.get('address2', ''):
                item['headquarters_street'] + ', ' + location_data.get('address2', '')
            item['headquarters_city'] = location_data.get('city', '')
            item['headquarters_state'] = location_data.get('state', '')
            item['headquarters_zipcode'] = location_data.get('zip', '')
            item['headquarters_country'] = location_data.get('country', '')

        yield item
