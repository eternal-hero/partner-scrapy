# import needed libraries
import re
import math
import json
import requests
import scrapy
import urllib.parse
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup as BS
from techpartners.spiders.base_spider import BaseSpider
from techpartners.functions import *


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'oraclepartner'
    partner_program_link = 'https://partner-finder.oracle.com/catalog/'
    partner_directory = 'Oracle Partner'
    partner_program_name = ''
    crawl_id = 1263

    # custom_settings = {
    #     'DOWNLOAD_DELAY': 0.5,
    # }

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

    start_urls = [partner_program_link]

    def parse(self, response):
        """
        parse partner page and get result json data
        :param response:
        :return:
        """
        # site filters
        filters = [
            {"xscLHSell":["Resale"]},
            {"xscSellExpertise": ["All"]},
            {"xscLHBuild": ["Build"]},
            {"xscLHService":["Service"]},
            {"xscBuildPoweredBy,xscBuildIntegratedWith": ["All"]},
            {"xscServiceCX,xscServiceEPM,xscServiceERP,xscServiceHCM,xscServiceSCM,xscServiceOCP,xscServiceOCI":["All"]}
        ]

        locations = list()

        # Add location based search to filters
        locs = list()
        loc_response = requests.get('https://partner-finder.oracle.com/catalog/api/file/OPN-EXPERTISELIST-DF')
        if loc_response.status_code != 200:
            pass
        else:
            loc_data = loc_response.json()
            if 'locationList' in loc_data:
                lvl3 = loc_data['locationList'][0]['level_2_list'][0]['level_3_list']
                for region in lvl3:
                    locations.append({"xscCompanyLocation": [region['level_id']]})
                    # countries = region['level_4_list']
                    # for country in countries:
                    #     locations.append({"xscCompanyLocation": [country['level_id']]})

        key_response = requests.get('https://partner-finder.oracle.com/catalog/api/file/OPN-COMPANYLIST-DF')
        keywords = list()
        if key_response.status_code != 200:
            pass
        else:
            key_data = key_response.json()
            if 'partners' in key_data:
                keywords = key_data['partners']

        self.sess = requests.Session()
        self.sess.get(self.partner_program_link)

        for filter in filters:
            for location in locations:
                fltr = {**location, **filter}
                pageNumber = 1
                last_page = 1
                while pageNumber <= last_page:
                    payload = {"keyword": "",
                               "pageNumber": f"{str(pageNumber)}",
                               "resultCount": "15",
                               "searchBy": "company",
                               "xscProfileType": "Partner Profile",
                               "searchExpertiseIds": {"ISE": "251,354,1225,247,1226,250,246,930,355,356,1351,1349,1348,1350,357,358,359,360,361,362,363,1146,891,890,1285,810,1325,1265,1145,671,990,1147,1205,1227,1372,1326,832,1306,1352,373,374,1165,375,379,376,1245,1037,382,378,380,377,381,383,384,389,385,386,387,388,366,367,369,370,368,371,372",
                                                      "CSPE": "1033,1032,1034,1035,1031,1125,1036,1051,1052,1053,1054,1055,1056,1057,1058,1059,1075,1076,1077,1081,1078,1068,1069,1074,1070,1071,1072,1073,1084,1080,1082,1079,1083,1061,1062,1064,1065,1063,1066,1067"},
                               "filters": fltr
                               }

                    response = self.sess.post('https://partner-finder.oracle.com/catalog/opf/partnerList',
                                              json=payload)

                    if response.status_code != 200:
                        continue
                    else:
                        data = response.json()
                        if 'profiles' not in data:
                            continue
                        profiles = data['profiles']

                        if last_page == 1 and len(profiles) != 0:
                            last_page = int(math.ceil(data['count'] / 15))

                        self.logger.info((str(fltr) + " Page: " + str(pageNumber) + ' of ' + str(
                            last_page) + " return profiles: " + str(len(profiles))))

                        for profile in profiles:

                            prof_id = profile['id']

                            if False:
                                pass
                            else:
                                profileId = profile['companyid']
                                prod_link = 'https://partner-finder.oracle.com/catalog/Partner/' + prof_id

                                if "xscBuildPoweredBy,xscBuildIntegratedWith" in filter:
                                    partner_type = 'Cloud Solution Builders & ISVs'
                                elif "xscServiceCX,xscServiceEPM,xscServiceERP,xscServiceHCM,xscServiceSCM,xscServiceOCP,xscServiceOCI" in fltr:
                                    partner_type = 'Cloud Services Partners'
                                elif "xscSellExpertise" in filter:
                                    partner_type = 'Cloud Resellers'
                                elif filter == {"xscLHBuild": ["Build"]}:
                                    partner_type = 'License & Hardware Partners - Build Expertise'
                                elif filter == {"xscLHSell": ["Resale"]}:
                                    partner_type = 'License & Hardware Partners - Sell Expertise'
                                elif filter == {"xscLHService": ["Service"]}:
                                    partner_type = 'License & Hardware Partners - Service Expertise'
                                else:
                                    partner_type = ''

                                yield scrapy.Request(prod_link, callback=self.parse_partner, dont_filter=True,
                                                     meta={'profileId': profileId,
                                                           'prod_link': prod_link,
                                                           'prof_id': prof_id,
                                                           'partner_type': partner_type,
                                                           'trial': 'First'
                                                           }
                                                     )
                        # follow next page
                        if 0 < len(profiles) <= 15 and pageNumber < last_page:
                            pageNumber += 1
                        # elif len(profiles) == 0 and pageNumber < last_page:
                        #     continue
                        else:
                            break

        if len(keywords) > 0:
            for keyword in keywords:
                fltr = {}
                pageNumber = 1
                last_page = 1
                while pageNumber <= last_page:
                    payload = {"keyword": keyword,
                               "pageNumber": f"{str(pageNumber)}",
                               "resultCount": "15",
                               "searchBy": "company",
                               "xscProfileType": "Partner Profile",
                               "searchExpertiseIds": {"ISE": "251,354,1225,247,1226,250,246,930,355,356,1351,1349,1348,1350,357,358,359,360,361,362,363,1146,891,890,1285,810,1325,1265,1145,671,990,1147,1205,1227,1372,1326,832,1306,1352,373,374,1165,375,379,376,1245,1037,382,378,380,377,381,383,384,389,385,386,387,388,366,367,369,370,368,371,372",
                                                      "CSPE": "1033,1032,1034,1035,1031,1125,1036,1051,1052,1053,1054,1055,1056,1057,1058,1059,1075,1076,1077,1081,1078,1068,1069,1074,1070,1071,1072,1073,1084,1080,1082,1079,1083,1061,1062,1064,1065,1063,1066,1067"},
                               "filters": fltr
                               }

                    response = self.sess.post('https://partner-finder.oracle.com/catalog/opf/partnerList',
                                              json=payload)

                    if response.status_code != 200:
                        continue
                    else:
                        data = response.json()
                        if 'profiles' not in data:
                            continue
                        profiles = data['profiles']

                        if last_page == 1 and len(profiles) != 0:
                            last_page = int(math.ceil(data['count'] / 15))

                        self.logger.info((str(fltr) + " Page: " + str(pageNumber) + ' of ' + str(last_page) + " return profiles: " + str(len(profiles))))

                        for profile in profiles:

                            prof_id = profile['id']

                            if False:
                                pass
                            else:
                                profileId = profile['companyid']
                                prod_link = 'https://partner-finder.oracle.com/catalog/Partner/' + prof_id

                                yield scrapy.Request(prod_link, callback=self.parse_partner, dont_filter=True,
                                                     meta={'profileId': profileId,
                                                           'prod_link': prod_link,
                                                           'prof_id': prof_id,
                                                           'partner_type': '',
                                                           'trial': 'First'
                                                           }
                                                     )
                        # follow next page
                        if 0 < len(profiles) <= 15 and pageNumber < last_page:
                            pageNumber += 1
                        # elif len(profiles) == 0 and pageNumber < last_page:
                        #     continue
                        else:
                            break

    def parse_partner(self, response):
        """
        parse partner page and get result json data
        :param response:
        :return:
        """

        # Initialize item
        item = dict()
        for k in self.item_fields:
            item[k] = ''

        item['partner_program_link'] = self.partner_program_link
        item['partner_directory'] = self.partner_directory
        item['partner_program_name'] = self.partner_program_name

        item['partner_type'] = response.meta['partner_type']

        # profileId = response.meta['profileId']
        # prod_link = response.meta['prod_link']
        prof_id = response.meta['prof_id']

        soup = BS(response.text, "html.parser")

        # item['general_phone_number'] = soup.find('span', {'id': 'partnerPhoneNumber'}).get_text() if soup.find('span', {'id': 'partnerPhoneNumber'}) else ''

        script_divs = soup.find_all("script")
        for div in script_divs:
            txt = div.text
            if 'const' in txt and 'profileData' in txt:
                json_data = re.search('const\s*profileData\s*=.+;', txt)[0]
                json_data = json_data[json_data.find('='):]
                json_data = json_data.lstrip('=').strip()
                json_data = json_data.rstrip(';')
                try:
                    profileData = json.loads(json_data)
                except:
                    if response.meta['trial'] == 'First':
                        response.meta['trial'] = 'Last'
                        yield scrapy.Request(response.request.url, callback=self.parse_partner, dont_filter=True,
                                             meta=response.meta)
                    return

                item['partner_company_name'] = profileData['xscCompanyName']
                item['locations_street'] = profileData['xscAddressLine1']
                item['locations_state'] = profileData['xscStateProvience']
                item['locations_city'] = profileData['xscCity']
                item['locations_zipcode'] = profileData['xscZipCode']
                review_link = 'https://partner-finder.oracle.com/catalog/api/file/' + prof_id
                review_response = requests.get(review_link)
                if review_response.status_code != 200:
                    if response.meta['trial'] == 'First':
                        response.meta['trial'] = 'Last'
                        yield scrapy.Request(response.request.url, callback=self.parse_partner, dont_filter=True,
                                             meta=response.meta)
                    return

                if 'name="PartnerSummary"' in review_response.text:
                    dataXml = ET.fromstring(review_response.content)

                    chk = dataXml.findall(".//*[@name='Content']")
                    if len(chk) > 0:
                        for i in chk:
                            desc_html = i.text
                            if desc_html and desc_html != '':
                                desc_soup = BS(desc_html, "html.parser")
                                description = desc_soup.text
                                item['company_description'] = cleanhtml(description.strip())
                                break

                    chk = dataXml.findall(".//*[@name='PartnerPhoneNumber']")
                    if len(chk) > 0:
                        for i in chk:
                            phone = i.text
                            if phone and phone != '':
                                item['general_phone_number'] = phone
                                break

                    chk = dataXml.findall(".//*[@name='CompanyURL']")
                    if len(chk) > 0:
                        for i in chk:
                            link = i.text
                            if link:
                                link = link.strip()
                                # item['company_domain_name'] = link

                                url_obj = urllib.parse.urlparse(link)
                                item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                                x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                                if x:
                                    item['company_domain_name'] = x[-1]

                            if link and link != '':
                                break

                    chk = dataXml.findall(".//*[@name='HQLocation']")
                    if len(chk) > 0:
                        for i in chk:
                            hq_Location = i.text
                            if hq_Location:
                                hq_Location = hq_Location.strip()
                                item['headquarters_country'] = hq_Location
                            if hq_Location and hq_Location != '':
                                break

                    chk = dataXml.findall(".//*[@name='AddressCountry']")
                    if len(chk) > 0:
                        for i in chk:
                            country = i.text
                            if country:
                                country = country.strip()
                                item['locations_country'] = country
                            if country and country != '':
                                break

                if (item['locations_country'] is None or item['locations_country'] == '') and item['headquarters_country'] != '':
                    item['locations_country'] = item['headquarters_country']

                item['specializations'] = profileData['xscSpecializationKeywords']

                # break script divs loop
                break

        divs = soup.find_all("div")

        for div in divs:
            if div.get('id') == 'profile-solutions':
                solutions = dict()

                sections = div.find_all("div", {"class": "o-2-columns"})
                for section in sections:
                    label = section.find("div", {"class": "o-col-1"}).find("h4").text

                    solutions[label] = list()
                    rows = section.find("div", {"class": "o-col-2"}).find_all("li")
                    for row in rows:
                        srvs_name, srvs_summary, srvs_link = '', '', ''
                        srvs_link = row.find("a")['href']
                        if 'catalog/solution' in srvs_link.lower():
                            solution_id = srvs_link[srvs_link.rfind('/')+1:]
                            srvs_link = 'https://partner-finder.oracle.com/catalog/Solution/' + solution_id
                            solution_response = requests.get('https://partner-finder.oracle.com/catalog/api/file/' + solution_id)
                            if '<wcm:list name="Solution">' in solution_response.text:
                                responseXml = ET.fromstring(solution_response.content)
                                if responseXml.find(".//*[@name='SolutionName']"):
                                    srvs_name = responseXml.find(".//*[@name='SolutionName']").text
                                if srvs_name == '':
                                    srvs_name = row.find("a").text
                                if srvs_name != '':
                                    srvs_name = cleanhtml(srvs_name)
                                if responseXml.find(".//*[@name='SolutionSummary']"):
                                    srvs_summary = responseXml.find(".//*[@name='SolutionSummary']").text
                                if srvs_summary != '':
                                    srvs_summary = cleanhtml(srvs_summary)
                        else:
                            srvs_name = row.find("a").text
                            row.a.decompose()
                            srvs_summary = row.find("p", recursive=False).text.strip()

                        solutions[label].append({'title': srvs_name,
                                                 'link': srvs_link,
                                                 'summary': srvs_summary})
                item['solutions'] = solutions

        # create result item
        yield item
