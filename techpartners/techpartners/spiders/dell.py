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
    name = 'dellpartner'
    site_name = 'Dell Partner Directory'
    page_link = 'https://www.delltechnologies.com/partner/en-us/partner/find-a-partner.htm'
    api_link = 'https://dellcommunities.force.com/FindAPartner/s/sfsites/aura?r=12&aura.ApexAction.execute=1'

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

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36',
               'Accept': '*/*',
               'Accept-Language': 'en-US,en;q=0.9',
               'Accept-Encoding': 'gzip, deflate, br',
               'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
               'Referer': 'https://dellcommunities.force.com/FindAPartner/s/partnersearch?language=en_us&country=us&partnerType=findareseller',
               'Origin': 'https://dellcommunities.force.com/',
               'Host': 'dellcommunities.force.com',
               'Connection': 'keep-alive',
               'sec-ch-ua': '"Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
               'sec-ch-ua-platform': 'Windows',
               'Sec-Fetch-Dest': 'empty',
               'Sec-Fetch-Mode': 'cors',
               'Sec-Fetch-Site': 'same-origin',
               'X-SFDC-Page-Scope-Id': '3aef3e89-9e29-418e-8bf9-039f2d0804c8',
               'X-SFDC-Request-Id': '3415405000004cb651',
               }

    def start_requests(self):

        partner_types = ['Cloud Service Provider', 'Distributor', 'Federal Partner',
                         'OEM Partner', 'Solution Provider', 'Systems Integrator']

        countries = ['AFGHANISTAN (AF)', 'ALBANIA (AL)', 'ALGERIA (DZ)', 'AMERICAN SAMOA (AS)', 'ANDORRA (AD)',
                     'ANGOLA (AO)',
                     'ANGUILLA (AI)', 'ANTARCTICA (AQ)', 'ANTIGUA AND BARBUDA (AG)', 'ARGENTINA (AR)', 'ARMENIA (AM)',
                     'ARUBA (AW)',
                     'AUSTRALIA (AU)', 'AUSTRIA (AT)', 'AZERBAIJAN (AZ)', 'BAHAMAS (BS)', 'BAHRAIN (BH)',
                     'BANGLADESH (BD)',
                     'BARBADOS (BB)', 'BELARUS (BY)', 'BELGIUM (BE)', 'BELIZE (BZ)', 'BENIN (BJ)', 'BERMUDA (BM)',
                     'BHUTAN (BT)',
                     'BOLIVIA (BO)', 'BOSNIA AND HERZEGOVINA (BA)', 'BOTSWANA (BW)', 'BOUVET ISLAND (BV)',
                     'BRAZIL (BR)',
                     'BRITISH INDIAN OCEAN TERRITORY (IO)', 'BRUNEI DARUSSALAM (BN)', 'BULGARIA (BG)',
                     'BURKINA FASO (BF)',
                     'BURUNDI (BI)', 'CAMBODIA (KH)', 'CAMEROON (CM)', 'CANADA (CA)', 'CAPE VERDE (CV)',
                     'CAYMAN ISLANDS (KY)',
                     'CENTRAL AFRICAN REPUBLIC (CF)', 'CHAD (TD)', 'CHILE (CL)', 'CHINA (CN)', 'CHRISTMAS ISLAND (CX)',
                     'COCOS (KEELING) ISLANDS (CC)', 'COLOMBIA (CO)', 'COMOROS (KM)', 'CONGO (CG)',
                     'CONGO, THE DEMOCRATIC REPUBLIC OF THE (CD)', 'COOK ISLANDS (CK)', 'COSTA RICA (CR)',
                     'CROATIA (HR)',
                     'CUBA (CU)', 'CYPRUS (CY)', 'CZECH REPUBLIC (CZ)', "CÔTE D'IVOIRE (CI)", 'DENMARK (DK)',
                     'DJIBOUTI (DJ)',
                     'DOMINICA (DM)', 'DOMINICAN REPUBLIC (DO)', 'ECUADOR (EC)', 'EGYPT (EG)', 'EL SALVADOR (SV)',
                     'EQUATORIAL GUINEA (GQ)', 'ERITREA (ER)', 'ESTONIA (EE)', 'ETHIOPIA (ET)',
                     'FALKLAND ISLANDS (MALVINAS) (FK)',
                     'FAROE ISLANDS (FO)', 'FIJI (FJ)', 'FINLAND (FI)', 'FRANCE (FR)', 'FRENCH GUIANA (GF)',
                     'FRENCH POLYNESIA (PF)', 'FRENCH SOUTHERN TERRITORIES (TF)', 'GABON (GA)', 'GAMBIA (GM)',
                     'GEORGIA (GE)',
                     'GERMANY (DE)', 'GHANA (GH)', 'GIBRALTAR (GI)', 'GREECE (GR)', 'GREENLAND (GL)', 'GRENADA (GD)',
                     'GUADELOUPE (GP)', 'GUAM (GU)', 'GUATEMALA (GT)', 'GUERNSEY (GG)', 'GUINEA (GN)',
                     'GUINEA-BISSAU (GW)',
                     'GUYANA (GY)', 'HAITI (HT)', 'HEARD ISLAND AND MCDONALD ISLANDS (HM)',
                     'HOLY SEE (VATICAN CITY STATE) (VA)',
                     'HONDURAS (HN)', 'HONG KONG (HK)', 'HUNGARY (HU)', 'ICELAND (IS)', 'INDIA (IN)', 'INDONESIA (ID)',
                     'IRAQ (IQ)',
                     'IRELAND (IE)', 'ISLE OF MAN (IM)', 'ISRAEL (IL)', 'ITALY (IT)', 'JAMAICA (JM)', 'JAPAN (JP)',
                     'JERSEY (JE)',
                     'JORDAN (JO)', 'KAZAKHSTAN (KZ)', 'KENYA (KE)', 'KIRIBATI (KI)', 'KOREA, REPUBLIC OF (KR)',
                     'KOSOVO (KV)',
                     'KUWAIT (KW)', 'KYRGYZSTAN (KG)', "LAO PEOPLE'S DEMOCRATIC REPUBLIC (LA)", 'LATVIA (LV)',
                     'LEBANON (LB)',
                     'LESOTHO (LS)', 'LIBERIA (LR)', 'LIBYAN ARAB JAMAHIRIYA (LY)', 'LIECHTENSTEIN (LI)',
                     'LITHUANIA (LT)',
                     'LUXEMBOURG (LU)', 'MACAO (MO)', 'MACEDONIA, THE FORMER YUGOSLAV REPUBLIC OF (MK)',
                     'MADAGASCAR (MG)',
                     'MALAWI (MW)', 'MALAYSIA (MY)', 'MALDIVES (MV)', 'MALI (ML)', 'MALTA (MT)',
                     'MARSHALL ISLANDS (MH)',
                     'MARTINIQUE (MQ)', 'MAURITANIA (MR)', 'MAURITIUS (MU)', 'MAYOTTE (YT)', 'MEXICO (MX)',
                     'MICRONESIA, FEDERATED STATES OF (FM)', 'MOLDOVA, REPUBLIC OF (MD)', 'MONACO (MC)',
                     'MONGOLIA (MN)',
                     'MONTENEGRO (ME)', 'MONTSERRAT (MS)', 'MOROCCO (MA)', 'MOZAMBIQUE (MZ)', 'MYANMAR (MM)',
                     'NAMIBIA (NA)',
                     'NAURU (NR)', 'NEPAL (NP)', 'NETHERLANDS (NL)', 'NETHERLANDS ANTILLES (AN)', 'NEW CALEDONIA (NC)',
                     'NEW ZEALAND (NZ)', 'NICARAGUA (NI)', 'NIGER (NE)', 'NIGERIA (NG)', 'NIUE (NU)',
                     'NORFOLK ISLAND (NF)',
                     'NORTHERN MARIANA ISLANDS (MP)', 'NORWAY (NO)', 'OMAN (OM)', 'PAKISTAN (PK)', 'PALAU (PW)',
                     'PALESTINIAN TERRITORY, OCCUPIED (PS)', 'PANAMA (PA)', 'PAPUA NEW GUINEA (PG)', 'PARAGUAY (PY)',
                     'PERU (PE)',
                     'PHILIPPINES (PH)', 'PITCAIRN (PN)', 'POLAND (PL)', 'PORTUGAL (PT)', 'PUERTO RICO (PR)',
                     'QATAR (QA)',
                     'REUNION (RE)', 'ROMANIA (RO)', 'RUSSIAN FEDERATION (RU)', 'RWANDA (RW)', 'SAINT BARTHÉLEMY (BL)',
                     'SAINT HELENA (SH)', 'SAINT KITTS AND NEVIS (KN)', 'SAINT LUCIA (LC)', 'SAINT MARTIN (MF)',
                     'SAINT PIERRE AND MIQUELON (PM)', 'SAINT VINCENT AND THE GRENADINES (VC)', 'SAMOA (WS)',
                     'SAN MARINO (SM)',
                     'SAO TOME AND PRINCIPE (ST)', 'SAUDI ARABIA (SA)', 'SENEGAL (SN)', 'SERBIA (RS)',
                     'SEYCHELLES (SC)',
                     'SIERRA LEONE (SL)', 'SINGAPORE (SG)', 'SLOVAKIA (SK)', 'SLOVENIA (SI)', 'SOLOMON ISLANDS (SB)',
                     'SOMALIA (SO)', 'SOUTH AFRICA (ZA)', 'SOUTH GEORGIA AND THE SOUTH SANDWICH ISLANDS (GS)',
                     'SPAIN (ES)',
                     'SRI LANKA (LK)', 'SURINAME (SR)', 'SVALBARD AND JAN MAYEN (SJ)', 'SWAZILAND (SZ)', 'SWEDEN (SE)',
                     'SWITZERLAND (CH)', 'TUNISIA (TN)', 'TURKEY (TR)', 'TURKMENISTAN (TM)',
                     'TURKS AND CAICOS ISLANDS (TC)',
                     'TUVALU (TV)', 'UGANDA (UG)', 'UKRAINE (UA)', 'UNITED ARAB EMIRATES (AE)', 'UNITED KINGDOM (UK)',
                     'UNITED STATES (US)', 'UNITED STATES MINOR OUTLYING ISLANDS (UM)', 'URUGUAY (UY)',
                     'UZBEKISTAN (UZ)',
                     'VANUATU (VU)', 'VENEZUELA (VE)', 'VIET NAM (VN)', 'VIRGIN ISLANDS, BRITISH (VG)',
                     'VIRGIN ISLANDS, U.S. (VI)',
                     'WALLIS AND FUTUNA (WF)', 'WESTERN SAHARA (EH)', 'YEMEN (YE)', 'ZAMBIA (ZM)', 'ZIMBABWE (ZW)',
                     'ÅLAND ISLANDS (AX)']

        for partner_type in partner_types:
            for country in countries:
                intPageNumber = 1
                data = f"message=%7B%22actions%22%3A%5B%7B%22id%22%3A%2290%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22FAPLX_PartnerSearchController%22%2C%22method%22%3A%22getPartnerTileList%22%2C%22params%22%3A%7B%22intPageSize%22%3A24%2C%22intPageNumber%22%3A{intPageNumber}%2C%22filters%22%3A%7B%22partnerTypes%22%3A%5B%22{urllib.parse.quote(partner_type)}%22%5D%2C%22countries%22%3A%5B%22{urllib.parse.quote(country)}%22%5D%2C%22city%22%3A%5B%5D%2C%22qualifications%22%3A%5B%5D%2C%22servicesCompetencies%22%3A%5B%5D%2C%22servicesDeliveryType%22%3A%5B%5D%2C%22serviceOffering%22%3A%5B%5D%2C%22dataCenterLocation%22%3A%5B%5D%2C%22cloudModel%22%3A%5B%5D%2C%22dataCenterArea%22%3A%5B%5D%2C%22industries%22%3A%5B%5D%2C%22federalUSContracts%22%3A%5B%5D%2C%22diversityCertifications%22%3A%5B%5D%7D%7D%2C%22cacheable%22%3Atrue%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22nj61v-uP3bGswhb-VTdr6Q%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22KbCmDBVbE10iCy1inwbbzA%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11yCoreCollector%22%3A%22kA6gW5EadEh9qZBKZj4IqQ%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2FFindAPartner%2Fs%2Fpartnersearch%3Flanguage%3Den_US%26country%3Dus%26partnerType%3Dfindareseller&aura.token=null"
                yield scrapy.Request(method='POST', url=self.api_link, body=data, callback=self.parse, headers=self.headers,
                                     meta={'country': country, 'partner_type': partner_type, 'data': data})

    def parse(self, response):
        if response.status == 200:
            partner_type = response.meta['partner_type']
            country = response.meta['country']

            jdata = response.json()
            if 'actions' in jdata and len(jdata) > 0 and 'returnValue' in jdata['actions'][0] and jdata['actions'][0]['state'] == 'SUCCESS':
                returnValue = jdata['actions'][0]['returnValue']['returnValue']
                intPageNumber = returnValue['intPageNumber']
                intPageSize = returnValue['intPageSize']
                intTotalItemCount = returnValue['intTotalItemCount']

                partners = jdata['actions'][0]['returnValue']['returnValue']['records']
                self.logger.info(f"Partner_Type: {partner_type}, Country: {country}, Page: {intPageNumber}, Result: {len(partners)}, TotalItemCount: {intTotalItemCount}")
                for partner in partners:
                    # Initialize item
                    item = dict()
                    for k in self.item_fields:
                        item[k] = ''

                    item['partner_program_link'] = 'https://www.delltechnologies.com/partner/en-us/partner/find-a-partner.htm'
                    item['partner_directory'] = 'Dell Technologies Partner'
                    item['partner_company_name'] = partner['track']['partner_track_account_name__c'] if 'partner_track_account_name__c' in partner['track'] else (('https://dellcommunities.force.com/FindAPartner/servlet/servlet.FileDownload?file=' + partner['track']['Logo__c']) if 'Logo__c' in partner['track'] else '')
                    if partner_type == '':
                        item['partner_type'] = partner['track']['Partner_Type__c']
                    else:
                        item['partner_type'] = partner_type

                    if 'track' in partner and 'Partner_Program_Tier__c' in partner['track']:
                        item['partner_tier'] = partner['track']['Partner_Program_Tier__c']

                    _id = partner['track']['Id']
                    data = f"message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22108%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22FAPLX_PartnerSearchController%22%2C%22method%22%3A%22getPartnerTrack%22%2C%22params%22%3A%7B%22partnerTrackId%22%3A%22{_id}%22%7D%2C%22cacheable%22%3Atrue%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22nj61v-uP3bGswhb-VTdr6Q%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22KbCmDBVbE10iCy1inwbbzA%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11yCoreCollector%22%3A%22kA6gW5EadEh9qZBKZj4IqQ%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2FFindAPartner%2Fs%2Fpartnerdetails%3FpartnerTrackId%3D{_id}%26country%3Dus%26language%3Den_US&aura.token=null"
                    yield scrapy.Request(method='POST', url=self.api_link, body=data, callback=self.parse_partner,
                                         headers=self.headers,
                                         meta={'item': item, 'data': data})

                # follow next pages
                if intPageNumber == 1 and intTotalItemCount > intPageSize:
                    for PageNumber in range(2, math.ceil(intTotalItemCount/intPageSize)+1):
                        data = f"message=%7B%22actions%22%3A%5B%7B%22id%22%3A%2290%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22FAPLX_PartnerSearchController%22%2C%22method%22%3A%22getPartnerTileList%22%2C%22params%22%3A%7B%22intPageSize%22%3A24%2C%22intPageNumber%22%3A{PageNumber}%2C%22filters%22%3A%7B%22partnerTypes%22%3A%5B%22{urllib.parse.quote(partner_type)}%22%5D%2C%22countries%22%3A%5B%22{urllib.parse.quote(country)}%22%5D%2C%22city%22%3A%5B%5D%2C%22qualifications%22%3A%5B%5D%2C%22servicesCompetencies%22%3A%5B%5D%2C%22servicesDeliveryType%22%3A%5B%5D%2C%22serviceOffering%22%3A%5B%5D%2C%22dataCenterLocation%22%3A%5B%5D%2C%22cloudModel%22%3A%5B%5D%2C%22dataCenterArea%22%3A%5B%5D%2C%22industries%22%3A%5B%5D%2C%22federalUSContracts%22%3A%5B%5D%2C%22diversityCertifications%22%3A%5B%5D%7D%7D%2C%22cacheable%22%3Atrue%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22nj61v-uP3bGswhb-VTdr6Q%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22KbCmDBVbE10iCy1inwbbzA%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11yCoreCollector%22%3A%22kA6gW5EadEh9qZBKZj4IqQ%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2FFindAPartner%2Fs%2Fpartnersearch%3Flanguage%3Den_US%26country%3Dus%26partnerType%3Dfindareseller&aura.token=null"
                        yield scrapy.Request(method='POST', url=self.api_link, body=data, callback=self.parse,
                                         headers=self.headers,
                                         meta={'country': country, 'partner_type': partner_type, 'data': data})
            else:
                print(jdata)
        else:
            print(response.status)

    def parse_partner(self, response):
        if response.status == 200:
            item = response.meta['item']
            jdata = response.json()
            if 'actions' in jdata and len(jdata) > 0 and 'returnValue' in jdata['actions'][0] and jdata['actions'][0]['state'] == 'SUCCESS':
                info = jdata['actions'][0]['returnValue']['returnValue']['track']

                if 'Services_Competencies__c' in info:
                    item['services'] = info['Services_Competencies__c']

                if 'Industries__c' in info:
                    item['industries'] = info['Industries__c']

                if 'Diversity_Certification__c' in info:
                    item['company_characteristics'] = info['Diversity_Certification__c']

                if 'Certification_Completed__c' in info:
                    item['competencies'] = info['Certification_Completed__c']

                if 'Partner_Self_Description_Short__c' in info or 'Partner_Self_Description_Long__c' in info:
                    item['company_description'] = info['Partner_Self_Description_Short__c']
                    if item['company_description'].strip() == '':
                        item['company_description'] = info['Partner_Self_Description_Long__c']

                # item['company_domain_name'] = profile['website_url']
                url_obj = urllib.parse.urlparse(info['Website__c'] if 'Website__c' in info else '')
                item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                if x:
                    item['company_domain_name'] = x[-1]

                if 'FAP_Social_Media_1__r' in info and len(info['FAP_Social_Media_1__r']) > 0:
                    social_data = info['FAP_Social_Media_1__r'][0]
                    item['linkedin_link'] = social_data['LinkedIn__c'] if 'LinkedIn__c' in social_data else ''
                    item['twitter_link'] = social_data['Twitter__c'] if 'Twitter__c' in social_data else ''
                    item['facebook_link'] = social_data['Facebook__c'] if 'Facebook__c' in social_data else ''
                    item['youtube_link'] = social_data['Youtube__c'] if 'Youtube__c' in social_data else ''
                    item['instagram_link'] = social_data['Instagram__c'] if 'Instagram__c' in social_data else ''

                for loc in info['Partner_Locations_1__r']:
                    if loc['Is_Primary__c']:
                        item['headquarters_street'] = loc['Street__c'] if 'Street__c' in loc else ''
                        item['headquarters_city'] = loc['City__c'] if 'City__c' in loc else ''
                        item['headquarters_state'] = loc['State_Province__c'] if 'State_Province__c' in loc else ''
                        item['headquarters_zipcode'] = loc['Postal_Code__c'] if 'Postal_Code__c' in loc else ''
                        item['headquarters_country'] = loc['Country__c'] if 'Country__c' in loc else ''
                        item['general_phone_number'] = loc['Point_of_Contact_Phone__c'] if 'Point_of_Contact_Phone__c' in loc else ''
                        item['general_email_address'] = loc['Point_of_Contact_Email__c'] if 'Point_of_Contact_Email__c' in loc else ''
                        item['primary_contact_name'] = loc['Point_of_Contact_Name__c'] if 'Point_of_Contact_Name__c' in loc else ''

                for loc in info['Partner_Locations_1__r']:
                    item['locations_street'] = loc['Street__c'] if 'Street__c' in loc else ''
                    item['locations_city'] = loc['City__c'] if 'City__c' in loc else ''
                    item['locations_state'] = loc['State_Province__c'] if 'State_Province__c' in loc else ''
                    item['locations_zipcode'] = loc['Postal_Code__c'] if 'Postal_Code__c' in loc else ''
                    item['locations_country'] = loc['Country__c'] if 'Country__c' in loc else ''
                    item['general_phone_number'] = loc['Point_of_Contact_Phone__c'] if 'Point_of_Contact_Phone__c' in loc else ''
                    item['general_email_address'] = loc['Point_of_Contact_Email__c'] if 'Point_of_Contact_Email__c' in loc else ''
                    item['primary_contact_name'] = loc['Point_of_Contact_Name__c'] if 'Point_of_Contact_Name__c' in loc else ''

                    yield item

            else:
                print(jdata)
        else:
            data = response.meta['data']
            yield scrapy.Request(method='POST', url=self.api_link, body=data, callback=self.parse_partner,
                                 headers=self.headers, meta=response.meta, dont_filter=True)

