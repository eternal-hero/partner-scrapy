# import needed libraries
import json
import math
import re

import requests
import scrapy
from techpartners.spiders.base_spider import BaseSpider
from bs4 import BeautifulSoup as BS
from techpartners.functions import *
import urllib.parse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'vmwarepartner'
    partner_program_link = 'https://partnerlocator.vmware.com/#sort=relevancy'
    partner_directory = 'VMware Partner'
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
                   'primary_contact_name', 'primary_contact_phone_number', 'primary_contact_email',
                   'industries', 'integrations', 'integration_level', 'competencies', 'partner_programs',
                   'validations', 'certifications', 'designations', 'contract_vehicles', 'certified_experts', 'certified',
                   'company_size', 'company_characteristics', 'partner_clients', 'notes']

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
        'Connection': 'keep-alive',
        'Authority': 'platform.cloud.coveo.com',
        'content-type': 'application/x-www-form-urlencoded; charset="UTF-8"',
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiJ9.eyJsaWNlbnNlRGVmaW5pdGlvbktleSI6IlBJRF9HdWVzdF9Vc2VyIiwidjgiOnRydWUsInJvbGVzIjpbInF1ZXJ5RXhlY3V0b3IiXSwidXNlcnR5cGUiOiJHdWVzdCIsInNhbGVzZm9yY2VDb21tdW5pdHkiOiJodHRwczpcL1wvcGFydG5lcmxvY2F0b3Iudm13YXJlLmNvbSIsInNhbGVzZm9yY2VVc2VyIjoicGFydG5lcl9sb2NhdG9yQHZtd2FyZS5mb3JjZS5jb20iLCJwaXBlbGluZSI6IkdQTyBQYXJ0bmVyIExvY2F0b3IiLCJ1c2VyR3JvdXBzIjpbIlBhcnRuZXIgTG9jYXRvciBQcm9maWxlIl0sInNlYXJjaEh1YiI6IkdQT1BhcnRuZXJMb2NhdG9yIiwic2FsZXNmb3JjZU9yZ2FuaXphdGlvbklkIjoiMDBENDAwMDAwMDA5aFFSRUFZIiwib3JnYW5pemF0aW9uIjoidm13YXJlZ3Nzc2VydmljZWNsb3VkN2tuZ2VsdTUiLCJ1c2VySWRzIjpbeyJwcm92aWRlciI6IkVtYWlsIFNlY3VyaXR5IFByb3ZpZGVyIiwibmFtZSI6ImFub255bW91cyIsInR5cGUiOiJVc2VyIn0seyJwcm92aWRlciI6IkVtYWlsIFNlY3VyaXR5IFByb3ZpZGVyIiwibmFtZSI6InBhcnRuZXJjZW50cmFsQHZtd2FyZS5jb20iLCJ0eXBlIjoiVXNlciJ9XSwiZXhwIjoxNjc3NjUyNjcyLCJpYXQiOjE2Nzc1NjYyNzIsInNhbGVzZm9yY2VGYWxsYmFja1RvQWRtaW4iOnRydWV9.WhuHpAOJOBSKFG1xZrdy5JWPsrkEslqs5lKeTk3wtHc',
        'Origin': 'https://partnerlocator.vmware.com',
        'Referer': 'https://partnerlocator.vmware.com/',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        }
    api_link = 'https://platform.cloud.coveo.com/rest/search/v2'

    def parse(self, response):
        if response.status != 200:
            self.logger.info(f'ERROR REQUEST STATUS: {response.status}, RESPONSE: {response.text}')
        else:
            soup = BS(response.text, "html.parser")
            scripts = soup.find_all('script')
            countries = dict()
            for script in scripts:
                if 'var countries' in script.text:
                    content = script.text[script.text.find('var countries'):]
                    content = content[content.find('=') + 1:]
                    content = content[: content.find(';')]
                    try:
                        countries = json.loads(content.strip())
                    except:
                        self.logger.info('countries ERROR')
                        self.logger.info(content.strip())
                    break

            for country in countries.keys():
                count = 0
                if country == 'UNITED STATES':
                    states = ['AK', 'AL', 'AR', 'AS', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MP', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']
                    for state in states:
                        payload = f"actionsHistory=%5B%5D&referrer=&visitorId=&isGuestUser=true&aq=(%40sfaccountcountryc%3D%3D%22{urllib.parse.quote(country)}%22)%20(%40sfaccountstateprovincec%3D%3D{state})%20(%24qre(expression%3A%20%40sfaccountcountryc%3D%22{urllib.parse.quote(country)}%22%2C%20modifier%3A%2050))%20(%24qre(expression%3A%20%40sfaccountstateprovincec%3D%22{state}%22%2C%20modifier%3A%2050))&dq=(%5B%5B%40foldfoldingfield%5D%20(%40sfaccountcountryc%3D%3D%22{urllib.parse.quote(country)}%22)%20(%40sfaccountstateprovincec%3D%3D{state})%5D)&searchHub=GPOPartnerLocator&locale=en&firstResult={count}&numberOfResults=10&excerptLength=200&filterField=%40foldfoldingfield&filterFieldRange=3&parentField=%40foldparentfield&childField=%40foldchildfield&fieldsToInclude=%5B%22sfaccountname%22%2C%22sfaccountaddress1delc%22%2C%22sfaccountaddress2c%22%2C%22sfaccountaddress3c%22%2C%22sfaccountaddress4c%22%2C%22sfaccountcityc%22%2C%22sfaccountstateprovincec%22%2C%22sfaccountzippostalcodec%22%2C%22sfaccountcountryc%22%2C%22sfprogramdetailspartnertypec%22%2C%22sfaccountphone%22%2C%22sfaccountpartnerlocatorcontactusemailc%22%2C%22sfaccountwebsite%22%2C%22outlookformacuri%22%2C%22outlookuri%22%2C%22connectortype%22%2C%22urihash%22%2C%22collection%22%2C%22source%22%2C%22author%22%2C%22%40sfaccountid%22%2C%22%40sfaccountpartnerlocatorcontactusemailc%22%2C%22%40sfaccountaccountnameextendedc%22%2C%22%40sfaccountcountryc%22%2C%22%40sfaccountstateprovincec%22%2C%22%40sfaccountzippostalcodec%22%2C%22totalNumberOfChildResults%22%2C%22sfid%22%2C%22plocpartnertype%22%2C%22plocpartnerlevel%22%2C%22sfaccountcompanynamenativelanguagec%22%2C%22language%22%2C%22objecttype%22%2C%22permanentid%22%2C%22%40foldfoldingfield%22%2C%22%40foldchildfield%22%2C%22%40foldparentfield%22%5D&enableDidYouMean=true&sortCriteria=relevancy&queryFunctions=%5B%5D&rankingFunctions=%5B%5D&groupBy=%5B%7B%22field%22%3A%22%40plocpartnertype%22%2C%22maximumNumberOfValues%22%3A13%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Afalse%2C%22allowedValues%22%3A%5B%22Corporate%20Reseller%22%2C%22Distributor%22%2C%22OEM%20Corporate%20Reseller%22%2C%22Global%20OEM%20Alliance%22%2C%22Service%20Provider%22%2C%22Solution%20Provider%22%2C%22Technology%20Alliance%20Partner%22%2C%22Training%20Center%22%2C%22VMware%20Partner%20Network%22%2C%22Partner%20Professional%20Services%22%2C%22Technology%20Alliance%20Partner%20Old%22%2C%22Partner%20Connect%22%5D%7D%2C%7B%22field%22%3A%22%40plocpartnerlevel%22%2C%22maximumNumberOfValues%22%3A6%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Afalse%2C%22allowedValues%22%3A%5B%22Principal%22%2C%22Strategic%22%5D%7D%2C%7B%22field%22%3A%22%40plocmasterservicescompetencies%22%2C%22maximumNumberOfValues%22%3A18%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Afalse%2C%22allowedValues%22%3A%5B%22Cloud%20Management%20and%20Automation%22%2C%22Cloud%20Native%22%2C%22Cloud%20Verified%22%2C%22Data%20Center%20Virtualization%22%2C%22Digital%20Workspace%22%2C%22Master%20Services%20Competency%20Cloud%20Management%20and%20Automation%22%2C%22Master%20Services%20Competency%20Cloud%20Native%22%2C%22Master%20Services%20Competency%20Data%20Center%20Virtualization%22%2C%22Master%20Services%20Competency%20Digital%20Workspace%22%2C%22Master%20Services%20Competency%20Network%20Virtualization%22%2C%22Master%20Services%20Competency%20Software%20Defined%20-%20Wide%20Area%20Network%22%2C%22Master%20Services%20Competency%20VMware%20Cloud%20Foundation%22%2C%22Master%20Services%20Competency%20VMware%20Cloud%20on%20AWS%22%2C%22Network%20Virtualization%22%2C%22Software%20Defined%20-%20Wide%20Area%20Network%22%2C%22VMware%20Cloud%20Foundation%22%2C%22VMware%20Cloud%20on%20AWS%22%5D%7D%2C%7B%22field%22%3A%22%40ploccompetencies%22%2C%22maximumNumberOfValues%22%3A15%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Afalse%2C%22allowedValues%22%3A%5B%22Business%20Continuity%22%2C%22Management%20Automation%22%2C%22Management%20Operations%22%2C%22Desktop%20Virtualization%22%2C%22Cloud%20Provider%22%2C%22Network%20Virtualization%22%2C%22Mobility%20Management%22%2C%22Hyper-Converged%20Infrastructure%22%2C%22VMware%20Cloud%20on%20AWS%22%2C%22Modern%20Applications%20Platform%22%2C%22Server%20Virtualization%22%2C%22Endpoint%20Protection%22%2C%22Network%20Security%22%2C%22Secure%20Access%20Service%20Edge%20*%22%5D%7D%2C%7B%22field%22%3A%22%40plocbadges%22%2C%22maximumNumberOfValues%22%3A7%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%22Cloud%20Solutions%20Badge%22%2C%22Data%20Center%20Badge%22%2C%22Digital%20Workspace%20Badge%22%2C%22Modern%20Applications%20Badge%22%2C%22Networking%20and%20Security%20Badge%22%2C%22Security%20Badge%22%5D%7D%2C%7B%22field%22%3A%22%40plocpurchasingprograms%22%2C%22maximumNumberOfValues%22%3A7%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%22VPP%22%2C%22EPP%22%2C%22HPP%22%2C%22SPPP%22%2C%22SPPM%22%2C%22SDP%22%5D%2C%22advancedQueryOverride%22%3A%22(%40sfaccountcountryc%3D%3D%5C%22{urllib.parse.quote(country)}%5C%22)%20(%40sfaccountstateprovincec%3D%3D{state})%22%2C%22constantQueryOverride%22%3A%22NOT%20%40plocpurchasingprograms%3D%3D%5C%22EPP%5C%22%22%7D%2C%7B%22field%22%3A%22%40sfaccountcountryc%22%2C%22maximumNumberOfValues%22%3A7%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%22{urllib.parse.quote(country)}%22%5D%7D%2C%7B%22field%22%3A%22%40sfaccountstateprovincec%22%2C%22maximumNumberOfValues%22%3A7%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%22{state}%22%5D%7D%5D&facetOptions=%7B%7D&categoryFacets=%5B%5D&retrieveFirstSentences=true&timezone=UTC&enableQuerySyntax=false&enableDuplicateFiltering=false&enableCollaborativeRating=false&debug=false&context=%7B%22country%22%3A%5B%22{urllib.parse.quote(country)}%22%5D%2C%22host%22%3A%22partnerlocator.vmware.com%22%7D&allowQueriesWithoutKeywords=true"
                        yield scrapy.Request(method='POST', url=self.api_link, callback=self.parse_country,
                                             headers=self.headers, body=payload,
                                             meta={'country': country,
                                                   'state': state,
                                                   'count': count})
                else:
                    payload = f"actionsHistory=%5B%5D&referrer=&visitorId=&isGuestUser=true&aq=(%40sfaccountcountryc%3D%3D%22{urllib.parse.quote(country)}%22)%20(%24qre(expression%3A%20%40sfaccountcountryc%3D%22{urllib.parse.quote(country)}%22%2C%20modifier%3A%2050))&dq=(%5B%5B%40foldfoldingfield%5D%20%40sfaccountcountryc%3D%3D%22{urllib.parse.quote(country)}%22%5D)&searchHub=GPOPartnerLocator&locale=en&firstResult={count}&numberOfResults=10&excerptLength=200&filterField=%40foldfoldingfield&filterFieldRange=3&parentField=%40foldparentfield&childField=%40foldchildfield&fieldsToInclude=%5B%22sfaccountname%22%2C%22sfaccountaddress1delc%22%2C%22sfaccountaddress2c%22%2C%22sfaccountaddress3c%22%2C%22sfaccountaddress4c%22%2C%22sfaccountcityc%22%2C%22sfaccountstateprovincec%22%2C%22sfaccountzippostalcodec%22%2C%22sfaccountcountryc%22%2C%22sfprogramdetailspartnertypec%22%2C%22sfaccountphone%22%2C%22sfaccountpartnerlocatorcontactusemailc%22%2C%22sfaccountwebsite%22%2C%22outlookformacuri%22%2C%22outlookuri%22%2C%22connectortype%22%2C%22urihash%22%2C%22collection%22%2C%22source%22%2C%22author%22%2C%22%40sfaccountid%22%2C%22%40sfaccountpartnerlocatorcontactusemailc%22%2C%22%40sfaccountaccountnameextendedc%22%2C%22%40sfaccountcountryc%22%2C%22%40sfaccountstateprovincec%22%2C%22%40sfaccountzippostalcodec%22%2C%22totalNumberOfChildResults%22%2C%22sfid%22%2C%22plocpartnertype%22%2C%22plocpartnerlevel%22%2C%22sfaccountcompanynamenativelanguagec%22%2C%22language%22%2C%22objecttype%22%2C%22permanentid%22%2C%22%40foldfoldingfield%22%2C%22%40foldchildfield%22%2C%22%40foldparentfield%22%5D&enableDidYouMean=true&sortCriteria=relevancy&queryFunctions=%5B%5D&rankingFunctions=%5B%5D&groupBy=%5B%7B%22field%22%3A%22%40plocpartnertype%22%2C%22maximumNumberOfValues%22%3A13%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Afalse%2C%22allowedValues%22%3A%5B%22Corporate%20Reseller%22%2C%22Distributor%22%2C%22OEM%20Corporate%20Reseller%22%2C%22Global%20OEM%20Alliance%22%2C%22Service%20Provider%22%2C%22Solution%20Provider%22%2C%22Technology%20Alliance%20Partner%22%2C%22Training%20Center%22%2C%22VMware%20Partner%20Network%22%2C%22Partner%20Professional%20Services%22%2C%22Technology%20Alliance%20Partner%20Old%22%2C%22Partner%20Connect%22%5D%7D%2C%7B%22field%22%3A%22%40plocpartnerlevel%22%2C%22maximumNumberOfValues%22%3A6%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Afalse%2C%22allowedValues%22%3A%5B%22Principal%22%2C%22Strategic%22%5D%7D%2C%7B%22field%22%3A%22%40plocmasterservicescompetencies%22%2C%22maximumNumberOfValues%22%3A18%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Afalse%2C%22allowedValues%22%3A%5B%22Cloud%20Management%20and%20Automation%22%2C%22Cloud%20Native%22%2C%22Cloud%20Verified%22%2C%22Data%20Center%20Virtualization%22%2C%22Digital%20Workspace%22%2C%22Master%20Services%20Competency%20Cloud%20Management%20and%20Automation%22%2C%22Master%20Services%20Competency%20Cloud%20Native%22%2C%22Master%20Services%20Competency%20Data%20Center%20Virtualization%22%2C%22Master%20Services%20Competency%20Digital%20Workspace%22%2C%22Master%20Services%20Competency%20Network%20Virtualization%22%2C%22Master%20Services%20Competency%20Software%20Defined%20-%20Wide%20Area%20Network%22%2C%22Master%20Services%20Competency%20VMware%20Cloud%20Foundation%22%2C%22Master%20Services%20Competency%20VMware%20Cloud%20on%20AWS%22%2C%22Network%20Virtualization%22%2C%22Software%20Defined%20-%20Wide%20Area%20Network%22%2C%22VMware%20Cloud%20Foundation%22%2C%22VMware%20Cloud%20on%20AWS%22%5D%7D%2C%7B%22field%22%3A%22%40ploccompetencies%22%2C%22maximumNumberOfValues%22%3A15%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Afalse%2C%22allowedValues%22%3A%5B%22Business%20Continuity%22%2C%22Management%20Automation%22%2C%22Management%20Operations%22%2C%22Desktop%20Virtualization%22%2C%22Cloud%20Provider%22%2C%22Network%20Virtualization%22%2C%22Mobility%20Management%22%2C%22Hyper-Converged%20Infrastructure%22%2C%22VMware%20Cloud%20on%20AWS%22%2C%22Modern%20Applications%20Platform%22%2C%22Server%20Virtualization%22%2C%22Endpoint%20Protection%22%2C%22Network%20Security%22%2C%22Secure%20Access%20Service%20Edge%20*%22%5D%7D%2C%7B%22field%22%3A%22%40plocbadges%22%2C%22maximumNumberOfValues%22%3A7%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%22Cloud%20Solutions%20Badge%22%2C%22Data%20Center%20Badge%22%2C%22Digital%20Workspace%20Badge%22%2C%22Modern%20Applications%20Badge%22%2C%22Networking%20and%20Security%20Badge%22%2C%22Security%20Badge%22%5D%7D%2C%7B%22field%22%3A%22%40plocpurchasingprograms%22%2C%22maximumNumberOfValues%22%3A7%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%22VPP%22%2C%22EPP%22%2C%22HPP%22%2C%22SPPP%22%2C%22SPPM%22%2C%22SDP%22%5D%2C%22advancedQueryOverride%22%3A%22%40sfaccountcountryc%3D%3D%5C%22{urllib.parse.quote(country)}%5C%22%22%2C%22constantQueryOverride%22%3A%22NOT%20%40plocpurchasingprograms%3D%3D%5C%22EPP%5C%22%22%7D%2C%7B%22field%22%3A%22%40sfaccountcountryc%22%2C%22maximumNumberOfValues%22%3A7%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%22{urllib.parse.quote(country)}%22%5D%7D%2C%7B%22field%22%3A%22%40sfaccountstateprovincec%22%2C%22maximumNumberOfValues%22%3A7%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%5D&facetOptions=%7B%7D&categoryFacets=%5B%5D&retrieveFirstSentences=true&timezone=UTC&enableQuerySyntax=false&enableDuplicateFiltering=false&enableCollaborativeRating=false&debug=false&context=%7B%22country%22%3A%5B%22{urllib.parse.quote(country)}%22%5D%2C%22host%22%3A%22partnerlocator.vmware.com%22%7D&allowQueriesWithoutKeywords=true"
                    yield scrapy.Request(method='POST', url=self.api_link, callback=self.parse_country,
                                         headers=self.headers, body=payload,
                                         meta={'country': country,
                                               'state': '',
                                               'count': count})

    def parse_country(self, response):

        count = response.meta['count']
        state = response.meta['state']
        country = response.meta['country']
        total_count = 0
        partners = list()

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST COUNTRY STATUS: {response.status}, RESPONSE: {response.text}')
        else:
            try:
                j = json.loads(response.text)
                if 'totalCount' in j:
                    total_count = j['totalCount']
                    partners = j['results']
            except:
                self.logger.info(f'ERROR PARSING COUNTRY: {country}')

        if len(partners) > 0:
            self.logger.info(f'Country: {country}{(", State: " + state) if state != "" else ""}, Count: {count}, totalCount: {total_count}, Partners#: {len(partners)}')
            for partner in partners:

                if 'parentResult' in partner and partner['parentResult']:
                    partner = partner['parentResult']

                # Initialize item
                item = dict()
                for k in self.item_fields:
                    item[k] = ''

                item['partner_program_link'] = self.partner_program_link
                item['partner_directory'] = self.partner_directory
                item['partner_program_name'] = self.partner_program_name

                item['partner_company_name'] = partner['title'] if 'title' in partner else ''
                if 'raw' in partner:
                    item['partner_type'] = partner['raw']['plocpartnertype'] if 'plocpartnertype' in partner['raw'] else ''
                    item['partner_tier'] = partner['raw']['plocpartnerlevel'] if 'plocpartnerlevel' in partner['raw'] else ''

                    item['general_email_address'] = partner['raw']['sfaccountpartnerlocatorcontactusemailc'] if 'sfaccountpartnerlocatorcontactusemailc' in partner['raw'] else ''
                    item['general_phone_number'] = partner['raw']['sfaccountphone'] if 'sfaccountphone' in partner['raw'] else ''

                    item['headquarters_street'] = partner['raw']['sfaccountaddress1delc'] if 'sfaccountaddress1delc' in partner['raw'] else ''
                    item['headquarters_city'] = partner['raw']['sfaccountcityc'] if 'sfaccountcityc' in partner['raw'] else ''
                    item['headquarters_state'] = partner['raw']['sfaccountstateprovincec'] if 'sfaccountstateprovincec' in partner['raw'] else ''
                    item['headquarters_country'] = partner['raw']['sfaccountcountryc'] if 'sfaccountcountryc' in partner['raw'] else ''
                    item['headquarters_zipcode'] = partner['raw']['sfaccountzippostalcodec'] if 'sfaccountzippostalcodec' in partner['raw'] else ''

                    item['company_domain_name'] = get_domain_from_url(partner['raw']['sfaccountwebsite'] if 'sfaccountwebsite' in partner['raw'] else '')
                    # try:
                    #     url_obj = urllib.parse.urlparse(item['company_domain_name'])
                    #     item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
                    #     x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
                    #     if x:
                    #         item['company_domain_name'] = x[-1]
                    #     if '/' in item['company_domain_name']:
                    #         item['company_domain_name'] = item['company_domain_name'][
                    #                                       :item['company_domain_name'].find('/')]
                    # except Exception as e:
                    #     print('DOMAIN ERROR: ', e)

                    _id = partner['raw']['sfid'] if 'sfid' in partner['raw'] else ''
                    if _id:
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36',
                            'Connection': 'keep-alive',
                            'Authority': 'platform.cloud.coveo.com',
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Accept-Language': 'en-US,en;q=0.9',
                            'Host': 'partnerlocator.vmware.com',
                            'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
                            'sec-ch-ua-mobile': '?0',
                            'sec-ch-ua-platform': 'Windows',
                            'Sec-Fetch-Dest': 'document',
                            'Sec-Fetch-Mode': 'navigate',
                            'Sec-Fetch-Site': 'none',
                            'Upgrade-Insecure-Requests': 1,
                        }
                        partner_link = f'https://partnerlocator.vmware.com/PartnerView?id={_id}&lang=en'
                        yield scrapy.Request(method='GET', url=partner_link, callback=self.parse_partner,
                                             headers=headers,
                                             meta={'item': item})
                    else:
                        yield item
                else:
                    yield item

        # follow next page
        if count == 0 and total_count > 10:
            for i in range(1, math.ceil(total_count / 10.0)):
                count = 10 * i
                if state == '':
                    payload = f"actionsHistory=%5B%5D&referrer=&visitorId=&isGuestUser=true&aq=(%40sfaccountcountryc%3D%3D%22{urllib.parse.quote(country)}%22)%20(%24qre(expression%3A%20%40sfaccountcountryc%3D%22{urllib.parse.quote(country)}%22%2C%20modifier%3A%2050))&dq=(%5B%5B%40foldfoldingfield%5D%20%40sfaccountcountryc%3D%3D%22{urllib.parse.quote(country)}%22%5D)&searchHub=GPOPartnerLocator&locale=en&firstResult={count}&numberOfResults=10&excerptLength=200&filterField=%40foldfoldingfield&filterFieldRange=3&parentField=%40foldparentfield&childField=%40foldchildfield&fieldsToInclude=%5B%22sfaccountname%22%2C%22sfaccountaddress1delc%22%2C%22sfaccountaddress2c%22%2C%22sfaccountaddress3c%22%2C%22sfaccountaddress4c%22%2C%22sfaccountcityc%22%2C%22sfaccountstateprovincec%22%2C%22sfaccountzippostalcodec%22%2C%22sfaccountcountryc%22%2C%22sfprogramdetailspartnertypec%22%2C%22sfaccountphone%22%2C%22sfaccountpartnerlocatorcontactusemailc%22%2C%22sfaccountwebsite%22%2C%22outlookformacuri%22%2C%22outlookuri%22%2C%22connectortype%22%2C%22urihash%22%2C%22collection%22%2C%22source%22%2C%22author%22%2C%22%40sfaccountid%22%2C%22%40sfaccountpartnerlocatorcontactusemailc%22%2C%22%40sfaccountaccountnameextendedc%22%2C%22%40sfaccountcountryc%22%2C%22%40sfaccountstateprovincec%22%2C%22%40sfaccountzippostalcodec%22%2C%22totalNumberOfChildResults%22%2C%22sfid%22%2C%22plocpartnertype%22%2C%22plocpartnerlevel%22%2C%22sfaccountcompanynamenativelanguagec%22%2C%22language%22%2C%22objecttype%22%2C%22permanentid%22%2C%22%40foldfoldingfield%22%2C%22%40foldchildfield%22%2C%22%40foldparentfield%22%5D&enableDidYouMean=true&sortCriteria=relevancy&queryFunctions=%5B%5D&rankingFunctions=%5B%5D&groupBy=%5B%7B%22field%22%3A%22%40plocpartnertype%22%2C%22maximumNumberOfValues%22%3A13%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Afalse%2C%22allowedValues%22%3A%5B%22Corporate%20Reseller%22%2C%22Distributor%22%2C%22OEM%20Corporate%20Reseller%22%2C%22Global%20OEM%20Alliance%22%2C%22Service%20Provider%22%2C%22Solution%20Provider%22%2C%22Technology%20Alliance%20Partner%22%2C%22Training%20Center%22%2C%22VMware%20Partner%20Network%22%2C%22Partner%20Professional%20Services%22%2C%22Technology%20Alliance%20Partner%20Old%22%2C%22Partner%20Connect%22%5D%7D%2C%7B%22field%22%3A%22%40plocpartnerlevel%22%2C%22maximumNumberOfValues%22%3A6%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Afalse%2C%22allowedValues%22%3A%5B%22Principal%22%2C%22Strategic%22%5D%7D%2C%7B%22field%22%3A%22%40plocmasterservicescompetencies%22%2C%22maximumNumberOfValues%22%3A18%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Afalse%2C%22allowedValues%22%3A%5B%22Cloud%20Management%20and%20Automation%22%2C%22Cloud%20Native%22%2C%22Cloud%20Verified%22%2C%22Data%20Center%20Virtualization%22%2C%22Digital%20Workspace%22%2C%22Master%20Services%20Competency%20Cloud%20Management%20and%20Automation%22%2C%22Master%20Services%20Competency%20Cloud%20Native%22%2C%22Master%20Services%20Competency%20Data%20Center%20Virtualization%22%2C%22Master%20Services%20Competency%20Digital%20Workspace%22%2C%22Master%20Services%20Competency%20Network%20Virtualization%22%2C%22Master%20Services%20Competency%20Software%20Defined%20-%20Wide%20Area%20Network%22%2C%22Master%20Services%20Competency%20VMware%20Cloud%20Foundation%22%2C%22Master%20Services%20Competency%20VMware%20Cloud%20on%20AWS%22%2C%22Network%20Virtualization%22%2C%22Software%20Defined%20-%20Wide%20Area%20Network%22%2C%22VMware%20Cloud%20Foundation%22%2C%22VMware%20Cloud%20on%20AWS%22%5D%7D%2C%7B%22field%22%3A%22%40ploccompetencies%22%2C%22maximumNumberOfValues%22%3A15%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Afalse%2C%22allowedValues%22%3A%5B%22Business%20Continuity%22%2C%22Management%20Automation%22%2C%22Management%20Operations%22%2C%22Desktop%20Virtualization%22%2C%22Cloud%20Provider%22%2C%22Network%20Virtualization%22%2C%22Mobility%20Management%22%2C%22Hyper-Converged%20Infrastructure%22%2C%22VMware%20Cloud%20on%20AWS%22%2C%22Modern%20Applications%20Platform%22%2C%22Server%20Virtualization%22%2C%22Endpoint%20Protection%22%2C%22Network%20Security%22%2C%22Secure%20Access%20Service%20Edge%20*%22%5D%7D%2C%7B%22field%22%3A%22%40plocbadges%22%2C%22maximumNumberOfValues%22%3A7%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%22Cloud%20Solutions%20Badge%22%2C%22Data%20Center%20Badge%22%2C%22Digital%20Workspace%20Badge%22%2C%22Modern%20Applications%20Badge%22%2C%22Networking%20and%20Security%20Badge%22%2C%22Security%20Badge%22%5D%7D%2C%7B%22field%22%3A%22%40plocpurchasingprograms%22%2C%22maximumNumberOfValues%22%3A7%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%22VPP%22%2C%22EPP%22%2C%22HPP%22%2C%22SPPP%22%2C%22SPPM%22%2C%22SDP%22%5D%2C%22advancedQueryOverride%22%3A%22%40sfaccountcountryc%3D%3D%5C%22{urllib.parse.quote(country)}%5C%22%22%2C%22constantQueryOverride%22%3A%22NOT%20%40plocpurchasingprograms%3D%3D%5C%22EPP%5C%22%22%7D%2C%7B%22field%22%3A%22%40sfaccountcountryc%22%2C%22maximumNumberOfValues%22%3A7%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%22{urllib.parse.quote(country)}%22%5D%7D%2C%7B%22field%22%3A%22%40sfaccountstateprovincec%22%2C%22maximumNumberOfValues%22%3A7%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%5D&facetOptions=%7B%7D&categoryFacets=%5B%5D&retrieveFirstSentences=true&timezone=UTC&enableQuerySyntax=false&enableDuplicateFiltering=false&enableCollaborativeRating=false&debug=false&context=%7B%22country%22%3A%5B%22{urllib.parse.quote(country)}%22%5D%2C%22host%22%3A%22partnerlocator.vmware.com%22%7D&allowQueriesWithoutKeywords=true"
                else:
                    payload = f"actionsHistory=%5B%5D&referrer=&visitorId=&isGuestUser=true&aq=(%40sfaccountcountryc%3D%3D%22{urllib.parse.quote(country)}%22)%20(%40sfaccountstateprovincec%3D%3D{state})%20(%24qre(expression%3A%20%40sfaccountcountryc%3D%22{urllib.parse.quote(country)}%22%2C%20modifier%3A%2050))%20(%24qre(expression%3A%20%40sfaccountstateprovincec%3D%22{state}%22%2C%20modifier%3A%2050))&dq=(%5B%5B%40foldfoldingfield%5D%20(%40sfaccountcountryc%3D%3D%22{urllib.parse.quote(country)}%22)%20(%40sfaccountstateprovincec%3D%3D{state})%5D)&searchHub=GPOPartnerLocator&locale=en&firstResult={count}&numberOfResults=10&excerptLength=200&filterField=%40foldfoldingfield&filterFieldRange=3&parentField=%40foldparentfield&childField=%40foldchildfield&fieldsToInclude=%5B%22sfaccountname%22%2C%22sfaccountaddress1delc%22%2C%22sfaccountaddress2c%22%2C%22sfaccountaddress3c%22%2C%22sfaccountaddress4c%22%2C%22sfaccountcityc%22%2C%22sfaccountstateprovincec%22%2C%22sfaccountzippostalcodec%22%2C%22sfaccountcountryc%22%2C%22sfprogramdetailspartnertypec%22%2C%22sfaccountphone%22%2C%22sfaccountpartnerlocatorcontactusemailc%22%2C%22sfaccountwebsite%22%2C%22outlookformacuri%22%2C%22outlookuri%22%2C%22connectortype%22%2C%22urihash%22%2C%22collection%22%2C%22source%22%2C%22author%22%2C%22%40sfaccountid%22%2C%22%40sfaccountpartnerlocatorcontactusemailc%22%2C%22%40sfaccountaccountnameextendedc%22%2C%22%40sfaccountcountryc%22%2C%22%40sfaccountstateprovincec%22%2C%22%40sfaccountzippostalcodec%22%2C%22totalNumberOfChildResults%22%2C%22sfid%22%2C%22plocpartnertype%22%2C%22plocpartnerlevel%22%2C%22sfaccountcompanynamenativelanguagec%22%2C%22language%22%2C%22objecttype%22%2C%22permanentid%22%2C%22%40foldfoldingfield%22%2C%22%40foldchildfield%22%2C%22%40foldparentfield%22%5D&enableDidYouMean=true&sortCriteria=relevancy&queryFunctions=%5B%5D&rankingFunctions=%5B%5D&groupBy=%5B%7B%22field%22%3A%22%40plocpartnertype%22%2C%22maximumNumberOfValues%22%3A13%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Afalse%2C%22allowedValues%22%3A%5B%22Corporate%20Reseller%22%2C%22Distributor%22%2C%22OEM%20Corporate%20Reseller%22%2C%22Global%20OEM%20Alliance%22%2C%22Service%20Provider%22%2C%22Solution%20Provider%22%2C%22Technology%20Alliance%20Partner%22%2C%22Training%20Center%22%2C%22VMware%20Partner%20Network%22%2C%22Partner%20Professional%20Services%22%2C%22Technology%20Alliance%20Partner%20Old%22%2C%22Partner%20Connect%22%5D%7D%2C%7B%22field%22%3A%22%40plocpartnerlevel%22%2C%22maximumNumberOfValues%22%3A6%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Afalse%2C%22allowedValues%22%3A%5B%22Principal%22%2C%22Strategic%22%5D%7D%2C%7B%22field%22%3A%22%40plocmasterservicescompetencies%22%2C%22maximumNumberOfValues%22%3A18%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Afalse%2C%22allowedValues%22%3A%5B%22Cloud%20Management%20and%20Automation%22%2C%22Cloud%20Native%22%2C%22Cloud%20Verified%22%2C%22Data%20Center%20Virtualization%22%2C%22Digital%20Workspace%22%2C%22Master%20Services%20Competency%20Cloud%20Management%20and%20Automation%22%2C%22Master%20Services%20Competency%20Cloud%20Native%22%2C%22Master%20Services%20Competency%20Data%20Center%20Virtualization%22%2C%22Master%20Services%20Competency%20Digital%20Workspace%22%2C%22Master%20Services%20Competency%20Network%20Virtualization%22%2C%22Master%20Services%20Competency%20Software%20Defined%20-%20Wide%20Area%20Network%22%2C%22Master%20Services%20Competency%20VMware%20Cloud%20Foundation%22%2C%22Master%20Services%20Competency%20VMware%20Cloud%20on%20AWS%22%2C%22Network%20Virtualization%22%2C%22Software%20Defined%20-%20Wide%20Area%20Network%22%2C%22VMware%20Cloud%20Foundation%22%2C%22VMware%20Cloud%20on%20AWS%22%5D%7D%2C%7B%22field%22%3A%22%40ploccompetencies%22%2C%22maximumNumberOfValues%22%3A15%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Afalse%2C%22allowedValues%22%3A%5B%22Business%20Continuity%22%2C%22Management%20Automation%22%2C%22Management%20Operations%22%2C%22Desktop%20Virtualization%22%2C%22Cloud%20Provider%22%2C%22Network%20Virtualization%22%2C%22Mobility%20Management%22%2C%22Hyper-Converged%20Infrastructure%22%2C%22VMware%20Cloud%20on%20AWS%22%2C%22Modern%20Applications%20Platform%22%2C%22Server%20Virtualization%22%2C%22Endpoint%20Protection%22%2C%22Network%20Security%22%2C%22Secure%20Access%20Service%20Edge%20*%22%5D%7D%2C%7B%22field%22%3A%22%40plocbadges%22%2C%22maximumNumberOfValues%22%3A7%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%22Cloud%20Solutions%20Badge%22%2C%22Data%20Center%20Badge%22%2C%22Digital%20Workspace%20Badge%22%2C%22Modern%20Applications%20Badge%22%2C%22Networking%20and%20Security%20Badge%22%2C%22Security%20Badge%22%5D%7D%2C%7B%22field%22%3A%22%40plocpurchasingprograms%22%2C%22maximumNumberOfValues%22%3A7%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%22VPP%22%2C%22EPP%22%2C%22HPP%22%2C%22SPPP%22%2C%22SPPM%22%2C%22SDP%22%5D%2C%22advancedQueryOverride%22%3A%22(%40sfaccountcountryc%3D%3D%5C%22{urllib.parse.quote(country)}%5C%22)%20(%40sfaccountstateprovincec%3D%3D{state})%22%2C%22constantQueryOverride%22%3A%22NOT%20%40plocpurchasingprograms%3D%3D%5C%22EPP%5C%22%22%7D%2C%7B%22field%22%3A%22%40sfaccountcountryc%22%2C%22maximumNumberOfValues%22%3A7%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%22{urllib.parse.quote(country)}%22%5D%7D%2C%7B%22field%22%3A%22%40sfaccountstateprovincec%22%2C%22maximumNumberOfValues%22%3A7%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%22{state}%22%5D%7D%5D&facetOptions=%7B%7D&categoryFacets=%5B%5D&retrieveFirstSentences=true&timezone=UTC&enableQuerySyntax=false&enableDuplicateFiltering=false&enableCollaborativeRating=false&debug=false&context=%7B%22country%22%3A%5B%22{urllib.parse.quote(country)}%22%5D%2C%22host%22%3A%22partnerlocator.vmware.com%22%7D&allowQueriesWithoutKeywords=true"
                yield scrapy.Request(method='POST', url=self.api_link, callback=self.parse_country,
                                     headers=self.headers, body=payload,
                                     meta={'country': country,
                                           'state': state,
                                           'count': count})

    def parse_partner(self, response):

        item = response.meta['item']

        if response.status != 200:
            self.logger.info(f'ERROR REQUEST PARTNER, COMPANY: {item["partner_company_name"]}, URL: {response.request.url}')
        else:
            soup = BS(response.text, "html.parser")

            item['company_description'] = cleanhtml(soup.find('span', {'class': 'company-overview'}).text if soup.find('span', {'class': 'company-overview'}) else '')
            if item['company_description'] == 'na':
                item['company_description'] = ''
            all_h2 = soup.find('div', {'class': 'detail-section active-section'}).find_all('h2')
            for h2 in all_h2:
                if 'Solution Competency' in h2.text:
                    item['solutions'] = [li.text for li in (h2.findNext('div', {'class': 'competencies'}).find_all('li') if soup.find('div', {'class': 'competencies'}) else list())]
                elif 'Master Services Competency' in h2.text:
                    item['services'] = [li.text for li in (h2.findNext('div', {'class': 'competencies'}).find_all('li') if soup.find('div', {'class': 'competencies'}) else list())]
                elif 'Purchasing Programs' in h2.text:
                    item['pricing'] = [li.text for li in (h2.findNext('div', {'class': 'competencies'}).find_all('li') if soup.find('div', {'class': 'competencies'}) else list())]

            locations = soup.find('table', {'class': 'locations'}).find_all('tr') if soup.find('table', {'class': 'locations'}) else list()
            if len(locations) > 0:
                for location in locations:
                    location_text = location.text
                    item['locations_address'] = cleanhtml(location_text)
                    item['locations_country'] = location_text.splitlines()[-2].strip()
                    yield item
            else:
                yield item
