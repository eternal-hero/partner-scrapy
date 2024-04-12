from techpartners.spiders.base_spider import BaseSpider
import scrapy,requests,string,json
from techpartners.functions import *
from price_parser import Price

domain = 'https://appexchange.salesforce.com'
class Spider(BaseSpider):
    name = 'salesforce_consulting'
    partner_program_link = 'https://appexchange.salesforce.com/consulting'
    partner_directory = 'Salesforce Directory'
    partner_program_name = ''
    crawl_id = 1290

    start_urls=['https://appexchange.salesforce.com/consulting']
    TMP=['partner_program_link','partner_directory','partner_program_name','partner_company_name','product/service_name','company_domain_name','partner_type','partner_tier','company_description','headquarters_street','headquarters_city','headquarters_state','headquarters_zipcode','headquarters_country','locations_street','locations_city','locations_state','locations_zipcode','locations_country','regions','languages','services','solutions','product','pricing','specializations','categories','year_founded','general_phone_number','general_email_address','linkedin_link','twitter_link','facebook_link','primary_contact_name','competencies','partner_programs','validations','certifications','certified_experts','designations','industries','partner_clients','notes']
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0','Accept': '*/*','Accept-Language': 'en-GB,en;q=0.5','Accept-Encoding': 'gzip, deflate, br','Referer': 'https://appexchange.salesforce.com/appxStore','Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8','Origin': 'https://appexchange.salesforce.com','Connection': 'keep-alive','Sec-Fetch-Dest': 'empty','Sec-Fetch-Mode': 'cors','Sec-Fetch-Site': 'same-origin',}
    IDS=[]
    def parse(self, response):
        Data=response.xpath('//li/a[@data-listing-id]/@href').getall()
        # Goto get content
        for link in Data:
            if link not in self.IDS:
                self.IDS.append(link)
                #print(link)
                yield scrapy.Request(link,callback=self.parse_product,dont_filter=True)
        #print('=>',len(Data),len(self.IDS),'Page:',1)
        BUTTON=response.xpath('//button[contains(@id,"appx-load-more-button-")]/@onclick').getall()
        if len(BUTTON)>0 and len(Data)>=10:
            #print('BUTTON:',BUTTON)
            for button in BUTTON:
                if 'ListingsJS()' in str(button):
                    acc=str(button).split('ListingsJS()')[0]
                else:
                    acc='Level'+str(button).split('Level')[1].split('(')[0]
                control=response.xpath('//script[@id and contains(text(),"'+acc+'")]/@id').get()
                #print(control)
                RUN=True
                page=1
                while RUN:
                    if page==1:
                        page_data = {'AJAXREQUEST': '_viewRoot','j_id0:AppxLayout:actionsForm': 'j_id0:AppxLayout:actionsForm','com.salesforce.visualforce.ViewState': str(response.xpath('//input[@id="com.salesforce.visualforce.ViewState"]/@value').get()),'com.salesforce.visualforce.ViewStateVersion': str(response.xpath('//input[@id="com.salesforce.visualforce.ViewStateVersion"]/@value').get()),'com.salesforce.visualforce.ViewStateMAC': str(response.xpath('//input[@id="com.salesforce.visualforce.ViewStateMAC"]/@value').get()),str(control): str(control)}
                    else:
                        page_data = {'AJAXREQUEST': '_viewRoot','j_id0:AppxLayout:actionsForm': 'j_id0:AppxLayout:actionsForm','com.salesforce.visualforce.ViewState': str(response_requests.xpath('//input[@id="com.salesforce.visualforce.ViewState"]/@value').get()),'com.salesforce.visualforce.ViewStateVersion': str(response_requests.xpath('//input[@id="com.salesforce.visualforce.ViewStateVersion"]/@value').get()),'com.salesforce.visualforce.ViewStateMAC': str(response_requests.xpath('//input[@id="com.salesforce.visualforce.ViewStateMAC"]/@value').get()),str(control): str(control)}
                    page+=1
                    response_requests = requests.post('https://appexchange.salesforce.com/appxStore', headers=self.headers, data=page_data)
                    response_requests=scrapy.Selector(text=response_requests.text)
                    page_Data=response_requests.xpath('//li/a[@data-listing-id]/@href').getall()
                    for link in page_Data:
                        if link not in self.IDS:
                            self.IDS.append(link)
                            #print(link)
                            yield scrapy.Request(link,callback=self.parse_product,dont_filter=True)
                    #print('=>',len(Data),len(self.IDS),'Page:',page)
                    if len(page_Data)<10:
                        RUN=False                    

    def parse_product(self,response):
        item={}
        for k in self.TMP:
            item[k]=''
        NAME=response.xpath('//h1[@class="appx-page-header-2_title"]/text()').get()
        item['product/service_name']=str(NAME).strip()
        PRICE_TXT=response.xpath('//p[@class="appx-pricing-detail-header"]/span[contains(@id,"planCharges")]/text()').get()
        PRICE = Price.fromstring(PRICE_TXT)
        if not PRICE.amount is None:
            item['pricing']=(str(PRICE.currency)+str(PRICE.amount)).replace('USD','$')
        item['company_description']=str(response.xpath('//div[contains(@class,"appx-extended-detail-description")]/text()').get()).strip()
        STATE1=response.xpath('//div[contains(@class,"appx-detail-subsection-values-columns")]//span[@class="appx-tooltip-text"]/text()').getall()
        STATE1_ALL=[]
        for rs in STATE1:
            rs=str(rs).split(', ')
            for rs1 in rs:
                STATE1_ALL.append(str(rs1).strip())
        item['locations_state']=STATE1_ALL
        COUNTRIES=response.xpath('//div[contains(@class,"appx-detail-subsection-values-columns")]/div')
        COUNTRIES_LIST={}
        for ros in COUNTRIES:
            TITLE=ros.xpath('.//span[@class="appx-detail-subsection-values-title"]/text()').get()
            VALUES=ros.xpath('.//span[@class="appx-tooltip-text"]/text()').get()
            if VALUES:
                COUNTRY=[]
                COUNTRYS=str(VALUES).split(', ')
                for rs in COUNTRYS:
                    rs=str(rs).strip()
                    if rs!='':
                        COUNTRY.append(rs)
                COUNTRIES_LIST[TITLE]=COUNTRY
            else:
                COUNTRIES_LIST[TITLE]=[]
        item['locations_country']=COUNTRIES_LIST
        DATA=response.xpath('//div[@class="appx-extended-detail-subsection"]')
        LANG=[]
        for ROW in DATA:
            TITLE=ROW.xpath('./p[@class="slds-section__title appx-section__title"]/text()').get()
            if 'Languages' in TITLE:
                LANG=ROW.xpath('./div[@class="appx-extended-detail-subsection-label-description"]/span/text()').getall()
        for i in range(len(LANG)):
            lang=str(LANG[i]).strip()
            if len(lang)>3:
                item['languages']+=lang
        DATA=response.xpath('//ul[@class="appx-summary-bar_facts-ul"]/li')
        for ROW in DATA:
            TITLE=ROW.xpath('./span[@class="appx-summary-bar_facts-label"]/text()').get()
            VALUE=str(ROW.xpath('./span[@class="appx-summary-bar_facts-value"]/text()').get()).strip()
            if 'Founded' in TITLE:
                item['year_founded']=VALUE
            if 'Certified Experts' in TITLE:
                item['certified_experts']=VALUE
        self.headers['Referer']=response.url
        page_data = {'AJAXREQUEST': 'AppxConsultingListingDetail:AppxLayout:j_id1453:j_id1454','AppxConsultingListingDetail:AppxLayout:j_id1453': 'AppxConsultingListingDetail:AppxLayout:j_id1453','com.salesforce.visualforce.ViewState': str(response.xpath('//input[@id="com.salesforce.visualforce.ViewState"]/@value').get()),'com.salesforce.visualforce.ViewStateVersion': str(response.xpath('//input[@id="com.salesforce.visualforce.ViewStateVersion"]/@value').get()),'com.salesforce.visualforce.ViewStateMAC': str(response.xpath('//input[@id="com.salesforce.visualforce.ViewStateMAC"]/@value').get()),'AppxConsultingListingDetail:AppxLayout:j_id1453:j_id1456': 'AppxConsultingListingDetail:AppxLayout:j_id1453:j_id1456'}
        item['partner_program_link']=response.url
        yield scrapy.FormRequest('https://appexchange.salesforce.com/appxConsultingListingDetail',callback=self.parse_next,headers=self.headers,formdata=page_data,meta={'item':item})
    def parse_next(self,response):
        item=response.meta['item']
        DATA_JSON_STR=str(str(response.text).split("JSON.parse('")[1].split("'),")[0]).replace('\\','')
        DATA_JSON={}
        try:
            DATA_JSON=json.loads(DATA_JSON_STR)
            #print('1: ',DATA_JSON)
        except:
            try:
                DATA_JSON=eval(DATA_JSON_STR)
            except:
                pass
            #print('2: ',DATA_JSON)
        DT=[]
        for rcs in DATA_JSON:
            for i in range(len(DATA_JSON[rcs])):
                #print(DATA_JSON[rcs][i])
                DT.append(DATA_JSON[rcs][i]['SpecialtyName__c'])
        item['specializations']=', '.join(DT)
        Data=response.xpath('//ul[@id="appx_accordion_industry"]/li/@psa-title').getall()
        item['industries']='; '.join(Data)
        Data=response.xpath('//ul[@id="appx_accordion_certifications"]//span[@class="appx-accordion__content-title"]/text()').getall()
        item['certifications']='; '.join(Data)
        url='https://appexchange.salesforce.com/AppxListingDetailOverviewTab?listingId='+str(item['partner_program_link']).split('listingId=')[1].split('&')[0]
        #item['URL']=url
        yield scrapy.Request(url,callback=self.parse_content,meta={'item':item})
    def parse_content(self,response):
        item=response.meta['item']
        partner_company_name=response.xpath('//div[@class="appx-company-name"]/text()').get() or response.xpath('//div[@class="appx-extended-detail-section-consultant-secondary"]/div[contains(@id,"AppxListingDetailOverviewTab")]//p[@class="slds-section__title appx-section__title"]/text()').get()
        if partner_company_name:
            item['partner_company_name']=str(partner_company_name).strip()
            if str(item['partner_company_name']).startswith('About '):
                item['partner_company_name']=str(item['partner_company_name']).split('About ')[1]
        company_domain_name=response.xpath('//a[@data-event="listing-publisher-website"]/@href').get()
        if company_domain_name:
            item['company_domain_name']=str(company_domain_name).strip()
            if '//' in item['company_domain_name'] or 'www.' in item['company_domain_name']:
                DOMAIN=re.split('www.|//',item['company_domain_name'])
                item['company_domain_name']=DOMAIN[len(DOMAIN)-1].split('/')[0]
                if 'salesforce.com' in item['company_domain_name']:
                    item['company_domain_name']='salesforce.com'
        ADD=response.xpath('//div[@class="appx-extended-detail-subsection-segment "]//div[contains(@id,"AppxListingDetailOverviewTab")]/text()').get()
        ADD=str(ADD).split(',')
        if len(ADD)>0:
            item['headquarters_city']=ADD[0]
            if len(ADD)>1:
                item['headquarters_state']=ADD[1]
                if len(ADD)>2:
                    item['headquarters_country']=ADD[2]
        
        Data=response.xpath('//div[@class="appx-extended-detail-subsection-label-description"]')
        IT={}
        for row in Data:
            TITLE=row.xpath('./div[@class="appx-extended-detail-subsection-label"]/text()').get()
            TXT=row.xpath('./div[contains(@class,"appx-extended-detail-subsection-description")]/a/text()').get() or row.xpath('./div[contains(@class,"appx-extended-detail-subsection-description")]/text()').get()
            if TITLE:
                IT[str(TITLE).strip()]=str(TXT).strip()
        if item['year_founded']=='' and 'Founded' in IT:
            item['year_founded']=IT['Founded']
        if 'Phone' in IT:
            item['general_phone_number']=IT['Phone']
        if 'Email' in IT:
            item['general_email_address']=IT['Email']
        if 'Headquarters' in IT:
            ADD=str(IT['Headquarters']).split(',')
            for j in range(len(ADD)):
                ADD[j]=str(ADD[j]).strip()
            STATE=0
            for j in range(len(ADD)):
                if len(ADD[j])==2 and ADD[j][0] in string.ascii_uppercase and ADD[j][1] in string.ascii_uppercase and STATE==0:
                    STATE=j
            if STATE>0:
                item['headquarters_state']=ADD[STATE]
                if STATE+1<len(ADD) and ADD[STATE+1]==Get_Number(ADD[STATE+1]):
                    item['headquarters_zipcode']=ADD[STATE+1]
                item['headquarters_city']=ADD[STATE-1]
                Add=[]
                for j in range(STATE):
                    Add.append(ADD[j])
                item['headquarters_street']=', '.join(Add)
            else:
                item['headquarters_street']=IT['Headquarters']
        #industries=response.xpath('//div[@class="appx-detail-section-industries"]/a/span/text()').getall()
        #item['industries']=', '.join(industries)
        if str(item['product/service_name']).strip()!='':
            yield(item)
        else:
            print('\n ------------')
            print(response.url)