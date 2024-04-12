### Add PostGreSQL Database:
## *Need PostGreSQL server installed to save to the Database in addition to the CSV files.

- First Please refer / update the Database connection configuration in the pipelines.py file (user, password, host, port).
- The code will create a Database called partners and a partner table with the fields (id, name, partner_type, raw_url, url, tagline, description, phone, industries, services, solutions, qualifications, country, hq_Location and source).
- process_item function in pipelines.py file been updated to insert new item to partner table in partners PostgreSQL Database.

### SAP partners scraping

Web Site: https://pf-prod-sapit-partner-prod.cfapps.eu10.hana.ondemand.com/partnerNavigator?products=Application%20Development%20and%20Integration&products=Analytics&products=Intelligent%20Technologies&products=Database%20and%20Data%20Management

Sample:  partner detail link: https://pf-prod-sapit-partner-prod.cfapps.eu10.hana.ondemand.com/profile/0002573648

Run spider:
1- Install prerequisite libraries from requirements.txt file
2- Go to project location (C:\........\techpartners)
3- using shell_command run below command
	scrapy crawl sappartner

Output:
Expected output table
location: ouput/sappartner_YYYY-MM-DD_HH.MM.SS.csv

### Scraper
Will scrape all search result link partners and save its items scraped in csv and partner DB table.

-------------------------------------------------------------------------------------------------------------------------------------------------------------

### AWS partners scraping

Web Site: https://partners.amazonaws.com/search/partners?facets=Use%20Case%20%3A%20Security

Run spider:
1- Install prerequisite libraries from requirements.txt file
2- Go to project location (C:\........\techpartners)
3- using shell_command run below command
	scrapy crawl awspartner

Output:
Expected output table
location: ouput/awspartner_YYYY-MM-DD_HH.MM.SS.csv

### Scraper
Will scrape all search result partners and save its items scraped in csv and partner DB table.

-------------------------------------------------------------------------------------------------------------------------------------------------------------

### Cynet partners scraping

Web Site: https://www.cynet.com/partners/find-a-partner/

Run spider:
1- Install prerequisite libraries from requirements.txt file
2- Go to project location (C:\........\techpartners)
3- using shell_command run below command
	scrapy crawl cynetpartner

Output:
Expected output table
location: ouput/cynetpartner_YYYY-MM-DD_HH.MM.SS.csv

### Scraper
Will scrape all search result partners and save its items scraped in csv and partner DB table.

-------------------------------------------------------------------------------------------------------------------------------------------------------------

### Adobe partners scraping

Web Site: https://solutionpartners.adobe.com/s/directory/

Run spider:
1- Install prerequisite libraries from requirements.txt file
2- Go to project location (C:\........\techpartners)
3- using shell_command run below command
	scrapy crawl adobepartner

Output:
Expected output table
location: ouput/adobepartner_YYYY-MM-DD_HH.MM.SS.csv

### Scraper
Will scrape all search result partners and save its items scraped in csv and partner DB table.

-------------------------------------------------------------------------------------------------------------------------------------------------------------

### Oracle partners scraping

Web Site: https://partner-finder.oracle.com/catalog/

Run spider:
1- Install prerequisite libraries from requirements.txt file
2- Go to project location (C:\........\techpartners)
3- using shell_command run below command
	scrapy crawl oraclepartner

Output:
Expected output table
location: ouput/oraclepartner_YYYY-MM-DD_HH.MM.SS.csv

### Scraper
Will scrape all search result partners and save its items scraped in csv.

-------------------------------------------------------------------------------------------------------------------------------------------------------------

### NetSuite partners scraping

Web Site: https://www.netsuite.com/portal/partners/alliance-partner-program.shtml

Run spider:
1- Install prerequisite libraries from requirements.txt file
2- Go to project location (C:\........\techpartners)
3- using shell_command run below command
	scrapy crawl netsuitepartner

Output:
Expected output table
location: ouput/netsuitepartner_YYYY-MM-DD_HH.MM.SS.csv

### Scraper
Will scrape all search result partners and save its items scraped in csv.

-------------------------------------------------------------------------------------------------------------------------------------------------------------

### Zoho partners scraping

Web Site: https://www.zoho.com/partners/find-partner-results.html

Run spider:
1- Install prerequisite libraries from requirements.txt file
2- Go to project location (C:\........\techpartners)
3- using shell_command run below command
	scrapy crawl zohopartner

Output:
Expected output table
location: ouput/zohopartner_YYYY-MM-DD_HH.MM.SS.csv

### Scraper
Will scrape all search result partners and save its items scraped in csv.

-------------------------------------------------------------------------------------------------------------------------------------------------------------

### Drift partners scraping

Web Site: https://www.drift.com/partners/partner-directory/

Run spider:
1- Install prerequisite libraries from requirements.txt file
2- Go to project location (C:\........\techpartners)
3- using shell_command run below command
	scrapy crawl driftpartner

Output:
Expected output table
location: ouput/driftpartner_YYYY-MM-DD_HH.MM.SS.csv

### Scraper
Will scrape all search result partners and save its items scraped in csv.

-------------------------------------------------------------------------------------------------------------------------------------------------------------

### Zendesk partners scraping

Web Site: http://www.zendesk.com/marketplace/partners/

Run spider:
1- Install prerequisite libraries from requirements.txt file
2- Go to project location (C:\........\techpartners)
3- using shell_command run below command
	scrapy crawl zendeskpartner

Output:
Expected output table
location: ouput/zendeskpartner_YYYY-MM-DD_HH.MM.SS.csv

### Scraper
Will scrape all search result partners and save its items scraped in csv.

-------------------------------------------------------------------------------------------------------------------------------------------------------------

### Microsoft partners scraping

Web Site: https://appsource.microsoft.com/en-us/marketplace/partner-dir?filter=sort%3D0%3BpageSize%3D18%3BonlyThisCountry%3Dtrue%3Bcountry%3DUS%3Blocname%3DUnited%2520States

As Microsoft API only return maximum 100 results, so created different search criteria to obtain almost all partners available.
created 2 spider based on different search criteria as follows:
- microsoftpartner1
Iterate through all US lat and lng locations lat from 25 to 50 and lng from -75 to -125
  each filter result by Focus industry and requesting first 5 pages each contain 20 partners.
- microsoftpartner2
Given Focus to given US cities locations with lat and lng, Iterate through all these locations
  and filter results by product, Focus industry, category and requesting first 5 pages each contain 20 partners.
Run spider:
1- Install prerequisite libraries from requirements.txt file
2- Go to project location (C:\........\techpartners)
3- using shell_command run below command
	scrapy crawl microsoftpartner1
	scrapy crawl microsoftpartner2

Output:
Expected output table
location: ouput/microsoftpartner#_YYYY-MM-DD_HH.MM.SS.csv

### Scraper
Will scrape all search result partners and save its items scraped in csv.

-------------------------------------------------------------------------------------------------------------------------------------------------------------

### Nutanix partners scraping

Web Site: https://www.nutanix.com/partners/find-a-partner

Run spider:
1- Install prerequisite libraries from requirements.txt file
2- Go to project location (C:\........\techpartners)
3- using shell_command run below command
	scrapy crawl nutanixpartner

Output:
Expected output table
location: ouput/nutanixpartner_YYYY-MM-DD_HH.MM.SS.csv

### Scraper
Will scrape all search result partners and save its items scraped in csv.

-------------------------------------------------------------------------------------------------------------------------------------------------------------

### Klipfolio partners scraping

Web Site: https://www.klipfolio.com/partners/directory

Run spider:
1- Install prerequisite libraries from requirements.txt file
2- Go to project location (C:\........\techpartners)
3- using shell_command run below command
	scrapy crawl klipfoliopartner

Output:
Expected output table
location: ouput/klipfoliopartner_YYYY-MM-DD_HH.MM.SS.csv

### Scraper
Will scrape all search result partners and save its items scraped in csv.

-------------------------------------------------------------------------------------------------------------------------------------------------------------

### Salesforce apps scraping

Web Site: https://appexchange.salesforce.com/appxStore

Run spider:
1- Install prerequisite libraries from requirements.txt file
2- Go to project location (C:\........\techpartners)
3- using shell_command run below command
	scrapy crawl salesforce

Output:
Expected output table
location: ouput/salesforce_YYYY-MM-DD_HH.MM.SS.csv

### Scraper
Will scrape all search result partners and save its items scraped in csv.

-------------------------------------------------------------------------------------------------------------------------------------------------------------

### Salesforce consultants scraping

Web Site: https://appexchange.salesforce.com/consulting

Run spider:
1- Install prerequisite libraries from requirements.txt file
2- Go to project location (C:\........\techpartners)
3- using shell_command run below command
	scrapy crawl salesforce_consulting

Output:
Expected output table
location: ouput/salesforce_consulting_YYYY-MM-DD_HH.MM.SS.csv

### Scraper
Will scrape all search result partners and save its items scraped in csv.

-------------------------------------------------------------------------------------------------------------------------------------------------------------
