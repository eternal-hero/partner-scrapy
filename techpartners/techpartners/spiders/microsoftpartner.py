# import needed libraries
import time
import scrapy
from techpartners.spiders.base_spider import BaseSpider
from techpartners.functions import *
import urllib.parse


class Spider(BaseSpider):
    # spider name; used for calling spider
    name = 'microsoftpartner'
    partner_program_link = 'https://appsource.microsoft.com/en-us/marketplace/'
    partner_directory = 'Microsoft AppSource Partners'
    partner_program_name = ''
    crawl_id = 1262

    api_link = "https://main.prod.marketplacepartnerdirectory.azure.com/api/partners/"

    custom_settings = {
        # 'DOWNLOAD_DELAY': 0.5,
        # 'CLOSESPIDER_ITEMCOUNT': 10,        # For testing purposes ****************************

        'CONCURRENT_REQUESTS': 8,
        'CONCURRENT_REQUESTS_PER_IP': 1,
        'DOWNLOADER_MIDDLEWARES': {
        'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
        'rotating_proxies.middlewares.BanDetectionMiddleware': 620},
        'ROTATING_PROXY_LIST_PATH': 'proxy_25000.txt',
        'ROTATING_PROXY_PAGE_RETRY_TIMES': 5,
    }

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

    done_ids = set()

    cities =   [{'location': 'New York, New York', 'lat': 40.7127281, 'lng': -74.0060152},
                {'location': 'Los Angeles, California', 'lat': 34.0536909, 'lng': -118.242766},
                {'location': 'Chicago, Illinois', 'lat': 41.8755616, 'lng': -87.6244212},
                {'location': 'Houston, Texas', 'lat': 29.7589382, 'lng': -95.3676974},
                {'location': 'Phoenix, Arizona', 'lat': 33.4484367, 'lng': -112.0741417},
                {'location': 'Philadelphia, Pennsylvania', 'lat': 39.9527237, 'lng': -75.1635262},
                {'location': 'San Antonio, Texas', 'lat': 29.4246002, 'lng': -98.4951405},
                {'location': 'San Diego, California', 'lat': 32.7174202, 'lng': -117.1627728},
                {'location': 'Dallas, Texas', 'lat': 32.7762719, 'lng': -96.7968559},
                {'location': 'San Jose, California', 'lat': 37.3361905, 'lng': -121.890583},
                {'location': 'Austin, Texas', 'lat': 30.2711286, 'lng': -97.7436995},
                {'location': 'Jacksonville, Florida', 'lat': 30.3321838, 'lng': -81.655651},
                {'location': 'Fort Worth, Texas', 'lat': 32.753177, 'lng': -97.3327459},
                {'location': 'Columbus, Ohio', 'lat': 39.9622601, 'lng': -83.0007065},
                {'location': 'Indianapolis, Indiana', 'lat': 39.7683331, 'lng': -86.1583502},
                {'location': 'Charlotte, North Carolina', 'lat': 35.2272086, 'lng': -80.8430827},
                {'location': 'San Francisco, California', 'lat': 37.7790262, 'lng': -122.419906},
                {'location': 'Seattle, Washington', 'lat': 47.6038321, 'lng': -122.3300624},
                {'location': 'Denver, Colorado', 'lat': 39.7392364, 'lng': -104.9848623},
                {'location': 'Washington, District of Columbia', 'lat': 38.8950368, 'lng': -77.0365427},
                {'location': 'Nashville, Tennessee', 'lat': 36.1622767, 'lng': -86.7742984},
                {'location': 'Oklahoma City, Oklahoma', 'lat': 35.4729886, 'lng': -97.5170536},
                {'location': 'El Paso, Texas', 'lat': 31.7754152, 'lng': -106.464634},
                {'location': 'Boston, Massachusetts', 'lat': 42.3602534, 'lng': -71.0582912},
                {'location': 'Portland, Oregon', 'lat': 45.5202471, 'lng': -122.6741949},
                {'location': 'Las Vegas, Nevada', 'lat': 36.1672559, 'lng': -115.148516},
                {'location': 'Detroit, Michigan', 'lat': 42.3315509, 'lng': -83.0466403},
                {'location': 'Memphis, Tennessee', 'lat': 35.1490215, 'lng': -90.0516285},
                {'location': 'Louisville, Kentucky', 'lat': 38.2542376, 'lng': -85.759407},
                {'location': 'Baltimore, Maryland', 'lat': 39.2908816, 'lng': -76.610759},
                {'location': 'Milwaukee, Wisconsin', 'lat': 43.0349931, 'lng': -87.922497},
                {'location': 'Albuquerque, New Mexico', 'lat': 35.212870949999996, 'lng': -106.71324849574629},
                {'location': 'Tucson, Arizona', 'lat': 32.2228765, 'lng': -110.9748477},
                {'location': 'Fresno, California', 'lat': 36.7394421, 'lng': -119.7848307},
                {'location': 'Sacramento, California', 'lat': 38.5810606, 'lng': -121.493895},
                {'location': 'Kansas City, Missouri', 'lat': 39.100105, 'lng': -94.5781416},
                {'location': 'Mesa, Arizona', 'lat': 33.4151117, 'lng': -111.8314792},
                {'location': 'Atlanta, Georgia', 'lat': 33.7489924, 'lng': -84.3902644},
                {'location': 'Omaha, Nebraska', 'lat': 41.2587459, 'lng': -95.9383758},
                {'location': 'Colorado Springs, Colorado', 'lat': 38.8339578, 'lng': -104.8253485},
                {'location': 'Raleigh, North Carolina', 'lat': 35.7803977, 'lng': -78.6390989},
                {'location': 'Long Beach, California', 'lat': 33.7690164, 'lng': -118.191604},
                {'location': 'Virginia Beach, Virginia', 'lat': 36.8529841, 'lng': -75.9774183},
                {'location': 'Miami, Florida', 'lat': 25.7741728, 'lng': -80.19362},
                {'location': 'Oakland, California', 'lat': 37.8044557, 'lng': -122.2713563},
                {'location': 'Minneapolis, Minnesota', 'lat': 44.9772995, 'lng': -93.2654692},
                {'location': 'Tulsa, Oklahoma', 'lat': 36.1556805, 'lng': -95.9929113},
                {'location': 'Bakersfield, California', 'lat': 35.3738712, 'lng': -119.0194639},
                {'location': 'Wichita, Kansas', 'lat': 37.6922361, 'lng': -97.3375448},
                {'location': 'Arlington, Texas', 'lat': 32.701938999999996, 'lng': -97.10562379033699},
                {'location': 'Aurora, Colorado', 'lat': 39.7405111, 'lng': -104.8309947},
                {'location': 'Tampa, Florida', 'lat': 27.9477595, 'lng': -82.458444},
                {'location': 'New Orleans, Louisiana', 'lat': 29.9499323, 'lng': -90.0701156},
                {'location': 'Cleveland, Ohio', 'lat': 41.5051613, 'lng': -81.6934446},
                {'location': 'Honolulu, Hawaii', 'lat': 21.304547, 'lng': -157.855676},
                {'location': 'Anaheim, California', 'lat': 33.8347516, 'lng': -117.911732},
                {'location': 'Lexington, Kentucky', 'lat': 38.0464066, 'lng': -84.4970393},
                {'location': 'Stockton, California', 'lat': 37.9577016, 'lng': -121.2907796},
                {'location': 'Corpus Christi, Texas', 'lat': 27.7951482, 'lng': -97.3942053},
                {'location': 'Henderson, Nevada', 'lat': 36.0301134, 'lng': -114.9826194},
                {'location': 'Riverside, California', 'lat': 33.9533546, 'lng': -117.3961623},
                {'location': 'Newark, New Jersey', 'lat': 40.735657, 'lng': -74.1723667},
                {'location': 'Saint Paul, Minnesota', 'lat': 44.9497487, 'lng': -93.0931028},
                {'location': 'Santa Ana, California', 'lat': 33.7494951, 'lng': -117.8732213},
                {'location': 'Cincinnati, Ohio', 'lat': 39.1014537, 'lng': -84.5124602},
                {'location': 'Irvine, California', 'lat': 33.6856969, 'lng': -117.8259819},
                {'location': 'Orlando, Florida', 'lat': 28.5421109, 'lng': -81.3790304},
                {'location': 'Pittsburgh, Pennsylvania', 'lat': 40.4416941, 'lng': -79.9900861},
                {'location': 'St. Louis, Missouri', 'lat': 38.6529545, 'lng': -90.24111656024635},
                {'location': 'Greensboro, North Carolina', 'lat': 36.0726355, 'lng': -79.7919754},
                {'location': 'Jersey City, New Jersey', 'lat': 40.7215682, 'lng': -74.047455},
                {'location': 'Anchorage, Alaska', 'lat': 61.2163129, 'lng': -149.894852},
                {'location': 'Lincoln, Nebraska', 'lat': 40.8088861, 'lng': -96.7077751},
                {'location': 'Plano, Texas', 'lat': 33.0136764, 'lng': -96.6925096},
                {'location': 'Durham, North Carolina', 'lat': 35.996653, 'lng': -78.9018053},
                {'location': 'Buffalo, New York', 'lat': 42.8867166, 'lng': -78.8783922},
                {'location': 'Chandler, Arizona', 'lat': 33.3061605, 'lng': -111.84125},
                {'location': 'Chula Vista, California', 'lat': 32.6400541, 'lng': -117.0841955},
                {'location': 'Toledo, Ohio', 'lat': 41.6529143, 'lng': -83.5378173},
                {'location': 'Madison, Wisconsin', 'lat': 43.074761, 'lng': -89.3837613},
                {'location': 'Gilbert, Arizona', 'lat': 33.3527632, 'lng': -111.7890373},
                {'location': 'Reno, Nevada', 'lat': 39.5261206, 'lng': -119.8126581},
                {'location': 'Fort Wayne, Indiana', 'lat': 41.0799898, 'lng': -85.1386015},
                {'location': 'North Las Vegas, Nevada', 'lat': 36.2005843, 'lng': -115.121584},
                {'location': 'St. Petersburg, Florida', 'lat': 27.7703796, 'lng': -82.6695085},
                {'location': 'Lubbock, Texas', 'lat': 33.5635206, 'lng': -101.879336},
                {'location': 'Irving, Texas', 'lat': 32.8295183, 'lng': -96.9442177},
                {'location': 'Laredo, Texas', 'lat': 27.5236998, 'lng': -99.497352},
                {'location': 'Winston–Salem, North Carolina', 'lat': 36.0998131, 'lng': -80.2440518},
                {'location': 'Chesapeake, Virginia', 'lat': 36.7183708, 'lng': -76.2466798},
                {'location': 'Glendale, Arizona', 'lat': 33.5386858, 'lng': -112.1859941},
                {'location': 'Garland, Texas', 'lat': 32.912624, 'lng': -96.6388833},
                {'location': 'Scottsdale, Arizona', 'lat': 33.4942189, 'lng': -111.9260184},
                {'location': 'Norfolk, Virginia', 'lat': 36.8968052, 'lng': -76.2602336},
                {'location': 'Boise, Idaho', 'lat': 43.6166163, 'lng': -116.200886},
                {'location': 'Fremont, California', 'lat': 37.5482697, 'lng': -121.988571},
                {'location': 'Spokane, Washington', 'lat': 47.6571934, 'lng': -117.4235106},
                {'location': 'Santa Clarita, California', 'lat': 34.3916641, 'lng': -118.542586},
                {'location': 'Baton Rouge, Louisiana', 'lat': 30.4459596, 'lng': -91.18738},
                {'location': 'Richmond, Virginia', 'lat': 37.5385087, 'lng': -77.43428},
                {'location': 'Hialeah, Florida', 'lat': 25.8670439, 'lng': -80.29146452058569},
                {'location': 'San Bernardino, California', 'lat': 34.8253019, 'lng': -116.0833144},
                {'location': 'Tacoma, Washington', 'lat': 47.2495798, 'lng': -122.4398746},
                {'location': 'Modesto, California', 'lat': 37.6390972, 'lng': -120.9968782},
                {'location': 'Huntsville, Alabama', 'lat': 34.729847, 'lng': -86.5859011},
                {'location': 'Des Moines, Iowa', 'lat': 41.5910323, 'lng': -93.6046655},
                {'location': 'Yonkers, New York', 'lat': 40.9312099, 'lng': -73.8987469},
                {'location': 'Rochester, New York', 'lat': 43.157285, 'lng': -77.615214},
                {'location': 'Moreno Valley, California', 'lat': 33.937517, 'lng': -117.2305944},
                {'location': 'Fayetteville, North Carolina', 'lat': 35.0525759, 'lng': -78.878292},
                {'location': 'Fontana, California', 'lat': 34.0922335, 'lng': -117.435048},
                {'location': 'Columbus, Georgia', 'lat': 32.4610708, 'lng': -84.9880449},
                {'location': 'Worcester, Massachusetts', 'lat': 42.2625621, 'lng': -71.8018877},
                {'location': 'Port St. Lucie, Florida', 'lat': 27.2939333, 'lng': -80.3503283},
                {'location': 'Little Rock, Arkansas', 'lat': 34.7464809, 'lng': -92.2895948},
                {'location': 'Augusta, Georgia', 'lat': 33.4709714, 'lng': -81.9748429},
                {'location': 'Oxnard, California', 'lat': 34.1976308, 'lng': -119.1803818},
                {'location': 'Birmingham, Alabama', 'lat': 33.5206824, 'lng': -86.8024326},
                {'location': 'Montgomery, Alabama', 'lat': 32.3669656, 'lng': -86.3006485},
                {'location': 'Frisco, Texas', 'lat': 33.1506744, 'lng': -96.8236116},
                {'location': 'Amarillo, Texas', 'lat': 35.2072185, 'lng': -101.8338246},
                {'location': 'Salt Lake City, Utah', 'lat': 40.7596198, 'lng': -111.8867975},
                {'location': 'Grand Rapids, Michigan', 'lat': 42.9632405, 'lng': -85.6678639},
                {'location': 'Huntington Beach, California', 'lat': 33.6783336, 'lng': -118.0000166},
                {'location': 'Overland Park, Kansas', 'lat': 38.9742502, 'lng': -94.6851702},
                {'location': 'Glendale, California', 'lat': 34.1469416, 'lng': -118.2478471},
                {'location': 'Tallahassee, Florida', 'lat': 30.4380832, 'lng': -84.2809332},
                {'location': 'Grand Prairie, Texas', 'lat': 32.7459645, 'lng': -96.9977846},
                {'location': 'McKinney, Texas', 'lat': 33.1976496, 'lng': -96.6154471},
                {'location': 'Cape Coral, Florida', 'lat': 26.6059432, 'lng': -81.9806771},
                {'location': 'Sioux Falls, South Dakota', 'lat': 43.5460587, 'lng': -96.7313928},
                {'location': 'Peoria, Arizona', 'lat': 33.5806115, 'lng': -112.237294},
                {'location': 'Providence, Rhode Island', 'lat': 41.8239891, 'lng': -71.4128343},
                {'location': 'Vancouver, Washington', 'lat': 45.6306954, 'lng': -122.6744557},
                {'location': 'Knoxville, Tennessee', 'lat': 35.9603948, 'lng': -83.9210261},
                {'location': 'Akron, Ohio', 'lat': 41.083064, 'lng': -81.518485},
                {'location': 'Shreveport, Louisiana', 'lat': 32.5221828, 'lng': -93.7651944},
                {'location': 'Mobile, Alabama', 'lat': 30.6943566, 'lng': -88.0430541},
                {'location': 'Brownsville, Texas', 'lat': 25.9140256, 'lng': -97.4890856},
                {'location': 'Newport News, Virginia', 'lat': 36.9775016, 'lng': -76.42977},
                {'location': 'Fort Lauderdale, Florida', 'lat': 26.1223084, 'lng': -80.1433786},
                {'location': 'Chattanooga, Tennessee', 'lat': 35.0457219, 'lng': -85.3094883},
                {'location': 'Tempe, Arizona', 'lat': 33.4255056, 'lng': -111.9400091},
                {'location': 'Aurora, Illinois', 'lat': 41.7571701, 'lng': -88.3147539},
                {'location': 'Santa Rosa, California', 'lat': 38.4404925, 'lng': -122.7141049},
                {'location': 'Eugene, Oregon', 'lat': 44.0505054, 'lng': -123.0950506},
                {'location': 'Elk Grove, California', 'lat': 38.4087993, 'lng': -121.3716178},
                {'location': 'Salem, Oregon', 'lat': 44.9391565, 'lng': -123.033121},
                {'location': 'Ontario, California', 'lat': 34.065846, 'lng': -117.6484304},
                {'location': 'Cary, North Carolina', 'lat': 35.7882893, 'lng': -78.7812081},
                {'location': 'Rancho Cucamonga, California', 'lat': 34.1279268, 'lng': -117.56432205849399},
                {'location': 'Oceanside, California', 'lat': 33.1958696, 'lng': -117.3794834},
                {'location': 'Lancaster, California', 'lat': 34.6981064, 'lng': -118.1366153},
                {'location': 'Garden Grove, California', 'lat': 33.7746292, 'lng': -117.9463717},
                {'location': 'Pembroke Pines, Florida', 'lat': 26.0031465, 'lng': -80.223937},
                {'location': 'Fort Collins, Colorado', 'lat': 40.5508527, 'lng': -105.0668085},
                {'location': 'Palmdale, California', 'lat': 34.5793131, 'lng': -118.1171108},
                {'location': 'Springfield, Missouri', 'lat': 37.2166779, 'lng': -93.2920373},
                {'location': 'Clarksville, Tennessee', 'lat': 36.5277607, 'lng': -87.3588703},
                {'location': 'Salinas, California', 'lat': 36.6744117, 'lng': -121.6550372},
                {'location': 'Hayward, California', 'lat': 37.6688205, 'lng': -122.080796},
                {'location': 'Paterson, New Jersey', 'lat': 40.9167654, 'lng': -74.171811},
                {'location': 'Alexandria, Virginia', 'lat': 38.8051095, 'lng': -77.0470229},
                {'location': 'Macon, Georgia', 'lat': 32.8406946, 'lng': -83.6324022},
                {'location': 'Corona, California', 'lat': 33.8752945, 'lng': -117.5664449},
                {'location': 'Kansas City, Kansas', 'lat': 39.100105, 'lng': -94.5781416},
                {'location': 'Lakewood, Colorado', 'lat': 39.631108499999996, 'lng': -105.11005818223015},
                {'location': 'Springfield, Massachusetts', 'lat': 42.1018764, 'lng': -72.5886727},
                {'location': 'Sunnyvale, California', 'lat': 37.3688301, 'lng': -122.036349},
                {'location': 'Jackson, Mississippi', 'lat': 32.2990021, 'lng': -90.1847691},
                {'location': 'Killeen, Texas', 'lat': 31.1171441, 'lng': -97.727796},
                {'location': 'Hollywood, Florida', 'lat': 26.0112014, 'lng': -80.1494901},
                {'location': 'Murfreesboro, Tennessee', 'lat': 35.8460396, 'lng': -86.3921096},
                {'location': 'Pasadena, Texas', 'lat': 29.6910625, 'lng': -95.2091006},
                {'location': 'Bellevue, Washington', 'lat': 47.6144219, 'lng': -122.192337},
                {'location': 'Pomona, California', 'lat': 34.0553813, 'lng': -117.7517496},
                {'location': 'Escondido, California', 'lat': 33.1216751, 'lng': -117.0814849},
                {'location': 'Joliet, Illinois', 'lat': 41.5263603, 'lng': -88.0840212},
                {'location': 'Charleston, South Carolina', 'lat': 32.7876012, 'lng': -79.9402728},
                {'location': 'Mesquite, Texas', 'lat': 32.7666103, 'lng': -96.599472},
                {'location': 'Naperville, Illinois', 'lat': 41.7728699, 'lng': -88.1479278},
                {'location': 'Rockford, Illinois', 'lat': 42.2713945, 'lng': -89.093966},
                {'location': 'Bridgeport, Connecticut', 'lat': 41.1670412, 'lng': -73.2048348},
                {'location': 'Syracuse, New York', 'lat': 43.0481221, 'lng': -76.1474244},
                {'location': 'Savannah, Georgia', 'lat': 32.0809263, 'lng': -81.0911768},
                {'location': 'Roseville, California', 'lat': 38.7521235, 'lng': -121.2880059},
                {'location': 'Torrance, California', 'lat': 33.8358492, 'lng': -118.3406288},
                {'location': 'Fullerton, California', 'lat': 33.8739385, 'lng': -117.9243399},
                {'location': 'Surprise, Arizona', 'lat': 33.6292271, 'lng': -112.3680189},
                {'location': 'McAllen, Texas', 'lat': 26.2043691, 'lng': -98.230082},
                {'location': 'Thornton, Colorado', 'lat': 39.8695516, 'lng': -104.985181},
                {'location': 'Visalia, California', 'lat': 36.3302284, 'lng': -119.2920585},
                {'location': 'Olathe, Kansas', 'lat': 38.8838856, 'lng': -94.81887},
                {'location': 'Gainesville, Florida', 'lat': 29.6519684, 'lng': -82.3249846},
                {'location': 'West Valley City, Utah', 'lat': 40.696629, 'lng': -111.9867271},
                {'location': 'Orange, California', 'lat': 33.7500378, 'lng': -117.8704931},
                {'location': 'Denton, Texas', 'lat': 33.1838787, 'lng': -97.1413417},
                {'location': 'Warren, Michigan', 'lat': 42.4932575, 'lng': -83.0062746},
                {'location': 'Pasadena, California', 'lat': 34.1476452, 'lng': -118.1444779},
                {'location': 'Waco, Texas', 'lat': 31.549333, 'lng': -97.1466695},
                {'location': 'Cedar Rapids, Iowa', 'lat': 41.9758872, 'lng': -91.6704053},
                {'location': 'Dayton, Ohio', 'lat': 39.7589478, 'lng': -84.1916069},
                {'location': 'Elizabeth, New Jersey', 'lat': 40.6639916, 'lng': -74.2107006},
                {'location': 'Hampton, Virginia', 'lat': 37.0300969, 'lng': -76.3452057},
                {'location': 'Columbia, South Carolina', 'lat': 38.889744050000004, 'lng': -77.04086075512475},
                {'location': 'Kent, Washington', 'lat': 47.3826903, 'lng': -122.2270272},
                {'location': 'Stamford, Connecticut', 'lat': 41.0534302, 'lng': -73.5387341},
                {'location': 'Lakewood, New Jersey', 'lat': 40.0978929, 'lng': -74.2176435},
                {'location': 'Victorville, California', 'lat': 34.5361067, 'lng': -117.2911565},
                {'location': 'Miramar, Florida', 'lat': 25.9873137, 'lng': -80.2322706},
                {'location': 'Coral Springs, Florida', 'lat': 26.271192, 'lng': -80.2706044},
                {'location': 'Sterling Heights, Michigan', 'lat': 42.5803122, 'lng': -83.0302033},
                {'location': 'New Haven, Connecticut', 'lat': 41.3082138, 'lng': -72.9250518},
                {'location': 'Carrollton, Texas', 'lat': 32.9537349, 'lng': -96.8902816},
                {'location': 'Midland, Texas', 'lat': 32.023939549999994, 'lng': -102.09648921925708},
                {'location': 'Norman, Oklahoma', 'lat': 35.2225717, 'lng': -97.4394816},
                {'location': 'Santa Clara, California', 'lat': 37.2333253, 'lng': -121.6846349},
                {'location': 'Athens, Georgia', 'lat': 33.9597677, 'lng': -83.376398},
                {'location': 'Thousand Oaks, California', 'lat': 34.17142715, 'lng': -118.91058772974445},
                {'location': 'Topeka, Kansas', 'lat': 39.049011, 'lng': -95.677556},
                {'location': 'Simi Valley, California', 'lat': 34.2677404, 'lng': -118.7538071},
                {'location': 'Columbia, Missouri', 'lat': 38.9464035, 'lng': -92.3484631580807},
                {'location': 'Vallejo, California', 'lat': 38.1040864, 'lng': -122.2566367},
                {'location': 'Fargo, North Dakota', 'lat': 46.877229, 'lng': -96.789821},
                {'location': 'Allentown, Pennsylvania', 'lat': 40.6022059, 'lng': -75.4712794},
                {'location': 'Pearland, Texas', 'lat': 29.5639758, 'lng': -95.2864299},
                {'location': 'Concord, California', 'lat': 37.9768525, 'lng': -122.0335624},
                {'location': 'Abilene, Texas', 'lat': 32.44645, 'lng': -99.7475905},
                {'location': 'Arvada, Colorado', 'lat': 39.8005505, 'lng': -105.0811573},
                {'location': 'Berkeley, California', 'lat': 37.8753497, 'lng': -122.23963364918777},
                {'location': 'Ann Arbor, Michigan', 'lat': 42.2681569, 'lng': -83.7312291},
                {'location': 'Independence, Missouri', 'lat': 39.0924792, 'lng': -94.4137923},
                {'location': 'Rochester, Minnesota', 'lat': 44.0234387, 'lng': -92.4630182},
                {'location': 'Lafayette, Louisiana', 'lat': 30.2240897, 'lng': -92.0198427},
                {'location': 'Hartford, Connecticut', 'lat': 41.764582, 'lng': -72.6908547},
                {'location': 'College Station, Texas', 'lat': 30.5955289, 'lng': -96.3071042},
                {'location': 'Clovis, California', 'lat': 36.8252277, 'lng': -119.7029194},
                {'location': 'Fairfield, California', 'lat': 38.2493581, 'lng': -122.039966},
                {'location': 'Palm Bay, Florida', 'lat': 27.9946969, 'lng': -80.6366144},
                {'location': 'Richardson, Texas', 'lat': 32.9481789, 'lng': -96.7297206},
                {'location': 'Round Rock, Texas', 'lat': 30.5085915, 'lng': -97.6788056},
                {'location': 'Cambridge, Massachusetts', 'lat': 42.3750997, 'lng': -71.1056157},
                {'location': 'Meridian, Idaho', 'lat': 43.6086295, 'lng': -116.392326},
                {'location': 'West Palm Beach, Florida', 'lat': 26.715364, 'lng': -80.0532942},
                {'location': 'Evansville, Indiana', 'lat': 37.9747645, 'lng': -87.5558483},
                {'location': 'Clearwater, Florida', 'lat': 27.9658533, 'lng': -82.8001026},
                {'location': 'Billings, Montana', 'lat': 45.7874957, 'lng': -108.49607},
                {'location': 'West Jordan, Utah', 'lat': 40.6096698, 'lng': -111.9391031},
                {'location': 'Richmond, California', 'lat': 37.9357576, 'lng': -122.347748},
                {'location': 'Westminster, Colorado', 'lat': 39.8366528, 'lng': -105.0372046},
                {'location': 'Manchester, New Hampshire', 'lat': 42.9956397, 'lng': -71.4547891},
                {'location': 'Lowell, Massachusetts', 'lat': 42.6334247, 'lng': -71.3161718},
                {'location': 'Wilmington, North Carolina', 'lat': 34.2257282, 'lng': -77.9447107},
                {'location': 'Antioch, California', 'lat': 38.0049214, 'lng': -121.805789},
                {'location': 'Beaumont, Texas', 'lat': 30.0860459, 'lng': -94.1018461},
                {'location': 'Provo, Utah', 'lat': 40.2338438, 'lng': -111.6585337},
                {'location': 'North Charleston, South Carolina', 'lat': 32.9131295, 'lng': -80.0629981965219},
                {'location': 'Elgin, Illinois', 'lat': 42.03726, 'lng': -88.2810994},
                {'location': 'Carlsbad, California', 'lat': 33.1580933, 'lng': -117.3505966},
                {'location': 'Odessa, Texas', 'lat': 31.8457149, 'lng': -102.367687},
                {'location': 'Waterbury, Connecticut', 'lat': 41.5538091, 'lng': -73.0438362},
                {'location': 'Springfield, Illinois', 'lat': 39.7990175, 'lng': -89.6439575},
                {'location': 'League City, Texas', 'lat': 29.5074538, 'lng': -95.0949303},
                {'location': 'Downey, California', 'lat': 33.942215, 'lng': -118.1235646},
                {'location': 'Gresham, Oregon', 'lat': 45.4997475, 'lng': -122.4309766},
                {'location': 'High Point, North Carolina', 'lat': 35.9556924, 'lng': -80.0053176},
                {'location': 'Broken Arrow, Oklahoma', 'lat': 36.0525993, 'lng': -95.7908195},
                {'location': 'Peoria, Illinois', 'lat': 40.6938609, 'lng': -89.5891008},
                {'location': 'Lansing, Michigan', 'lat': 42.7337712, 'lng': -84.5553805},
                {'location': 'Lakeland, Florida', 'lat': 28.0394654, 'lng': -81.9498042},
                {'location': 'Pompano Beach, Florida', 'lat': 26.2378597, 'lng': -80.1247667},
                {'location': 'Costa Mesa, California', 'lat': 33.6636611, 'lng': -117.91946518435181},
                {'location': 'Pueblo, Colorado', 'lat': 38.2544472, 'lng': -104.609141},
                {'location': 'Lewisville, Texas', 'lat': 33.046233, 'lng': -96.994174},
                {'location': 'Miami Gardens, Florida', 'lat': 25.9420377, 'lng': -80.2456045},
                {'location': 'Las Cruces, New Mexico', 'lat': 32.3140354, 'lng': -106.7798078},
                {'location': 'Sugar Land, Texas', 'lat': 29.6196787, 'lng': -95.6349463},
                {'location': 'Murrieta, California', 'lat': 33.577752399999994, 'lng': -117.18845420205307},
                {'location': 'Ventura, California', 'lat': 34.4458248, 'lng': -119.0779359},
                {'location': 'Everett, Washington', 'lat': 47.9673056, 'lng': -122.2013998},
                {'location': 'Temecula, California', 'lat': 33.4946353, 'lng': -117.1473661},
                {'location': 'Dearborn, Michigan', 'lat': 42.3222599, 'lng': -83.1763145},
                {'location': 'Santa Maria, California', 'lat': 34.9531295, 'lng': -120.4358577},
                {'location': 'West Covina, California', 'lat': 34.0471735, 'lng': -117.8963242650494},
                {'location': 'El Monte, California', 'lat': 34.0751571, 'lng': -118.036849},
                {'location': 'Greeley, Colorado', 'lat': 40.4233142, 'lng': -104.7091322},
                {'location': 'Sparks, Nevada', 'lat': 39.5404679, 'lng': -119.7487235},
                {'location': 'Centennial, Colorado', 'lat': 39.5680644, 'lng': -104.97783081285026},
                {'location': 'Boulder, Colorado', 'lat': 40.0149856, 'lng': -105.270545},
                {'location': 'Sandy Springs, Georgia', 'lat': 33.9242688, 'lng': -84.3785379},
                {'location': 'Inglewood, California', 'lat': 33.9562003, 'lng': -118.353132},
                {'location': 'Edison, New Jersey', 'lat': 40.538457, 'lng': -74.39450186737218},
                {'location': 'South Fulton, Georgia', 'lat': 33.5902659, 'lng': -84.6712292},
                {'location': 'Green Bay, Wisconsin', 'lat': 44.5126379, 'lng': -88.0125794},
                {'location': 'Burbank, California', 'lat': 34.1816482, 'lng': -118.3258554},
                {'location': 'Renton, Washington', 'lat': 47.4799078, 'lng': -122.2034496},
                {'location': 'Hillsboro, Oregon', 'lat': 45.5228939, 'lng': -122.989827},
                {'location': 'El Cajon, California', 'lat': 32.7947731, 'lng': -116.9625269},
                {'location': 'Tyler, Texas', 'lat': 32.3512601, 'lng': -95.3010624},
                {'location': 'Davie, Florida', 'lat': 26.0628665, 'lng': -80.2331038},
                {'location': 'San Mateo, California', 'lat': 37.496904, 'lng': -122.3330573},
                {'location': 'Brockton, Massachusetts', 'lat': 42.0834335, 'lng': -71.0183787},
                {'location': 'Concord, North Carolina', 'lat': 35.4094178, 'lng': -80.5800049},
                {'location': 'Jurupa Valley, California', 'lat': 33.9798472, 'lng': -117.4515754},
                {'location': 'Daly City, California', 'lat': 37.6904826, 'lng': -122.47267},
                {'location': 'Allen, Texas', 'lat': 33.1031744, 'lng': -96.6705503},
                {'location': 'Rio Rancho, New Mexico', 'lat': 35.269381, 'lng': -106.632819},
                {'location': 'Rialto, California', 'lat': 34.1064001, 'lng': -117.3703235},
                {'location': 'Woodbridge, New Jersey', 'lat': 40.5576045, 'lng': -74.2845911},
                {'location': 'South Bend, Indiana', 'lat': 41.6833813, 'lng': -86.2500066},
                {'location': 'Spokane Valley, Washington', 'lat': 47.6571104, 'lng': -117.2613936},
                {'location': 'Norwalk, California', 'lat': 33.9092802, 'lng': -118.0849169},
                {'location': 'Menifee, California', 'lat': 33.6864432, 'lng': -117.1770437},
                {'location': 'Vacaville, California', 'lat': 38.3565773, 'lng': -121.9877444},
                {'location': 'Wichita Falls, Texas', 'lat': 33.9137085, 'lng': -98.4933873},
                {'location': 'Davenport, Iowa', 'lat': 41.5236436, 'lng': -90.5776368},
                {'location': 'Quincy, Massachusetts', 'lat': 42.2509914, 'lng': -71.0037374},
                {'location': 'Chico, California', 'lat': 39.7284945, 'lng': -121.8374777},
                {'location': 'Lynn, Massachusetts', 'lat': 42.466763, 'lng': -70.9494939},
                {'location': "Lee's Summit, Missouri", 'lat': 38.9107156, 'lng': -94.3821295},
                {'location': 'New Bedford, Massachusetts', 'lat': 41.6362152, 'lng': -70.934205},
                {'location': 'Federal Way, Washington', 'lat': 47.313494, 'lng': -122.3393103},
                {'location': 'Clinton, Michigan', 'lat': 42.9435238, 'lng': -84.6125345},
                {'location': 'Edinburg, Texas', 'lat': 26.3013982, 'lng': -98.1624501},
                {'location': 'Nampa, Idaho', 'lat': 43.5737361, 'lng': -116.559631},
                {'location': 'Roanoke, Virginia', 'lat': 37.270973, 'lng': -79.9414313}]

    products = [None, 'Azure', 'DeveloperTools', 'Dynamics365Business', 'Dynamics365Enterprise',
                'EnterpriseMobilityAndSecurity', 'Exchange', 'Microsoft365', 'Office', 'PowerBI',
                'Project', 'SQL', 'SharePoint', 'SkypeForBusiness', 'Surface', 'SurfaceHub', 'Teams',
                'Visio', 'Windows', 'Yammer']

    industry_lst = [None, 'Agriculture', 'Distribution', 'Education', 'Financial Services', 'Government', 'Healthcare',
                      'Hospitality  & Travel', 'Manufacturing & Resources', 'Media & Communications', 'Nonprofit & IGO',
                      'Power and utilities', 'Professional services', 'Public Safety and National Security',
                      'Retail & Consumer Goods', 'Transportation']

    category_lst = [None, 'Analytics', 'Artificial Intelligence', 'Azure Stack', 'BackupDisasterRecovery',
                    'Big Data', 'Blockchain', 'Chatbot', 'Cloud Database Migration', 'CloudMigration',
                    'CloudVoice', 'Cognitive Services', 'CompetitiveDatabaseMigration', 'Containers',
                    'Data Warehouse', 'DatabaseonLinux', 'Developer Tools', 'Enterprise Business Intelligence',
                    'HighPerformanceComputing', 'Hybrid Storage', 'IdentityAccessMngmnt',
                    'Information Management', 'InformationProtection', 'Internet of Things',
                    'AI + Machine Learning', 'Media', 'Microserviceapplications', 'Mobile applications',
                    'Networking', 'RegulatorycomplianceGDPR', 'Security', 'ServerlessComputing',
                    'Threat Protection', 'WebDevelopment']

    services = [None, 'Consulting', 'Custom solution', 'Deployment or Migration', 'Hardware', 'IP Services (ISV)',
                'Integration', 'Learning and Certification', 'Licensing', 'Managed Services (MSP)', 'ProjectManagement']

    sort_lst = [2, 1, 0]

    def create_params(self, filters):

        params = f"sort={filters['sort']};pageSize={filters['pageSize']};pageOffset={filters['pageOffset']};onlyThisCountry=true;country={filters['country']};radius=100;lat={filters['lat']};lng={filters['lng']};"
        if filters['product'] is not None:
            params += f"products={filters['product']};"
        if filters['industry'] is not None:
            params += f"industries={filters['industry']};"
        if filters['category'] is not None:
            params += f"categories={filters['category']};"
        if filters['service'] is not None:
            params += f"services={filters['service']};"
        return self.api_link + '?filter=' + urllib.parse.quote(params)

    def start_requests(self):
        """
        landing function of spider class
        will get api results, create the search pages and pass it to parse function
        :return:
        """
        # for sort in self.sort_lst:
        for product in self.products:
            for industry in self.industry_lst:
                for category in self.category_lst:
                    for service in self.services:
                        for value in self.cities:
                            # self.logger.info('Running City: ' + str(value['location']))
                            filters = {'product': product,
                                       'industry': industry,
                                       'category': category,
                                       'service': service,
                                       'sort': 2,
                                       'city': value['location'],
                                       'lat': value['lat'],
                                       'lng': value['lng'],
                                       'pageOffset': 0,
                                       'pageSize': 20,
                                       'country': 'US',
                                       }

                            while filters['pageOffset'] < 100:
                                yield scrapy.Request(self.create_params(filters), callback=self.parse,
                                                     dont_filter=True)

                                filters['pageOffset'] += filters['pageSize']
            #                     break
            #                 break
            #             break
            #         break
            #     break
            # break

    def parse(self, response):
        """
        parse partner page and get result json data
        :param response:
        :return:
        """

        if response.status == 200:
            search_response = response.json()
        elif response.status == 403:
            # self.crawler.engine.pause()
            # print('Engine Paused')
            # time.sleep(10)
            # self.crawler.engine.unpause()
            yield scrapy.Request(response.request.url, callback=self.parse, meta=response.meta, dont_filter=True)
            return
        else:
            self.logger.info('*'*50)
            self.logger.info('ERROR: ' + str(response.status))
            self.logger.info(response.request.url)
            self.logger.info(response.text)
            self.logger.info('*' * 50)
            return

        if 'estimatedTotalMatchingPartners' in search_response \
                and search_response['estimatedTotalMatchingPartners'] >= 0 \
                and 'matchingPartners' in search_response \
                and 'totalCount' in search_response['matchingPartners'] \
                and search_response['matchingPartners']['totalCount'] >= 0:

            partners = search_response['matchingPartners']['items']
            self.logger.info(response.request.url + f'<result: {len(partners)}>')

            for partner in partners:
                # get partner page link
                partnerId = partner['partnerId']
                if partnerId in self.done_ids:
                    continue
                partner_link = self.api_link + partnerId
                yield scrapy.Request(partner_link, callback=self.parse_partner,
                                     meta={'partnerId': partnerId,
                                           'location': partner['location']['address'],
                                           'trial': 1})
                # print(partner_link)
                # break

    def parse_partner(self, response):
        """
        parse partner page and get result json data
        :param response:
        :return:
        """

        partner_link = response.request.url
        partnerId = response.meta['partnerId']
        location = response.meta['location']
        trial = response.meta['trial']

        if response.status == 200:
            search_response = response.json()
        elif response.status == 403:
            # self.crawler.engine.pause()
            # print('Engine Paused')
            # time.sleep(10)
            # self.crawler.engine.unpause()
            yield scrapy.Request(partner_link, callback=self.parse_partner, dont_filter=True,
                                 meta={'partnerId': partnerId,
                                       'location': location,
                                       'trial': 1})
            return
        else:
            if trial == 1:
                yield scrapy.Request(partner_link, callback=self.parse_partner, dont_filter=True,
                                     meta={'partnerId': partnerId,
                                           'location': location,
                                           'trial': 2})
            return

        # Initialize item
        item = dict()
        for k in self.item_fields:
            item[k] = ''

        item['partner_program_link'] = self.partner_program_link
        item['partner_directory'] = self.partner_directory
        item['partner_program_name'] = self.partner_program_name

        data = response.json()
        partner = data['partnerDetails']
        self.done_ids.add(partnerId)

        # 'profileId': partnerId,
        # 'microsoft_link': 'https://appsource.microsoft.com/en-us/marketplace/partner-dir/' + partnerId + '/overview',
        item['partner_company_name'] = partner['name'] if 'name' in partner else ''
        # item['company_domain_name'] = partner['url'] if 'url' in partner else ''
        url_obj = urllib.parse.urlparse(partner['url'] if 'url' in partner else '')
        item['company_domain_name'] = url_obj.netloc if url_obj.netloc != '' else url_obj.path
        x = re.split(r'www\.', item['company_domain_name'], flags=re.IGNORECASE)
        if x:
            item['company_domain_name'] = x[-1]

        item['linkedin_link'] = partner['linkedInOrganizationProfile'] if 'linkedInOrganizationProfile' in partner else ''
        item['specializations'] = partner['programQualificationsAsp'] if 'programQualificationsAsp' in partner else ''
        item['headquarters_street'] = location['addressLine1'] if 'addressLine1' in location else '' + (location['addressLine2'] if 'addressLine2' in location else '')
        item['headquarters_city'] = location['city'] if 'city' in location else ''
        item['headquarters_state'] = location['state'] if 'state' in location else ''
        item['headquarters_zipcode'] = location['postalCode'] if 'postalCode' in location else ''
        item['headquarters_country'] = location['country'] if 'country' in location else ''
        item['company_description'] = cleanhtml(partner['description']) if 'description' in partner else ''
        # item['partner_tier'] = partner['organizationSize'] if 'organizationSize' in partner else ''
        item['industries'] = partner['industryFocus'] if 'industryFocus' in partner else ''
        item['services'] = partner['serviceType'] if 'serviceType' in partner else ''
        item['products'] = partner['product'] if 'product' in partner else ''
        item['solutions'] = partner['solutions'] if 'solutions' in partner else ''
        item['competencies'] = partner['competencies'] if 'competencies' in partner else ''
        item['company_size'] = partner['organizationSize'] if 'organizationSize' in partner else ''
        # item['partner_clients'] = partner['targetCustomerCompanySizes'] if 'targetCustomerCompanySizes' in partner else ''
        if 'allLocations' in data and len(data['allLocations']) > 0:
            for loc in data['allLocations']:
                item['locations_street'] = loc['address']['addressLine1'] + (' ' + loc['address']['addressLine2']) if 'addressLine2' in loc['address'] and loc['address']['addressLine2'] != '' else ''
                item['locations_city'] = loc['address']['city'] if 'city' in loc['address'] else ''
                item['locations_state'] = loc['address']['state'] if 'state' in loc['address'] else ''
                item['locations_zipcode'] = loc['address']['postalCode'] if 'postalCode' in loc['address'] else ''
                item['locations_country'] = loc['address']['country'] if 'country' in loc['address'] else ''

                # create result item
                yield item
        else:
            # create result item
            yield item
