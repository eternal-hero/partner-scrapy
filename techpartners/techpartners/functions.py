import hashlib,re,requests,urllib.parse
#pip install price-parser
from price_parser import Price
def Get_Price(xau):
    PRICE = Price.fromstring(xau)
    KQ=PRICE.amount
    if '$' in KQ:
        KQ=str(PRICE.currency)+str(PRICE.amount)
    return(KQ)
def translate(text,fromlag,tolang):
    data = {'text': text,'gfrom': fromlag,'gto': tolang}
    response = requests.post('https://www.webtran.eu/gtranslate/', data=data)
    return(response.text)
def Get_Price(xau):
    KQ=re.sub(r"([^$0-9.])","", str(xau).strip())
    return KQ
def Get_Number(xau):
    KQ=re.sub(r"([^0-9.])","", str(xau).strip())
    return KQ
def Get_String(xau):
    KQ=re.sub(r"([^A-Za-z_])","", str(xau).strip())
    return KQ
def complete_url(domain_url,url):
    str_url=str(domain_url).split('/')
    if str(url).startswith('//'):
        url=str_url[0]+url
    elif str(url).startswith('/'):
        url=str_url[0]+'//'+str_url[2]+url
    elif not str(url).startswith('http'):
        url=str_url[0]+'//'+str_url[2]+'/'+url
    return url
def is_domain(url):
    url=str(url).strip()
    CHK=False
    if str(url).count('/')==2:
        CHK=True
    elif str(url).count('/')==3 and str(url).endswith('/'):
        CHK=True
    return CHK
def cleanhtml(raw_html):
    if raw_html:
        raw_html=str(raw_html).replace('</',' ^</')
        cleanr = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
        cleantext = re.sub(cleanr, '', raw_html)
        cleantext=(' '.join(cleantext.split())).strip()
        cleantext=str(cleantext).replace(' ^','^').replace('^ ','^')
        while '^^' in cleantext:
            cleantext=str(cleantext).replace('^^','^')
        cleantext=str(cleantext).replace('^','\n')
        cleantext = re.sub(r'[\r\n\t\s]+', ' ', cleantext).strip()
        return cleantext.strip()
    else:
        return ''
def kill_space(xau):
    xau=str(xau).replace('\t','').replace('\r','').replace('\n',' ')
    xau=(' '.join(xau.split())).strip()
    return xau
def key_MD5(xau):
    xau=(xau.upper()).strip()
    KQ=hashlib.md5(xau.encode('utf-8')).hexdigest()
    return KQ
def get_item_from_json(result,item,space):
    if isinstance(item,dict):
        for k,v in item.items():
            if isinstance(v,dict) or isinstance(v,list):
                if space=='':
                    get_item_from_json(result,v,k)
                else:
                    get_item_from_json(result,v,space+'.'+k)
            else:
                if space=='':
                    result[k]=v
                else:
                    result[space+'.'+k]=v
    else:
        for i in range(len(item)):
            k=str(i)
            v=item[i]
            if isinstance(v,dict) or isinstance(v,list):
                if space=='':
                    get_item_from_json(result,v,k)
                else:
                    get_item_from_json(result,v,space+'.'+k)
            else:
                if space=='':
                    result[k]=v
                else:
                    result[space+'.'+k]=v
    return result
def get_domain_from_url(url):
    try:
        # url_obj = urllib.parse.urlparse(url)
        # url = url_obj.netloc if url_obj.netloc != '' else url_obj.path
        # x = re.split(r'www\.', url, flags=re.IGNORECASE)
        # if x:
        #     url = x[-1]
        # if '/' in url:
        #     url = url_obj[:url.find('/')]
        # return url
        if '@' in url:
            url = 'www.' + url[url.find('@') + 1:]
        pattern = re.search(r"(https?:?/*)?(w*\.)?([^./]*?)(\.[^/]+)(/.*)?", url, re.IGNORECASE)
        if pattern:
            return pattern.group(3) + pattern.group(4)
        else:
            return url
    except Exception as e:
        print('DOMAIN ERROR: ', e)
        return url
