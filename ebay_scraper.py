from itertools import count
import requests
from bs4 import BeautifulSoup as bs
from time import sleep
from concurrent.futures import ThreadPoolExecutor
import gspread
from gspread import authorize
from oauth2client.service_account import ServiceAccountCredentials
import datetime
datetimes = str(datetime.datetime.now())[:19]
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36'
}
#--connecting g.sheet------------
scopes = ["https://spreadsheets.google.com/feeds",
                  "https://www.googleapis.com/auth/spreadsheets",
                  "https://www.googleapis.com/auth/drive",
                  "https://www.googleapis.com/auth/drive"]
cred = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scopes)
gc= authorize(cred)
worksheet = gc.open("Ebay_Scraping").sheet1
worksheet2 = gc.open("Ebay_Scraping").get_worksheet(1)
worksheet3 = gc.open("products_export_1").get_worksheet(0)
product_list = worksheet.col_values(1)[1:]
datetimes = str(datetime.datetime.now())[:19]
worksheet.update(f'G2', f'"Searching" {len(product_list)} products started..at {datetimes}')
lower_list = []
for product in product_list:
    lower_list.append(product.lower())
product_search_links = []
for product_name in product_list:
    product_name = product_name.replace(' ','+')
    search_url = f'https://www.ebay.com/sch/i.html?_from=R40&_nkw={product_name}&_sacat=0&LH_TitleDesc=0&rt=nc&LH_ItemCondition=3'
    product_search_links.append(search_url)



#--connecting g.sheet------------
def threading(links):
    res_html = []
    def fetch(session, url):
        with session.get(url) as response:
            if response.status_code == 200:
                res_html.append(response.content)
    def main():
        
            # with requests.Session() as session:
                # adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
                # session.mount('http://', adapter)
                # session.mount('https://', adapter)
                # session.headers.update({'Connection':'Keep-Alive'})
        count = 0
        counter = [x for x in range(50) if x%10==0][1:]
        executor = ThreadPoolExecutor(max_workers=50)
        with requests.Session() as session:
            for link in links:
                count = count + 1
                executor.map(fetch, [session], [link])
                if count in counter:
                    executor.shutdown(wait=True)
                    executor = ThreadPoolExecutor(max_workers=50)
    main()
    return res_html




def product_update():
    product_list_to_u = worksheet3.col_values(2)[1:]
    prod_up = [[name] for name in product_list_to_u]
    worksheet.update('A2:A100000', prod_up)
    sleep(0.5)

def scrape(product_search_links):
    #--------------------------------
    def send_links(product_search_links):
        final_list =[] 
        lists = product_search_links
        print(len(product_search_links))
        loop = int(len(lists)/50)
        if loop==0:
            loop = 1
        i_list = [x*50+50 for x in range(loop)]
        k_list = [x*50 for x in range(loop)]
        count = 50
        for i,k in zip(i_list,k_list):
            print(f'Scraping Products Data: {count}')
            count = count + 50
            new_five = lists[k:i]
            res_html = threading(new_five) # calling the thread
            final_list = final_list + res_html
            sleep(2)
        if loop*50<len(lists):
            print(f'Scraping Products Data: rest')
            last_five = lists[i_list[-1]:]
            res_html = threading(last_five) # calling the thread
            final_list = final_list + res_html
            sleep(2)
        return final_list
    final_list = send_links(product_search_links)
    datetimes = str(datetime.datetime.now())[:19]
    worksheet.update(f'G3', f'"Searching" each product Ended..at {datetimes}')


    row_check = []
    product_update = []
    for html in final_list:
        try:
            try:
                final_html = str(html).split('class="clearfix srp-controls__row-2"')[1]
            except:
                final_html = str(html).split('class="s-answer-region s-answer-region-center-top"')[1]
            soup = bs(final_html,'html.parser')

            product_name = soup.findAll('span',attrs = {'class':'BOLD'})[1].text
            print(product_name)
            row_no = lower_list.index(product_name)

            li_tags = soup.find('ul',attrs = {'class':'srp-results srp-list clearfix'}).findAll('li')
            prices = []
            product_urls = []

            
            products_found = int(soup.find('span',attrs = {'class':'BOLD'}).text.strip())
            if products_found >5:
                products_found = 5

            if products_found == 1:
                li = li_tags[0]
                try:
                    div = li.find('div',attrs={'class':'s-item__info clearfix'})
                    product_url = str(div.find('a')['href']).split('?')[0]
                    price  = div.find('span',attrs = {'class':'s-item__price'}).text.replace('$','').replace(',','').split('.')[0]
                    if 'to' in price:
                        price = price.split('to')[1].replace('$','').replace(',','').strip().split('.')[0]
                except:
                    product_url = ''
            if products_found>1:
                for li in li_tags:
                    try:
                        try:
                            product_n = li.find('span',attrs = {'role':'heading'}).text.lower()
                        except:
                            product_n = ''
                        div = li.find('div',attrs={'class':'s-item__info clearfix'})
                        product_url = str(div.find('a')['href']).split('?')[0]
                        
                        price  = div.find('span',attrs = {'class':'s-item__price'}).text.replace('$','').replace(',','').split('.')[0]
                        if 'to' in price:
                            price = price.split('to')[1].replace('$','').replace(',','').strip().split('.')[0]
                        
                        product_n_check = product_name.split(' ')[0]
                        product_n_check2 = product_name.split(' ')[0].replace('-','')[:6]
                        if product_n_check in product_n:
                            prices.append(int(price))
                            product_urls.append(product_url)
                        elif product_n_check2 in product_n.replace('-',''):
                            prices.append(int(price))
                            product_urls.append(product_url)
                    except:
                        pass
                try:
                    if len(prices)>=5:
                        max_price = max(prices[:products_found])
                    else:
                        max_price = max(prices)
                except:
                    max_price = prices[0]
                index = prices.index(max_price)
                product_url = product_urls[index]

                row_check.append(row_no)
                product_update.append(product_url)
        except:
            pass
        
    list_of_product_links_to_upload = []

    list_to_check = ['' for i in range(len(product_search_links))]
    
    for row,link in zip(row_check,product_update):
        list_to_check[row] = link
    list_of_product_links_to_upload = [[link] for link in list_to_check]
    print(list_of_product_links_to_upload)
    worksheet.update('B2:B100000', list_of_product_links_to_upload)




    product_links  = worksheet.col_values(2)[1:]
    # final_list = send_links(product_search_links)
    datetimes = str(datetime.datetime.now())[:19]
    worksheet.update(f'G5', f'Scraping each product "Started"..at {datetimes}')
        

    rows_to_add = []
    count = 1
    for product_link in product_links:
        each_row = []
        print(f'Scraping[{count}/{len(product_links)}]: {product_link}')
        count = count + 1
        if 'ebay' in product_link:
            try:
                r = requests.get(product_link,headers = headers)
                try:
                    final_html = str(r.content).split('id="CenterPanelInternal"')[1]
                except:
                    final_html = str(r.content).split('id="CenterPanel"')[1]
                soup = bs(final_html,'html.parser')

                try:
                    try:
                        price = '$' + str(soup.find('span',attrs = {'id':'prcIsum'}).text.split('$')[1].replace('/ea',''))
                    except:
                        price = '$' + str(soup.find('span',attrs = {'id':'mm-saleDscPrc'}).text.split('$')[1].replace('/ea',''))
                except:
                    price = ''
                if price == '':
                    try:
                        price = str(soup.find('span',attrs = {'id':'prcIsum'}).text.reaplce('/ea',''))
                    except:
                        price = ''
                try:
                    available = str(soup.find('span',attrs = {'id':'qtySubTxt'}).find('span').text.strip()).replace('\n\t\t\t\t\t\t\t\t','').split('\\t')[-1]
                except:
                    available = ''
                try:
                    feedback  = soup.findAll('div',attrs = {'class':'ux-seller-section__item'})[1].text.strip().replace('Contact seller','')
                except:
                    feedback = ''
                # print(price)
                # print(available)
                # print(feedback)
            except:
                pass
        else:
            price = ''
            available = ''
            feedback = ''
        each_row.append(price)
        each_row.append(available)
        each_row.append(feedback)
        rows_to_add.append(each_row)
        price = ''
        available = ''
        feedback = ''


    worksheet.update('C2:E100000', rows_to_add)

    datetimes = str(datetime.datetime.now())[:19]
    worksheet.update(f'G6', f'Scraping each product "Ended"..at {datetimes}')

    #--Price Update------------------------------------------------
    print('Updting Prices By Markup')
    worksheet.update(f'G8', f'Start: Adding Markup To Prices')
    product_prices  = worksheet.col_values(3)[1:]                                
    min1 = int(worksheet2.acell('B2').value)
    min2 = int(worksheet2.acell('B3').value)
    min3 = int(worksheet2.acell('B4').value)
    min4 = int(worksheet2.acell('B5').value)
    min5 = int(worksheet2.acell('B6').value)

    max1 = int(worksheet2.acell('C2').value)
    max2 = int(worksheet2.acell('C3').value)
    max3 = int(worksheet2.acell('C4').value)
    max4 = worksheet2.acell('C5').value
    if max4 == '':
        pass
    else:
        max4 = int(max4)
    max5 = worksheet2.acell('C6').value
    if max5 == '':
        pass
    else:
        max5 = int(max5)

    mark1 = worksheet2.acell('D2').value
    mark2 = worksheet2.acell('D3').value
    mark3 = worksheet2.acell('D4').value
    mark4 = worksheet2.acell('D5').value
    mark5 = worksheet2.acell('D6').value

    prices_to_update = []
    for price in product_prices:
        each = []
        try:
            price = int(float(price.replace('$','').replace(',','')))
        except:
            price = ''
        if price == '':
            new_price = ''
            each.append(new_price)
            prices_to_update.append(each)
        else:
            if price>=min1:
                if price<=max1:
                    margin = int(str(mark1.replace('%','')))/100
                    new_price = price + price*margin
            if price>=min2:
                if price<=max2:
                    margin = int(str(mark2.replace('%','')))/100
                    new_price = price + price*margin
            if price>=min3:
                if price<=max3:
                    margin = int(str(mark3.replace('%','')))/100
                    new_price = price + price*margin
            if price>=min4:
                if max4 == '':
                    margin = int(str(mark4.replace('%','')))/100
                    new_price = price + price*margin
                elif price<=max4:
                    margin = int(str(mark4.replace('%','')))/100
                    new_price = price + price*margin
            if price>=min5:
                if max5 == '':
                    margin = int(str(mark5.replace('%','')))/100
                    new_price = price + price*margin
                elif price<=max5:
                    margin = int(str(mark5.replace('%','')))/100
                    new_price = price + price*margin
            new_price = round(new_price)
            each.append(new_price)
            prices_to_update.append(each)

    worksheet.update('F2:F100000', prices_to_update)
    worksheet.update(f'G9', f'Done! Adding Markup To Prices')
    print('All Done!')

def price_quantity_update():
    mark_prices = worksheet.col_values(6)[1:]
    quantities = worksheet.col_values(4)[1:]
    quantity = []
    for quan in quantities:
        qu = quan.replace('available','').replace('More than','').strip()
        if qu == '':
            qu = 0
        quantity.append(qu)
    
    quant_up = [[quant] for quant in quantity]
    mark_pr = [[mark_p] for mark_p in mark_prices]
    worksheet3.update('R2:R100000', quant_up)
    worksheet3.update('U2:U100000', mark_pr)
    sleep(22000)



while True:
    product_search_urls = product_search_links[:1000]
    product_update()
    scrape(product_search_urls)
    price_quantity_update()


    
    r = requests.get('https://www.timeanddate.com/worldclock/bangladesh/dhaka',headers= headers)
    soup = bs(r.content, 'html.parser')
    date = soup.find('span',attrs={'id':'ctdat'}).text.lower()
    if 'mon' in date or 'tues' in date or 'wed' in date or 'thurs' in date or 'fri' in date:

        time1 = soup.find('span',attrs= {'id':'ct'}).text.lower()
        print(time1)
        time = time1[:5]
        
        if 'pm' in str(time1):
            if '10:01' in str(time):
                product_update()
                scrape()
                price_quantity_update()
            if '10:02' in str(time):
                product_update()
                scrape()
                price_quantity_update()
            if '10:03' in str(time):
                product_update()
                scrape()
                price_quantity_update()
            if '10:04' in str(time):
                product_update()
                scrape()
                price_quantity_update()
            if '10:05' in str(time):
                product_update()
                scrape()
                price_quantity_update()
            if '10:06' in str(time):
                product_update()
                scrape()
                price_quantity_update()
            if '10:07' in str(time):
                product_update()
                scrape()
                price_quantity_update()
            if '10:08' in str(time):
                product_update()
                scrape()
                price_quantity_update()
        sleep(179)
    else:
        sleep(179)