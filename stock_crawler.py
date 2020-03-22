import pandas as pd
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup as bs
import time
import requests
import json
from pandas.io.json import json_normalize
from datetime import datetime
from dateutil.parser import parse
from calendar import monthrange
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


options = Options()
options.headless = True

def try_urlopen(url):
	resp = None
	retry = False
	success = False

	for i in range(10):
		try:
			if retry:
				print('connection retry:', url)
			req = Request(url)
			resp = urlopen(req)
		except ConnectionResetError:
			time.sleep(5)
			print('connection reset:', url)
			retry = True
			pass
		except Exception as e:
			print('connection exception:', url)
			print(e)
			time.sleep(5)
			retry = True
			pass
		else:
			if retry:
				print('connection retry success:', url)
			success = True
		if success:
			break	
		
	return resp


def try_urlopen_with_selenium(url):	
	options = Options()
	options.headless = True	
	chrome_dir = r'C:\Users\mashgold\Anaconda3\Lib\site-packages\selenium\webdriver\chrome\chromedriver.exe'
	driver = webdriver.Chrome(chrome_dir, options=options)
	
	resp = None
	retry = False
	success = False	

	for i in range(10):
		try:
			if retry:
				print('connection retry:', url)
			driver.get(url)
			resp = driver.page_source
		except ConnectionResetError:
			time.sleep(5)
			print('connection reset:', url)
			retry = True
			pass
		except Exception as e:
			print('connection exception:', url)
			print(e)
			time.sleep(5)
			retry = True
			pass
		else:
			if retry:
				print('connection retry success:', url)
			success = True
		if success:
			break
			
	driver.close()
	
	return resp


def get_etf_ticker():
	result_list = []
	type_list = ['etf', 'etn']
	
	for t in type_list:		
		url = f'https://finance.naver.com/api/sise/{t}ItemList.nhn'
		print(url)
		json_data = json.loads(requests.get(url).text)
		df_ = json_normalize(json_data['result'][f'{t}ItemList'])
		df_ = (df_.loc[:, ['itemcode', 'itemname', 'marketSum']]
			  .rename(columns={'marketSum': 'market_sum', 'itemcode': 'ticker'}))
		result_list.append(df_)
	
	df = pd.concat(result_list)			
	
	return df


def get_krx_ticker():
	df_ticker = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13', header=0)[0]
	df_ticker.종목코드 = df_ticker.종목코드.map('{:06d}'.format).astype(str)
	df_ticker = df_ticker[['회사명', '종목코드', '업종']]
	df_ticker = df_ticker.rename(columns={'회사명': 'company_name', '종목코드': 'ticker', '업종': 'industry'})

	return df_ticker


def fix_ymd(ym):
	"""
	yyyy/m 형식의 문자열을 받아 yyyy-mm-dd 문자열을 반환한다
	"""
	tdate = parse(ym)
	y, m = tdate.year, tdate.month
	d = monthrange(y, m)[1]
	tdate = datetime.strptime((str(y) + '-' + str(m) + '-' + str(d)),
							  ('%Y-%m-%d')).strftime('%Y-%m-%d')

	return tdate


def get_fng_snapshot(ticker):
	"""
	fnguide snapshot 데이터의 연, 분기 별 데이터를 읽어서 매출액, 영업이익, 순이익, ROE를 리턴한다
	"""
	snapshot_url = f'http://comp.fnguide.com/SVO2/asp/SVD_Main.asp?pGB=1&gicode=A{ticker}&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701'
	snapshot_page = try_urlopen(snapshot_url)
	snapshot_tables = pd.read_html(snapshot_page)

	df_yearly = snapshot_tables[11]
	df_yearly = df_yearly.set_index(df_yearly.columns[0])
	df_yearly = df_yearly.loc[['매출액', '영업이익', '지배주주순이익', 'ROE', 'BPS', 'DPS']]
	df_yearly = df_yearly.T.reset_index(level=0, drop=True)
	df_yearly['type'] = 'Y'

	df_quaterly = snapshot_tables[12]
	df_quaterly = df_quaterly.set_index(df_quaterly.columns[0])
	df_quaterly = df_quaterly.loc[['매출액', '영업이익', '지배주주순이익', 'ROE', 'BPS', 'DPS']]
	df_quaterly = df_quaterly.T.reset_index(level=0, drop=True)
	df_quaterly['type'] = 'Q'

	df = pd.concat([df_yearly, df_quaterly])
	df['ticker'] = ticker

	df = (
		df
		.reset_index()
		.rename(columns={'index': 'tdate',
						 '매출액': 'revenue',
						 '영업이익': 'opm',
						 '지배주주순이익': 'earning',
						 'ROE': 'roe',
						 'BPS': 'bps',
						 'DPS': 'dps'}))

	df['r3'] = [td[-3::] for td in df.tdate]
	df['is_forecast'] = 0
	df.loc[df['r3'] == "(E)", ['is_forecast']] = 1
	df['ym'] = [td[:7] for td in df.tdate]
	df['tdate'] = [fix_ymd(ym) for ym in df.ym]
	df = df.loc[:, ['ticker', 'tdate', 'revenue', 'opm', 'earning', 'roe', 'bps', 'dps', 'type', 'is_forecast']]

	return df


def get_fng_statements(ticker):
	fs_url = f'http://comp.fnguide.com/SVO2/asp/SVD_Finance.asp?pGB=1&cID=&MenuYn=Y&ReportGB=&NewMenuID=103&stkGb=701&gicode=A{ticker}'
	fs_page = try_urlopen(fs_url)
	fs_tables = pd.read_html(fs_page)

	# 연간
	df_temp1, df_temp2, df_temp3 = fs_tables[0], fs_tables[2], fs_tables[4]

	df_temp1 = df_temp1.set_index(df_temp1.columns[0])
	df_temp1 = df_temp1[df_temp1.columns[:4]]
	df_temp1 = df_temp1.loc[['매출액', '영업이익', '당기순이익']]

	df_temp2 = df_temp2.set_index(df_temp2.columns[0])
	df_temp2 = df_temp2.loc[['자산', '부채', '자본']]

	df_temp3 = df_temp3.set_index(df_temp3.columns[0])
	df_temp3 = df_temp3.loc[['영업활동으로인한현금흐름']]

	df_yearly = pd.merge(
		pd.merge(
			df_temp1.T, df_temp2.T, how='outer', left_index=True, right_index=True),
		df_temp3.T, how='outer', left_index=True, right_index=True)

	df_yearly['type'] = 'Y'

	# 분기
	df_temp4, df_temp5, df_temp6 = fs_tables[1], fs_tables[3], fs_tables[5]

	df_temp4 = df_temp4.set_index(df_temp4.columns[0])
	df_temp4 = df_temp4[df_temp4.columns[:4]]
	df_temp4 = df_temp4.loc[['매출액', '영업이익', '당기순이익']]

	df_temp5 = df_temp5.set_index(df_temp5.columns[0])
	df_temp5 = df_temp5.loc[['자산', '부채', '자본']]

	df_temp6 = df_temp6.set_index(df_temp6.columns[0])
	df_temp6 = df_temp6.loc[['영업활동으로인한현금흐름']]

	df_quarterly = pd.merge(
		pd.merge(
			df_temp4.T, df_temp5.T, how='outer', left_index=True, right_index=True),
		df_temp6.T, how='outer', left_index=True, right_index=True)

	df_quarterly['type'] = 'Q'

	df = pd.concat([df_yearly, df_quarterly])
	df['ticker'] = ticker
	df = (
		df
		.reset_index()
		.rename(columns={'index': 'tdate',
						 '매출액': 'revenue',
						 '영업이익': 'opm',
						 '당기순이익': 'earning',
						 '자산': 'assets',
						 '부채': 'liabilities',
						 '자본': 'capital',
						 '영업활동으로인한현금흐름': 'cashflow'}))

	df['tdate'] = [fix_ymd(ym) for ym in df.tdate]
	columns = ['ticker', 'tdate', 'revenue', 'opm', 'earning', 'assets', 'liabilities', 'capital', 'cashflow', 'type']
	df = df.loc[:, columns]

	return df


def get_fng_consensus(ticker):
	url = f'http://comp.fnguide.com/SVO2/asp/SVD_Consensus.asp?pGB=1&gicode=A{ticker}&cID=&MenuYn=Y&ReportGB=&NewMenuID=108&stkGb=701'
	print(url)  
		
	html = try_urlopen_with_selenium(url)
	soup = bs(html, 'html.parser')
	content = soup.find('tbody', attrs={'id': 'bodycontent3'})

	l_list = [l.text for l in content.findAll('td', attrs={'class': 'l'})]
	c_list = [c.text for c in content.findAll('td', attrs={'class': 'c'})]
	c_list[0] = c_list[1]
	r_list = [r.text for r in content.findAll('td', attrs={'class': 'r'})]

	parse_range = range(0, len(r_list), 5)
	list_of_lists = [r_list[i:i+5] for i in parse_range]
	df = pd.DataFrame(list_of_lists)
	columns = ['ct_price', 'lt_price', 'change_ratio', 'opinion', 'l_opinion']
	df.columns = columns
	df['source'] = l_list
	df['tdate'] = c_list
	df['ticker'] = ticker
	columns_ordered = ['ticker', 'source', 'tdate', 'ct_price', 'lt_price', 'change_ratio', 'opinion', 'l_opinion']
	df = df[columns_ordered].dropna()

	return df


def get_stock_price(ticker, timeframe, period):
	url = r'https://fchart.stock.naver.com/sise.nhn?symbol={tk}&timeframe={tf}&count={p}&requestType=0'.format(tk=ticker, tf=timeframe, p=period)
	print("processing: ", url)
	price_data = try_urlopen(url)
	soup = bs(price_data)	
	item_list = soup.find_all('item')
	
	list_of_list = [item.get('data').split('|') for item in item_list]
	df_price = (pd.DataFrame(list_of_list,
							 columns=['tdate', 'open', 'high', 'low', 'close', 'volume']))
	df_price['ticker'] = ticker
	
	return df_price


def get_kr_indexes():
	ticker_list_market = ['KOSPI', 'KOSDAQ']
	df_list = []

	s = time.time()
	for ticker in ticker_list_market:
		df = get_stock_price(ticker, 'day', 10000)
		df_list.append(df)

	df_market = pd.concat(df_list)
	columns_select = ['ticker', 'tdate', 'open', 'high', 'low','close', 'volume']
	df_market = df_market.loc[:, columns_select]

	e = time.time()
	minute, second = divmod((e - s), 60)
	minute, second = int(minute), int(round(second, 0))
	print(f'crawl cost: {minute} min {second} sec')
	print('number of stocks: ', len(df_list))
	print('get_kr_indexes completed \n')
	
	return df_market


def get_global_indexes():
	config_USA = {'ticker': 'SPI@SPX',
				  'itemname': 'S&P500'}
	config_JAPAN = {'ticker': 'NII@NI225',
					 'itemname': 'Nikkei'}
	config_EURO = {'ticker': 'STX@SX5E',
					'itemname': 'Eurostoxx'}
	config_CHINA = {'ticker': 'SHS@000001',
					'itemname': 'Shanghai'}
	config_BRAZIL = {'ticker': 'BRI@BVSP',
					'itemname': 'Bovespa'}
	config_RUSSIA = {'ticker': 'RUI@RTSI',
					'itemname': 'RTS'}

	config_list = [config_USA, config_JAPAN, config_EURO, config_CHINA, config_BRAZIL, config_RUSSIA]
	print("config_list: , ", config_list)

	df_list = []
	column_select = ['symb', 'xymd', 'open', 'high', 'low', 'clos', 'gvol']

	s = time.time()
	for config in config_list:
		ticker = config.get('ticker')
		url = f'https://finance.naver.com/world/worldDayListJson.nhn?symbol={ticker}&fdtc=0&page='
		print("processing: ", ticker)
		result_list = []

		for i in range(1, 701):
			url_ = url + str(i) 
			resp = json.load(try_urlopen(url_))			
			result_list.extend(resp)		

		df = pd.DataFrame(result_list)
		df = (df.loc[:, column_select]
			  .rename(columns={'xymd': 'tdate', 'symb': 'ticker', 'clos': 'close', 'gvol': 'volume'})
			  .drop_duplicates())
		df_list.append(df)

	df_market_global = pd.concat(df_list)
	e = time.time()
	minute, second = divmod((e - s), 60)
	minute, second = int(minute), int(round(second, 0))
	print(f'crawl cost: {minute} min {second} sec')
	print('number of stocks: ', len(df_list))
	print('number of records: ', len(df_market_global))
	print('get_global_indexes completed \n')
	
	return df_market_global


def get_af_price(ticker, n_months):
	url = f'https://finance.naver.com/fund/fundDailyQuoteList.nhn?fundCd={ticker}&page='
	print("processing: ", ticker)
	
	column_select = ['날짜', '기준가', '설정액 (억)', '순 자산액(억)']
	df_list = []
	n_pages = (n_months*2) + 10

	for i in range(1, n_pages):
		url_ = url + str(i) 
		resp = try_urlopen(url_)
		df_price = pd.read_html(resp)[0]
		df_price = df_price.dropna()
		df_price = df_price.loc[:, column_select]
		df_list.append(df_price)  
		
	df = pd.concat(df_list)
	df = (df
		  .rename(columns={'날짜': 'tdate',
						   '기준가': 'close',
						   '설정액 (억)': 'cum_invest',
						   '순 자산액(억)': 'eval_total'}))
	df['tdate'] = [t.replace('.', '') for t in df['tdate']]
	df['ticker'] = ticker
	print("crawl completed: ", ticker)

	return df


def get_af_ticker():
	url = r'https://finance.naver.com/fund/fundFinderList.nhn?search=&sortOrder=&page='
	date_list = []
	ticker_list = []
	name_list = []

	for i in range(1, 401):
		url_ = url + str(i)
		resp = try_urlopen(url_)
		soup = bs(resp.read(), features="lxml")

		tbodys = soup.find_all('tbody')
		d_list = []
		for i in range(1, len(tbodys)):
			d = tbodys[i].text.replace('\n', '').split('설정일')[1].split('유형')[0]
			d_list.append(d)

		if len(d_list) > 0:
			date_list.extend(d_list)
			link_list = [a['href'] for a in soup.find_all('a', href=True) if a.text]
			link_list = [l.split('=')[1] for l in link_list if "/fund/fundDetail.nhn?fundCd=" in l]
			ticker_list.extend(link_list)
			target_list = [a.text for a in soup.find_all('a', href=True) if "/fund/fundDetail.nhn?fundCd=" in a['href']]
			name_list.extend(target_list)
		else:
			break

	df_ticker = pd.DataFrame(zip(date_list, ticker_list, name_list))
	df_ticker.columns = ['sdate', 'ticker', 'name']
	df_ticker['sdate'] = pd.to_datetime(df_ticker['sdate'], format='%Y.%m.%d')

	t = datetime.today()
	y, m = t.year, t.month

	df_ticker['n_months'] = [(y - d.year)*12 + (m - d.month) for d in df_ticker['sdate']]
	df_ticker = df_ticker.set_index('sdate')

	return df_ticker