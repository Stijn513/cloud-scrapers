########################################################
# Funda Crawler 
# By Stijn de Ruijter 
# December 2020
########################################################
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
import json 

class Funda(scrapy.Spider):
    name = 'funda'

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-NL,en;q=0.9,nl-NL;q=0.8,nl;q=0.7,en-US;q=0.6',
        'cache-control': 'max-age=0',
        'dnt': '1',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'
        }
    
    custom_settings = {
        'FEED_FORMAT': 'csv',
        'FEED_URI': 'alkmaar.csv' ,
        'LOG_FILE': 'houses_funda1.log',
        'CONCURRENT_REQUESTS_PER_DOMAIN':1,
        'DOWNLOADER_MIDDLEWARES': {'scrapy_crawlera.CrawleraMiddleware': 610},
        'CRAWLERA_ENABLED': True,
        'CRAWLERA_APIKEY': '9d1fd32d73094e0999120802d5457989' 

    } 
    def start_requests(self):
        stad = 'alkmaar' # if city no capitals
        
        url = 'https://www.funda.nl/en/koop/' + stad + '/beschikbaar/0-275000/50+woonopp/bouwperiode-1960-1970/bouwperiode-1971-1980/bouwperiode-1981-1990/bouwperiode-1991-2000/bouwperiode-2001-2010/bouwperiode-na-2010/'        
        yield scrapy.Request(url=url, headers=self.headers, callback=self.parse)

    def parse(self, response):
        for link in response.css('div[class="search-result__header-title-col"] *::attr(href)').extract():
            next_url = 'https://www.funda.nl' + link
            yield response.follow(url = next_url, headers=self.headers, callback=self.parse_detail)
        
        next_page = 'https://www.funda.nl' + response.css('a[rel="next"] *::attr(href)').extract()[0]
        if next_page:
            yield response.follow(url = next_page, callback=self.parse, headers=self.headers )
    
    def parse_detail(self, response):
        
        #property features
        features = {            
            'url': response.url,
            
            'address': response.css('span[class="object-header__title"]::text').extract()[0],
            
            'postal_code': response.css('span[class="object-header__subtitle fd-color-dark-3"]::text').extract()[0][:7],
            
            'town': response.css('span[class="fd-text--ellipsis fd-text--nowrap fd-overflow-hidden"]::text').extract()[-2],
        
            'price': response.css('strong[class="object-header__price"]::text').extract()[0].replace(',', '').strip('k.k.').strip('€').replace('.', '').strip(),
            
            'neighborhood': response.css('span[class="fd-text--ellipsis fd-text--nowrap fd-overflow-hidden"]::text').extract()[-1],
        
            #'floor_area': response.css('span[class="kenmerken-highlighted__value fd-text--nowrap"]::text').extract()[0].strip(' m²'),
        
            #'ground_area': response.css('span[class="kenmerken-highlighted__value fd-text--nowrap"]::text').extract()[1].strip(' m²'),
                     
            'agent': response.css('.object-contact-aanbieder-link::text').extract()[0],
             
            'construction': [],
            
            'transfer': [],
            
            'surface':[],
            
            'layout':[],
            
            'energy': [],   
            
            'cadastral': [],
            
            'exterior_space': [],
            
            'garage':[],
            
            'parking':[]
            }

        headers = response.css('h3[class="object-kenmerken-list-header"]')
        
        # Ownership extraction 
        try:
            #key features
            key_features = {}
            
            # init constuction in index
            construction_index = None
            
            for index in range(0, len(headers)):
                if headers[index].css('::text').get() == 'Transfer of ownership':
                    construction_index = index
                    
            #extract features selector
            feature_selector = response.css('h3[class="object-kenmerken-list-header"] + dl[class="object-kenmerken-list"]')
            feature_selector = feature_selector[construction_index]
            
            #extract feature keys
            feature_keys = list(filter(None, [
                val.replace('\n', '').strip()
                for val in 
                Selector(text=feature_selector.get()).css('dt::text').getall()
                ]))
            
            #extract features values
            feature_vals = list(filter(None, [
                val.replace('\n', '').strip()
                for val in 
                Selector(text=feature_selector.get()).css('dd::text').getall()
                ]))
            
            #combinekeys and values into a dictionary
            for index in range(0, len(feature_vals)):
                key_features[feature_keys[index]] = feature_vals[index]
                
            #store key features
            
            features['transfer'] = [key_features]
            

        ## Construction
        except:
            pass
        # key feature extraction 
        try:
            #key features
            key_features = {}
            
            # init constuction in index
            construction_index = None
            
            for index in range(0, len(headers)):
                if headers[index].css('::text').get() == 'Construction':
                    construction_index = index
                    
            #extract features selector
            feature_selector = response.css('h3[class="object-kenmerken-list-header"] + dl[class="object-kenmerken-list"]')
            feature_selector = feature_selector[construction_index]
            
            #extract feature keys
            feature_keys = list(filter(None, [
                val.replace('\n', '').strip()
                for val in 
                Selector(text=feature_selector.get()).css('dt::text').getall()
                ]))
            
            #extract features values
            feature_vals = list(filter(None, [
                val.replace('\n', '').strip()
                for val in 
                Selector(text=feature_selector.get()).css('dd::text').getall()
                ]))
            
            #combinekeys and values into a dictionary
            for index in range(0, len(feature_vals)):
                key_features[feature_keys[index]] = feature_vals[index]
                
            #store key features
            
            features['construction'] = [key_features]
            
        except:
            pass
        
        ## Surface areas and volume
        try:
            #key features
            key_features = {}
            
            # init constuction in index
            construction_index = None
            
            for index in range(0, len(headers)):
                if headers[index].css('::text').get() == 'Surface areas and volume':
                    construction_index = index
                    
            #extract features selector
            feature_selector = response.css('h3[class="object-kenmerken-list-header"] + dl[class="object-kenmerken-list"]')
            feature_selector = feature_selector[construction_index]
            
            #extract feature keys
            feature_keys = list(filter(None, [
                val.replace('\n', '').strip()
                for val in 
                Selector(text=feature_selector.get()).css('dt::text').getall()
                ]))
            
            #extract features values
            feature_vals = list(filter(None, [
                val.replace('\n', '').strip()
                for val in 
                Selector(text=feature_selector.get()).css('dd::text').getall()
                ]))
            
            #combinekeys and values into a dictionary
            for index in range(0, len(feature_vals)):
                key_features[feature_keys[index]] = feature_vals[index]
                
            #store key features
            
            features['surface'] = [key_features]
            
        except:
            pass

        ## Layout
        try:
            #key features
            key_features = {}
            
            # init constuction in index
            construction_index = None
            
            for index in range(0, len(headers)):
                if headers[index].css('::text').get() == 'Layout':
                    construction_index = index
                    
            #extract features selector
            feature_selector = response.css('h3[class="object-kenmerken-list-header"] + dl[class="object-kenmerken-list"]')
            feature_selector = feature_selector[construction_index]
            
            #extract feature keys
            feature_keys = list(filter(None, [
                val.replace('\n', '').strip()
                for val in 
                Selector(text=feature_selector.get()).css('dt::text').getall()
                ]))
            
            #extract features values
            feature_vals = list(filter(None, [
                val.replace('\n', '').strip()
                for val in 
                Selector(text=feature_selector.get()).css('dd::text').getall()
                ]))
            
            #combinekeys and values into a dictionary
            for index in range(0, len(feature_vals)):
                key_features[feature_keys[index]] = feature_vals[index]
                
            #store key features
            
            features['layout'] = [key_features]
            
        except:
            pass          
        
        ## Energy extraction
        try:
            #key features
            key_features = {}
            
            # init constuction in index
            construction_index = None
            
            for index in range(0, len(headers)):
                if headers[index].css('::text').get() == 'Energy':
                    construction_index = index
                    
            #extract features selector
            feature_selector = response.css('h3[class="object-kenmerken-list-header"] + dl[class="object-kenmerken-list"]')
            feature_selector = feature_selector[construction_index]
            
            #extract feature keys
            feature_keys = list(filter(None, [
                val.replace('\n', '').strip()
                for val in 
                Selector(text=feature_selector.get()).css('dt::text').getall()
                ]))
            
            #extract features values
            feature_vals = list(filter(None, [
                val.replace('\n', '').strip()
                for val in 
                Selector(text=feature_selector.get()).css('dd::text').getall()
                ]))
            
            #combinekeys and values into a dictionary
            for index in range(0, len(feature_vals)):
                key_features[feature_keys[index]] = feature_vals[index]
                
            #store key features
            
            features['energy'] = [key_features]
            
        except:
            pass

        #extract Cadastral data
        try:
            #key features
            key_features = {}
            
            # init constuction in index
            construction_index = None
            
            for index in range(0, len(headers)):
                if headers[index].css('::text').get() == 'Cadastral data':
                    construction_index = index
                    
            #extract features selector
            feature_selector = response.css('h3[class="object-kenmerken-list-header"] + dl[class="object-kenmerken-list"]')
            feature_selector = feature_selector[construction_index]
            
            #extract feature keys
            feature_keys = list(filter(None, [
                val.replace('\n', '').strip()
                for val in 
                Selector(text=feature_selector.get()).css('dt::text').getall()
                ]))
            
            #extract features values
            feature_vals = list(filter(None, [
                val.replace('\n', '').strip()
                for val in 
                Selector(text=feature_selector.get()).css('dd::text').getall()
                ]))
            
            #combinekeys and values into a dictionary
            for index in range(0, len(feature_vals)):
                key_features[feature_keys[index]] = feature_vals[index]
                
            #store key features
            
            features['cadastral'] = [key_features]
            
        except:
            pass
        
        
        # Extract Exterior space
        try:
            #key features
            key_features = {}
            
            # init constuction in index
            construction_index = None
            
            for index in range(0, len(headers)):
                if headers[index].css('::text').get() == 'Exterior space':
                    construction_index = index
                    
            #extract features selector
            feature_selector = response.css('h3[class="object-kenmerken-list-header"] + dl[class="object-kenmerken-list"]')
            feature_selector = feature_selector[construction_index]
            
            #extract feature keys
            feature_keys = list(filter(None, [
                val.replace('\n', '').strip()
                for val in 
                Selector(text=feature_selector.get()).css('dt::text').getall()
                ]))
            
            #extract features values
            feature_vals = list(filter(None, [
                val.replace('\n', '').strip()
                for val in 
                Selector(text=feature_selector.get()).css('dd::text').getall()
                ]))
            
            #combinekeys and values into a dictionary
            for index in range(0, len(feature_vals)):
                key_features[feature_keys[index]] = feature_vals[index]
                
            #store key features
            
            features['exterior_space'] = [key_features]
        
        except:
            pass

        #Extract Garage
        try:
            #key features
            key_features = {}
            
            # init constuction in index
            construction_index = None
            
            for index in range(0, len(headers)):
                if headers[index].css('::text').get() == 'Garage':
                    construction_index = index
                    
            #extract features selector
            feature_selector = response.css('h3[class="object-kenmerken-list-header"] + dl[class="object-kenmerken-list"]')
            feature_selector = feature_selector[construction_index]
            
            #extract feature keys
            feature_keys = list(filter(None, [
                val.replace('\n', '').strip()
                for val in 
                Selector(text=feature_selector.get()).css('dt::text').getall()
                ]))
            
            #extract features values
            feature_vals = list(filter(None, [
                val.replace('\n', '').strip()
                for val in 
                Selector(text=feature_selector.get()).css('dd::text').getall()
                ]))
            
            #combinekeys and values into a dictionary
            for index in range(0, len(feature_vals)):
                key_features[feature_keys[index]] = feature_vals[index]
                
            #store key features
            
            features['garage'] = [key_features]
        
        except:
            pass

        #Extract Parking                
        try:
            #key features
            key_features = {}
            
            # init constuction in index
            construction_index = None
            
            for index in range(0, len(headers)):
                if headers[index].css('::text').get() == 'Parking':
                    construction_index = index
                    
            #extract features selector
            feature_selector = response.css('h3[class="object-kenmerken-list-header"] + dl[class="object-kenmerken-list"]')
            feature_selector = feature_selector[construction_index]
            
            #extract feature keys
            feature_keys = list(filter(None, [
                val.replace('\n', '').strip()
                for val in 
                Selector(text=feature_selector.get()).css('dt::text').getall()
                ]))
            
            #extract features values
            feature_vals = list(filter(None, [
                val.replace('\n', '').strip()
                for val in 
                Selector(text=feature_selector.get()).css('dd::text').getall()
                ]))
            
            #combinekeys and values into a dictionary
            for index in range(0, len(feature_vals)):
                key_features[feature_keys[index]] = feature_vals[index]
                
            features['parking'] = [key_features]
        except:
            pass        
        
        yield features
        
# run spider
process =  CrawlerProcess()
process.crawl(Funda)
process.start()

    
