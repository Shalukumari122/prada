import pymysql
import scrapy
from scrapy.cmdline import execute

from prada.items import PralinkItem


class ProductPageSpider(scrapy.Spider):
    name = "product_page"
    # allowed_domains = ["'google.com'"]
    # start_urls = ["https://'google.com'"]

    def __init__(self):
        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='actowiz',
            database='prada_db',
            # cursorclass=pymysql.cursor.Dictcurosr
        )
        self.cursor = self.conn.cursor()


    def start_requests(self):
        query = f"SELECT * FROM states_store_locator where  status='pending'"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            state = row[1]
            lat=row[2]
            lon=row[3]

            cookies = {
                'PRADACONSENT_V3': 'c1:0%7Cc3:0%7Cc6:0%7Cc9:1%7Cts:1727241188809%7Cconsent:true%7Cid:01922798f5c8009c69320a27b0b80506f002e067007e8',
                'api-client-correlation-id': 'a6e3f8e1-5ba9-4013-a88a-23b8270b650d',
                'AWSELB': 'D36DB76308D92364DA26ECCA64ECE8DCDE7B8274473AB4E05FC4E0BDDDF0BD3A16C1518FCB37F6C5FE4293736FC3D38D7EA6F72E3334E9B7B1EEA2523BBC6A0D2E43622E7E',
                'AWSELBCORS': 'D36DB76308D92364DA26ECCA64ECE8DCDE7B8274473AB4E05FC4E0BDDDF0BD3A16C1518FCB37F6C5FE4293736FC3D38D7EA6F72E3334E9B7B1EEA2523BBC6A0D2E43622E7E',
                'WCTokens_base': '%7Ba192d0c876f501617c56fe75c966b8f25162740cea0794f8c43ee08928fd06952be7bd5b72f26f37a643670d8d56f415667a7b012f8fe62e55ccf74d28842c0633a6cd209242d66fa97100cc6179b82ed69ebb8d7942e1a5fc8fc104511ea2911a472db25551b16f97ab8926888b8648dadb081dfb883ad1e477274d91853f07f6f31f6d98636fddd0647373bb5f53e7b7906161557342f036b3f74a494bfd1ed7ae80ed12fca0c7ad7910842b9d449fa46d71f0a5a9fbd39bc27b7dfcc25c730e8f6048b6315d387c59d30440e3fc739b2c4c92e691baaf699869a2f370e49bd2918cbcd07a66c0ad0a21cbc60c1eeb8b13a3e0b0b5640c078a12869626e4245cf6677c2c4182fa1e1faa21b4b5a4d595f32d1715a69c3eb85a6eeeedee1eb09f0f20b634bc5c7795e5ad842e52df36ed186ddc145ff9993121c97debe91807725df2e92b8d54eefaf3982c149a02509d0dab1b35f1dbdc882d8140d607ad681c41165dd78278e22f46cf8cc0a91f52cbd9d140784ea7cfb3e16f1a9f2411b4a4844b961ba0f0015039f9b15e59e06f0e68a6ad3667e69f82ec4caf3bc57560122ac3dfa95c115f4005e5c4867a70693c160a42073560d098698470e418738764117e5ba324d1c7fe0e981afb3ec52ecd4658539f0c51dc9174ca9541642dad88bb801d6d2b36e39bac3348004f2b7b3f461bdd380771db87dac3f0dcb0bf8156799ca4a764e3c21b584e792c94dcc20036a17db2cb19df6f7f207774f8d97a5c29ca403e0357c66078b6072d3b10ea%7D',
                'WC_TOKEN': '1727186527040',
                'cookie_mobile': 'c',
                'cookie_banner_v2': 'b',
                'cookie_desk': 'a',
                'cookie_banner_us': 'c',
                'AMCVS_89B51D4B55B90FBA7F000101%40AdobeOrg': '1',
                'kndctr_89B51D4B55B90FBA7F000101_AdobeOrg_identity': 'CiY4MzE3NDYyMjY5MDYwOTQzMTMzNDU2NjM3MTQ4MTM3NDExNjMyM1IRCLPm26KiMhgBKgRJTkQxMAPwAbPm26KiMg==',
                '_abck': '3329F982B2DAE4EA8711A064B7748CCA~0~YAAQR3LBFwm3hx6SAQAAA+R5JwyRPdXd/D6LCDOThioDQYyRD/M6hmBtKKMG3xuVF9QEZNHj2IspnV9X8Q1tYK7jkrrT1w1Ri6FuMEj2fhON3ZApArZTEJIEC+8cash4QoxnLpt8V8bsfEX1Qds49ERzTfzWjtrCTKL8XFq6d9xQrSfOzxPbZBUzOciZnuIIeepX7kLX9XHw4iwP/tAVQJ5fgHm379WsF3h6itTfJLntTeVQ/OvJlVFNDhnytJMw2z9/AbawVoUEsQkh1c18Y2FNd8wWQPjNTGWxv30B94ZzeTtVYFWPR/A1h+O14a3ye0amAn7bk4JitpVN5VxXY9kP8UKmLOKTGsIdpGLyT2aiYXJff1J1DaBi7vF4t8qGHoD4imohAmcHod1HKqajFhn1A+j8rCT/thJ+IFpW4pytDwAMzc6uR3XTP/E+Y8Pfa/yxeKgLcjM=~-1~-1~-1',
                'WC_JSESSIONID': '%7B%22WC_JSESSIONID%22%3A%220000IIPSSQptuxvrb2Figd-25Sm%3Awcsprod1%22%7D',
                'ak_bmsc': '6E1C746704391EB289AD90868449DD9C~000000000000000000000000000000~YAAQR3LBFxe7hx6SAQAAW/t5Jxmg5d+LV3ghXF0yWzyJEPLaCvRr06qK7PTNgYh3HrwPBu1zXoaaFV+g2fURa+Dgm8YD1XtEFc6Zg6uggmxKKVPaa0kL3zIjEqU6CHCaxJ8Sn4n2SfC4aFZw1vAi7VcvZjLKjQYk+OGCphAeO5aJbETW0u3YKHNKpRLlmuupSO2Ifzyupjp5IHyRnJQRvIzKBFvwSiTDRTWsblbkGQ9dX/MaCldM9RHDckV/Y8XsMbwUMmlm8NPxjDAcnR9nq+t8Dbxp1nknVwFIV24iLUsihkoneX/reoA95m4h8qJBHJCH+afE+4Ue9P2YkHbqrZBb+L1EWvdwzki7rlZVQbBCLXeqNkEu6ITVxDFwh5z15PZStMGuDYAusea78/bDPdr6+LO+9OC39G8ERF0DHwGgBCVOgAdvf2t1nfrazojU0IAzYfaIJCJHB9nq',
                'AMCV_89B51D4B55B90FBA7F000101%40AdobeOrg': '1585540135%7CMCMID%7C83174622690609431334566371481374116323%7CMCAAMLH-1727843954%7C12%7CMCAAMB-1727843954%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1727246354s%7CNONE%7CvVersion%7C4.4.0%7CMCIDTS%7C19991%7CMCAID%7CNONE',
                'kndctr_89B51D4B55B90FBA7F000101_AdobeOrg_cluster': 'ind1',
                'gig_canary': 'false',
                'gig_canary_ver': '16460-3-28787355',
                'gig_bootstrap_4_d3XaNbaSuPQZ-KGlC7MUiA': 'cdc_ver4',
                'stimgs': '{%22sessionId%22:11409503%2C%22didReportCameraImpression%22:false%2C%22newUser%22:true}',
                'syte_uuid': 'd1a196b0-7afc-11ef-a691-119f32742864',
                'bm_sz': 'B59D872F34AA7B8BCB0DA8A291904AC8~YAAQR3LBF1hSlB6SAQAAyOmuJxn9rKkH/DoyWqzaQH0+6qdywNToONGvL6yDWfmqlfMuwIf+vOaHgr2uauezJWJ8FJJdp8Llvf0UuD8+1TecAWsyPLe5+9mmqbfVfvG7hEnQABOmwh4Lu4QaeSVczjSLe/6pewYuGV7LnaTZbZADmJ1WKexlpDJAEGrpsKNeKlcnhZeCmpbqheMFu5Ek02bfJgT49y+nFzsnudW8Fuaciys5KQD7Pg7Fy2rdCoxawHjIztCjzytdBKWuVKgnC4Zd1tnVdzO08Jja5F3kq1XgEgfqFIycst1u3xqao1sZOlk5pcSMlB+O/BbxT7OHjWw/n2UP2enY8Z3dZrMIIL1L3u9Q24B/BsJ2pbU2SjfBilt7BA2T7TBROAJnXE+j8pmGrX6VFsuoureaeitFaI8jlXxLP7O/8EArSS+78ZxFW4wwWAO0cxV4cO0u5krC~3750211~3555634',
                'utag_main': '_sn:2$_se:24%3Bexp-session$_ss:0%3Bexp-session$_st:1727244576737%3Bexp-session$ses_id:1727239152121%3Bexp-session$_pn:7%3Bexp-session$vapi_domain:prada.com',
                'websdk_prev_page': 'ca:search noresult',
                'aa_prev_type': 'search',
                'bm_sv': 'B83919627FC5EB35ECB9303DA29D646D~YAAQxvXSF0HHVeiRAQAAMX20JxmL7azg1JnCixPy5+NrM9fI8fOIMLTlCX1GOmFUEFd4/JMldflKJ5q3bIilCKOgj/J5MFuSqaiuXYOuB1LbyVZN6yY2TCPELTcM+BvvCZW+YbYqaLaLRMAEJlqhYa9hKBeEAzrbCruTFSgMol51WrxPbfcoWKPcWsw/ssHWAn1VYdMsJMzs+Gtur3KLcsBx5N3IoGKADsuv54oh729UV43GtblGQb73KZoZ5R43~1',
                'RT': '"z=1&dm=prada.com&si=4a77f349-ee70-4e57-a91b-622ad44df9d1&ss=m1hdlayh&sl=e&tt=135m&bcn=%2F%2F684d0d4c.akstat.io%2F&ul=2pnqu"',
            }

            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'cache-control': 'no-cache',
                # 'cookie': 'PRADACONSENT_V3=c1:0%7Cc3:0%7Cc6:0%7Cc9:1%7Cts:1727241188809%7Cconsent:true%7Cid:01922798f5c8009c69320a27b0b80506f002e067007e8; api-client-correlation-id=a6e3f8e1-5ba9-4013-a88a-23b8270b650d; AWSELB=D36DB76308D92364DA26ECCA64ECE8DCDE7B8274473AB4E05FC4E0BDDDF0BD3A16C1518FCB37F6C5FE4293736FC3D38D7EA6F72E3334E9B7B1EEA2523BBC6A0D2E43622E7E; AWSELBCORS=D36DB76308D92364DA26ECCA64ECE8DCDE7B8274473AB4E05FC4E0BDDDF0BD3A16C1518FCB37F6C5FE4293736FC3D38D7EA6F72E3334E9B7B1EEA2523BBC6A0D2E43622E7E; WCTokens_base=%7Ba192d0c876f501617c56fe75c966b8f25162740cea0794f8c43ee08928fd06952be7bd5b72f26f37a643670d8d56f415667a7b012f8fe62e55ccf74d28842c0633a6cd209242d66fa97100cc6179b82ed69ebb8d7942e1a5fc8fc104511ea2911a472db25551b16f97ab8926888b8648dadb081dfb883ad1e477274d91853f07f6f31f6d98636fddd0647373bb5f53e7b7906161557342f036b3f74a494bfd1ed7ae80ed12fca0c7ad7910842b9d449fa46d71f0a5a9fbd39bc27b7dfcc25c730e8f6048b6315d387c59d30440e3fc739b2c4c92e691baaf699869a2f370e49bd2918cbcd07a66c0ad0a21cbc60c1eeb8b13a3e0b0b5640c078a12869626e4245cf6677c2c4182fa1e1faa21b4b5a4d595f32d1715a69c3eb85a6eeeedee1eb09f0f20b634bc5c7795e5ad842e52df36ed186ddc145ff9993121c97debe91807725df2e92b8d54eefaf3982c149a02509d0dab1b35f1dbdc882d8140d607ad681c41165dd78278e22f46cf8cc0a91f52cbd9d140784ea7cfb3e16f1a9f2411b4a4844b961ba0f0015039f9b15e59e06f0e68a6ad3667e69f82ec4caf3bc57560122ac3dfa95c115f4005e5c4867a70693c160a42073560d098698470e418738764117e5ba324d1c7fe0e981afb3ec52ecd4658539f0c51dc9174ca9541642dad88bb801d6d2b36e39bac3348004f2b7b3f461bdd380771db87dac3f0dcb0bf8156799ca4a764e3c21b584e792c94dcc20036a17db2cb19df6f7f207774f8d97a5c29ca403e0357c66078b6072d3b10ea%7D; WC_TOKEN=1727186527040; cookie_mobile=c; cookie_banner_v2=b; cookie_desk=a; cookie_banner_us=c; AMCVS_89B51D4B55B90FBA7F000101%40AdobeOrg=1; kndctr_89B51D4B55B90FBA7F000101_AdobeOrg_identity=CiY4MzE3NDYyMjY5MDYwOTQzMTMzNDU2NjM3MTQ4MTM3NDExNjMyM1IRCLPm26KiMhgBKgRJTkQxMAPwAbPm26KiMg==; _abck=3329F982B2DAE4EA8711A064B7748CCA~0~YAAQR3LBFwm3hx6SAQAAA+R5JwyRPdXd/D6LCDOThioDQYyRD/M6hmBtKKMG3xuVF9QEZNHj2IspnV9X8Q1tYK7jkrrT1w1Ri6FuMEj2fhON3ZApArZTEJIEC+8cash4QoxnLpt8V8bsfEX1Qds49ERzTfzWjtrCTKL8XFq6d9xQrSfOzxPbZBUzOciZnuIIeepX7kLX9XHw4iwP/tAVQJ5fgHm379WsF3h6itTfJLntTeVQ/OvJlVFNDhnytJMw2z9/AbawVoUEsQkh1c18Y2FNd8wWQPjNTGWxv30B94ZzeTtVYFWPR/A1h+O14a3ye0amAn7bk4JitpVN5VxXY9kP8UKmLOKTGsIdpGLyT2aiYXJff1J1DaBi7vF4t8qGHoD4imohAmcHod1HKqajFhn1A+j8rCT/thJ+IFpW4pytDwAMzc6uR3XTP/E+Y8Pfa/yxeKgLcjM=~-1~-1~-1; WC_JSESSIONID=%7B%22WC_JSESSIONID%22%3A%220000IIPSSQptuxvrb2Figd-25Sm%3Awcsprod1%22%7D; ak_bmsc=6E1C746704391EB289AD90868449DD9C~000000000000000000000000000000~YAAQR3LBFxe7hx6SAQAAW/t5Jxmg5d+LV3ghXF0yWzyJEPLaCvRr06qK7PTNgYh3HrwPBu1zXoaaFV+g2fURa+Dgm8YD1XtEFc6Zg6uggmxKKVPaa0kL3zIjEqU6CHCaxJ8Sn4n2SfC4aFZw1vAi7VcvZjLKjQYk+OGCphAeO5aJbETW0u3YKHNKpRLlmuupSO2Ifzyupjp5IHyRnJQRvIzKBFvwSiTDRTWsblbkGQ9dX/MaCldM9RHDckV/Y8XsMbwUMmlm8NPxjDAcnR9nq+t8Dbxp1nknVwFIV24iLUsihkoneX/reoA95m4h8qJBHJCH+afE+4Ue9P2YkHbqrZBb+L1EWvdwzki7rlZVQbBCLXeqNkEu6ITVxDFwh5z15PZStMGuDYAusea78/bDPdr6+LO+9OC39G8ERF0DHwGgBCVOgAdvf2t1nfrazojU0IAzYfaIJCJHB9nq; AMCV_89B51D4B55B90FBA7F000101%40AdobeOrg=1585540135%7CMCMID%7C83174622690609431334566371481374116323%7CMCAAMLH-1727843954%7C12%7CMCAAMB-1727843954%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1727246354s%7CNONE%7CvVersion%7C4.4.0%7CMCIDTS%7C19991%7CMCAID%7CNONE; kndctr_89B51D4B55B90FBA7F000101_AdobeOrg_cluster=ind1; gig_canary=false; gig_canary_ver=16460-3-28787355; gig_bootstrap_4_d3XaNbaSuPQZ-KGlC7MUiA=cdc_ver4; stimgs={%22sessionId%22:11409503%2C%22didReportCameraImpression%22:false%2C%22newUser%22:true}; syte_uuid=d1a196b0-7afc-11ef-a691-119f32742864; bm_sz=B59D872F34AA7B8BCB0DA8A291904AC8~YAAQR3LBF1hSlB6SAQAAyOmuJxn9rKkH/DoyWqzaQH0+6qdywNToONGvL6yDWfmqlfMuwIf+vOaHgr2uauezJWJ8FJJdp8Llvf0UuD8+1TecAWsyPLe5+9mmqbfVfvG7hEnQABOmwh4Lu4QaeSVczjSLe/6pewYuGV7LnaTZbZADmJ1WKexlpDJAEGrpsKNeKlcnhZeCmpbqheMFu5Ek02bfJgT49y+nFzsnudW8Fuaciys5KQD7Pg7Fy2rdCoxawHjIztCjzytdBKWuVKgnC4Zd1tnVdzO08Jja5F3kq1XgEgfqFIycst1u3xqao1sZOlk5pcSMlB+O/BbxT7OHjWw/n2UP2enY8Z3dZrMIIL1L3u9Q24B/BsJ2pbU2SjfBilt7BA2T7TBROAJnXE+j8pmGrX6VFsuoureaeitFaI8jlXxLP7O/8EArSS+78ZxFW4wwWAO0cxV4cO0u5krC~3750211~3555634; utag_main=_sn:2$_se:24%3Bexp-session$_ss:0%3Bexp-session$_st:1727244576737%3Bexp-session$ses_id:1727239152121%3Bexp-session$_pn:7%3Bexp-session$vapi_domain:prada.com; websdk_prev_page=ca:search noresult; aa_prev_type=search; bm_sv=B83919627FC5EB35ECB9303DA29D646D~YAAQxvXSF0HHVeiRAQAAMX20JxmL7azg1JnCixPy5+NrM9fI8fOIMLTlCX1GOmFUEFd4/JMldflKJ5q3bIilCKOgj/J5MFuSqaiuXYOuB1LbyVZN6yY2TCPELTcM+BvvCZW+YbYqaLaLRMAEJlqhYa9hKBeEAzrbCruTFSgMol51WrxPbfcoWKPcWsw/ssHWAn1VYdMsJMzs+Gtur3KLcsBx5N3IoGKADsuv54oh729UV43GtblGQb73KZoZ5R43~1; RT="z=1&dm=prada.com&si=4a77f349-ee70-4e57-a91b-622ad44df9d1&ss=m1hdlayh&sl=e&tt=135m&bcn=%2F%2F684d0d4c.akstat.io%2F&ul=2pnqu"',
                'pragma': 'no-cache',
                'priority': 'u=0, i',
                'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'none',
                'sec-fetch-user': '?1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
            }

            params = {
                'location': 'Alabama, USA',
                'lat': '32.3182314',
                'lng': '-86.902298',
            }

            yield scrapy.Request(url=f'https://www.prada.com/ca/en/store-locator.html?location={state}%2C+USA&lat={lat}&lng={lon}', cookies=cookies,
                                    headers=headers,callback=self.parse,
                                 meta={'state':state})

    def parse(self, response):
        item=PralinkItem()
        links=response.xpath('//div[@class="d-none"]/a/@href').extract()
        for each_link in links:
            link=each_link
            item['link']=link
            item['state']=response.request.meta['state']

            yield item

        try:

            update_query = "UPDATE states_store_locator SET status = 'Done' WHERE state = %s"
            self.cursor.execute(update_query, response.request.meta['state'])
            self.conn.commit()

        except Exception as e:
            print(f"Error updating states_store_locator: {e}")


if __name__ == '__main__':
    execute('scrapy crawl product_page'.split())
