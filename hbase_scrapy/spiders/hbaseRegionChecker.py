#! /usr/bin/env python
# -*- coding: utf-8 -*-
## this is a spider to monitor for hbase region state is not good on master pages
import argparse
import scrapy
from scrapy import Spider
import scrapy.spiderloader
import scrapy.statscollectors
import scrapy.logformatter
import scrapy.dupefilters
import scrapy.squeues
import scrapy.extensions.spiderstate
import scrapy.extensions.corestats
import scrapy.extensions.telnet
import scrapy.extensions.logstats
import scrapy.extensions.memusage
import scrapy.extensions.memdebug
import scrapy.extensions.feedexport
import scrapy.extensions.closespider
import scrapy.extensions.debug
import scrapy.extensions.httpcache
import scrapy.extensions.statsmailer
import scrapy.extensions.throttle
import scrapy.core.scheduler
import scrapy.core.engine
import scrapy.core.scraper
import scrapy.core.spidermw
import scrapy.core.downloader
import scrapy.downloadermiddlewares.stats
import scrapy.downloadermiddlewares.httpcache
import scrapy.downloadermiddlewares.cookies
import scrapy.downloadermiddlewares.useragent
import scrapy.downloadermiddlewares.httpproxy
import scrapy.downloadermiddlewares.ajaxcrawl
#import scrapy.downloadermiddlewares.chunked
import scrapy.downloadermiddlewares.decompression
import scrapy.downloadermiddlewares.defaultheaders
import scrapy.downloadermiddlewares.downloadtimeout
import scrapy.downloadermiddlewares.httpauth
import scrapy.downloadermiddlewares.httpcompression
import scrapy.downloadermiddlewares.redirect
import scrapy.downloadermiddlewares.retry
import scrapy.downloadermiddlewares.robotstxt
import scrapy.spidermiddlewares.depth
import scrapy.spidermiddlewares.httperror
import scrapy.spidermiddlewares.offsite
import scrapy.spidermiddlewares.referer
import scrapy.spidermiddlewares.urllength
import scrapy.pipelines
import scrapy.core.downloader.handlers.http
import scrapy.core.downloader.contextfactory
from scrapy.crawler import CrawlerProcess
import os
result_file = 'scrapy.result'
if os.path.exists(result_file):
    os.remove(result_file)
parser = argparse.ArgumentParser(description="useage eg: \n  --url http://tempt22.ops.lycc.qihoo.net:16010 --name hbaseSpiderTest")
base_name = 'hbaseSpider'
parser.add_argument('--url', required=False  , type=str)
parser.add_argument('--name', required=False  , type=str,default='hbaseSpiderTest')
arg = parser.parse_args()
base_url = 'http://localhost:16010'
if arg.url:
    base_url = arg.url
if arg.name:
    base_name = arg.name
class hbaseSpider(Spider):
    name=base_name
    # start_url = 'http://tempt22.ops.lycc.qihoo.net:16010/master-status'
    url = base_url + '/tablesDetailed.jsp'
    table_url = base_url + '/table.jsp?name='
    table_path = 'a[href*="table.jsp?name="]::text'
    context_path = '#regionServerDetailsTable tbody tr'
    td_value = 'td::text'
    td_value_t = 'td'
    td_value_v = '::text'
    url_domains = [ url,]
    table_region_name_offset = 0
    table_region_read_offset = 2
    table_region_write_offset = 3
    table_region_storefile_size_offset = 4
    table_region_storefile_num_offset = 5
    table_region_memory_size_offset = 6
    table_region_start_key_offset = 8
    table_region_end_key_offset = 9
    overlap = "suspected overlap region :%s"
    offline = "suspected closed region :%s"
    enable_table_css = '.container-fluid .table-striped'
    enable_table_value = 'tr:nth-child(2) td:nth-child(2)::text'
    default_none = ''
    default_zero = '0'

    def start_requests(self):
        #self.logger.isEnabledFor(scrapy.log.ERROR)
        for url in self.url_domains:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self,response):
        ##  we need table names and get the table url
        tables = response.css(self.table_path)
        for table in tables :
            yield response.follow( (self.table_url +table.extract().strip()) , self.parse_table)##  here we need to do second parse

    def parse_table(self,response):
        ###  skip disabled
        enable_table = response.css(self.enable_table_css)
        enable_flag = enable_table[0].css(self.enable_table_value).get(default='').strip()
        if (enable_flag == 'false'):
            ### do nothing
            return
        regions = response.css(self.context_path)

        def extract_with_css(item,offset):
            return item[offset].css(self.td_value_v).get(default=self.default_none)
        last_start_key=None
        last_end_key=None
        last_region = None
        with open(result_file, 'a+') as file:
            for region in regions:
                temp = region.css(self.td_value_t)
                t_region = extract_with_css(temp,self.table_region_name_offset)
                read_request = extract_with_css(temp,self.table_region_read_offset)
                write_request = extract_with_css(temp,self.table_region_write_offset)
                storefile_size = extract_with_css(temp,self.table_region_storefile_size_offset)
                storefile_num = extract_with_css(temp,self.table_region_storefile_num_offset)
                memory_siz = extract_with_css(temp,self.table_region_memory_size_offset)
                start_key = extract_with_css(temp,self.table_region_start_key_offset)
                end_key = extract_with_css(temp,self.table_region_end_key_offset)
            ## every table we need the startkey and endkey , their endkeys are same with the start key
                if (( start_key == last_start_key ) or ( end_key == last_end_key)):
                    file.write(self.overlap % last_region +'\n')
                elif ( read_request==self.default_zero and  write_request == self.default_zero and  storefile_num == self.default_zero  ) :
                    ## this part will be write to the
                    file.write( self.offline % t_region + "\n")
                last_start_key = start_key
                last_end_key = end_key
                last_region = t_region
http_head = {
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
}
process = CrawlerProcess(http_head)
process.crawl(hbaseSpider)
process.start()