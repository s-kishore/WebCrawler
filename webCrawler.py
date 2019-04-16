
import sys
import time
import os.path
import mechanize
import urllib
import urlparse
import operator
import hashlib
import re
from bs4 import BeautifulSoup

br = mechanize.UserAgentBase()
br.set_handle_robots(True)
br.set_handle_refresh(False)

inval_ext =('.pdf','.jpg','.jpeg','.gif','.png','.js','.css','.mp3','.mp4',
            '.mpeg','.zip','.7z','.tif','.tiff','.tar','.swf','.apk','.avi')

restrict =('http://www.law.harvard.edu/calendar/','http://www.youtube.com/',
           'https://play.google.com/store/')

url_incnt ={'http://www.harvard.edu': 100,
            'http://www.harvard.edu/harvard-glance':100,
            'http://www.hbs.edu/Pages/default.aspx':100,
            'https://www.cappex.com/colleges/Harvard-University/tuition-and-costs' : 100}
url_id = {}
in_links = {}
error_url = {}
vstd_sites = {}
crawl_lst = []
file_data=[]
visited = []
docs_loc = 'D:/IR/results/assign 3/docs/d_'
line = '__'
url_count = 1
doc_count = 0
curr_url =''
doc_name= 0
total_docs = 0
def main():
    global url_count; global doc_count; global doc_name
    tt = time.time()
    runpgm = 1
    for url in url_incnt:
        url_id[url] = url_count
        url_count +=1
    
    while(runpgm == 1):
        if(len(url_incnt) == 0 or total_docs >= 15000):
            print 'Count of ' + str(total_docs) + ' reached'
            runpgm = -1
            if(len(file_data) > 0):
                WriteData(docs_loc+ str(doc_name)+'.txt')
        else:
            if((total_docs+1)%750 == 0):
                dumpLogsToFile()
                
            if( doc_count >= 500):
                newdoc = docs_loc+ str(doc_name)+'.txt'
                WriteData(newdoc)
                doc_count = 0
                doc_name += 1
            print 'COUNT: ' + str(total_docs) + ' LINKS: ' + str(len(url_incnt))
            getNextURL()
            print curr_url
            fetch_url()
            
    else:
        dumpLogsToFile()
        print 'Total run time: ' + str(time.time() - tt)

def WriteData(newdoc):
    global file_data
    try:
        fl = open(newdoc,'w')
        for i in range(len(file_data)):
            fl.write(file_data.pop())
        fl.close()
    except:
        ErrorPrint(sys.exc_info()[0],'Error while writing File ')
        pass
        
def getNextURL():
    global curr_url
    start = time.time()
    min = 999999
    try:
        #Sorting urls based on value (in-count)
        sorted_url_incnt = sorted(url_incnt.items(), key = operator.itemgetter(1))
        max_in_cnt = int(sorted_url_incnt[len(sorted_url_incnt)-1][1])
        
        #Filtering urls with the same in-count
        same_incnt_urls = filter(lambda x: x[1] == max_in_cnt,sorted_url_incnt)

        #Retriving the oldest of the urls with same max in-count
        if(len(same_incnt_urls) == 1):
            curr_url = same_incnt_urls[0][0]
        else:
            for url in same_incnt_urls:
                tmp = url_id[url[0]]
                if( tmp < min):
                    min = tmp
                    curr_url = url[0]
        del url_incnt[curr_url]
    except:
        ErrorPrint(sys.exc_info()[0],'Error while Choosing next URL to fetch ')
    print line * 40 #+ '\n PATH: ' + curr_url
    print 'URL RTRV: ' + ("%.2f" % (time.time() - start))

def fetch_url():
    global vstd_sites; global visited
    try:
        website = curr_url.split('//')[1].split('/')[0].replace('www.','')
        ts = time.time()
##        if vstd_sites.has_key(website):
##            if ((ts - vstd_sites[website]) < 1):
##                print 'sleeping'
##                time.sleep(1- (ts - vstd_sites[website]))
        
        html = br.open(curr_url,timeout=5.0)
        print 'time ftch: ' + ("%.2f" % (time.time() - ts))
        vstd_sites[website] = time.time()
        header = html.info()
        ctype = header['Content-Type'].split(';')[0]
        if( ctype == 'text/html'):
            doc = html.read()
            soup = BeautifulSoup(doc)
            #print 'time soup: ' + ("%.2f" % (time.time() - ds))
            title = getTitle(soup)
            outlinks = getLinks(soup)
            text = getDisplayText(soup)
            saveData(outlinks,doc,text,header,title)
            visited.append(curr_url)
            addToCrawlList(outlinks)
            print'time totl: ' +("%.2f" % (time.time() - ts))
        else:
            error_url[curr_url] = 'invalid content type or lang : ' + ctype + ' ' + header['Content-Language']
        
    except(mechanize.HTTPError, mechanize.URLError) as e:
        ErrorPrint(e,'Mechanize error ')

    except Exception as inst:
        ErrorPrint(inst,'Exception in fetching ')

    except:
        ErrorPrint(sys.exc_info()[0],'Error while URL Fetch ')

def getTitle(soup):
    try:
        return soup.find('title').get_text().encode("utf-8")
    except:
        return ''
    
def getLinks(soup):
    #ts = time.time()
    links =[]
    for link in soup.find_all('a'):
        href = link.get("href")
        if(href is not None and not(href.startswith('#') or href.startswith('mailto') \
                                    or href.startswith('javascript'))):
            links.append(canonise(href.encode("utf8")))
    #print 'URL Retrv: ' + ("%.2f" % (time.time() - ts))
    return links

def canonise(url):
    newurl=''
    tmp = []
    url = urlparse.urljoin(curr_url,url)
    if(url.startswith('http://')):
        newurl ='http://'
        remurl = url[7:].replace('//','/').replace(':80/','/')
    elif(url.startswith('https://')):
        newurl ='https://'
        remurl = url[8:].replace('//','/').replace(':80/','/')
    else:
        remurl = url.replace('//','/').replace(':80/','/')

    tmp.extend(remurl.split('/'))
    newurl = newurl + tmp[0].lower()+'/'
    tmp.remove(tmp[0])
    for s in tmp:
        sp = s.split("#")
        newurl = newurl + sp[0] + '/'
    return newurl[:-1]

def getDisplayText(soup):
    try:
        #ts = time.time()
        for e in soup.findAll(['script', 'style','form']):
            e.extract()
        data = soup.get_text()
        data = re.sub(r'\s+', ' ', data.encode("utf8").strip());
        #print 'Text Retrv: ' + ("%.2f" % (time.time() - ts))
        
    except:
        ErrorPrint(sys.exc_info()[0],'Error while retrieving text from soup ')
    return data

def saveData(links,doc,text,header,title):
    global doc_count; global file_data; global total_docs
    #ts = time.time()
    try:
        header = re.sub(r'\s+', ' ', str(header).strip())
        doc = re.sub(r'\s+', ' ', doc.strip());

        fl_data = '<DOC> \n<DOCNO>' + str(hashlib.md5(curr_url).hexdigest()) + '</DOCNO>\n' \
                    +'<TITLE>'+title+'</TITLE>\n<URL>'+str(curr_url)+'</URL>\n<TEXT>' \
                    +text+'</TEXT>\n<HEADERS>'+header+'</HEADERS>\n<RAW>' \
                    +doc+'</RAW>\n<OUTLINKS>'+str(links)+'</OUTLINKS>\n</DOC>\n'
        file_data.append(fl_data)
        doc_count += 1
        total_docs +=1
        #print 'Saving Data: ' + ("%.2f" % (time.time() - ts))
        
    except:
        ErrorPrint(sys.exc_info()[0],'Error while updating File data ')

def addToCrawlList(links):
    global url_count
    #ts = time.time()
    try:
        for l in links:
            if( not(l in visited) and not(l in error_url)
                and not l.startswith(restrict) and (len(l) <= 270)):
                if (not l.endswith(inval_ext)):
                    if(url_incnt.has_key(l)):
                        url_incnt[l] = url_incnt[l] +1
                    else:
                        url_incnt[l] = 1
                        url_id[l] = url_count
                        url_count += 1
            updateInLink(l)
        #print 'Add to Crawl: ' + ("%.2f" % (time.time() - ts))
    except:
        ErrorPrint(sys.exc_info()[0],'Error while updating crawl list ')

def updateInLink(url):
    try:
        if(in_links.has_key(url)):
            in_links[url] = in_links[url] + '$$;$$' + curr_url
        else:
            in_links[url] = curr_url;
    except:
         ErrorPrint(sys.exc_info()[0],'Error while updating inliks ')

def ErrorPrint(e,src):
    error_url[curr_url] = e
    print src
    print str(e)

def dumpLogsToFile():
    fl = open('error.txt','w')
    for x in error_url: fl.write(str(x)+ ' :' +str(error_url[x])  + '\n')
    fl.close()
    fl = open('in-link.txt','w')
    for x in in_links: fl.write(str(x)+ ' :' +str(in_links[x]) + '\n')
    fl.close()
main() # Fn call
