
import sys
import time
import thread
import os.path
import mechanize
import urllib
import urlparse
import operator
from bs4 import BeautifulSoup
from threading import Thread
from multiprocessing import Pool

br = mechanize.UserAgentBase()
br.set_handle_robots(True)
inval_ext = ('.pdf','.jpg','.jpeg','.gif','.png','.js','.css','.mp3','.mp4',
             '.mpeg','.zip','.7z','.tif','.tiff','.tar','.swf','.apk','.avi')
url_incnt ={'http://www.harvard.edu': 100,
            'http://en.wikipedia.org/wiki/Harvard_University': 100,
            'https://www.google.com/search?client=safari&rls=en&q=Harvard&ie=UTF-8&oe=UTF-8' : 100,
            'https://www.cappex.com/colleges/Harvard-University/tuition-and-costs' : 100}
url_id = {}
in_links = {}
error_url = {}
vstd_sites = {}
crawl_lst = []
visited = []
docs_loc = 'D:/IR/results/assign 3/docs/d_'
line = '__'
url_count = 1
doc_count = 1
doc_name= -1

def main():
    global url_count; global doc_count; global doc_name
    tt = time.time()
    runpgm = 1
    for url in url_incnt:
        url_id[url] = url_count
        url_count +=1
        
    while(runpgm == 1): 
        if(len(url_incnt) == 0 or doc_count == 8):
            print 'Count of ' + str(doc_count) + ' reached'
            runpgm = -1

        else:
            nurl = getNextURL()
            thread.start_new_thread(threader,(nurl,))
            #fetch_url(fl) 

    else:
        dumpLogsToFile()
        print 'Total run time: ' + str(time.time() - tt)

def threader(url):
    lock = threading.RLock()
    global doc_count
    try:
        newdoc = docs_loc+ str(doc_count)+'.txt'
        print ' File: '+newdoc
        fl = open(newdoc,'w')
        print 'DOC_COUNT: ' + str(doc_count)

        lock.acquire()
        doc_count += 1
        lock.release()
        
        print url + ' ' + str(newdoc)
    except Exception as inst:
        ErrorPrint(inst,'Exception in fetching ',url)

    except:
        ErrorPrint(sys.exc_info()[0],'Error while URL Fetch ',url)
    finally:
        lock.release
    #fetch_url(fl,url)
            
def getNextURL():
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
        print line * 40 + '\n PATH: ' + curr_url
        print 'Time to RTRV C_URL: ' + str(time.time() - start)
        return curr_url
    except:
        ErrorPrint(sys.exc_info()[0],'Error while Choosing next URL to fetch ',"No URL FETCHED")


def fetch_url(url):
    start = time.time()
    global vstd_sites; global visited
    try:
        website = url.split('//')[1].split('/')[0].replace('www.','')
        ts = time.time()
##        if vstd_sites.has_key(website):
##            if ((ts - vstd_sites[website]) < 1):
##                print 'sleeping'
##                time.sleep(1- (ts - vstd_sites[website]))
        
        html = br.open(curr_url)
        vstd_sites[website] = time.time()
        header = html.info()
        ctype = header['Content-Type'].split(';')[0]
        if( ctype == 'text/html'):
            doc = html.read()
            soup = BeautifulSoup(doc)
            outlinks = getLinks(soup,url)
            addToCrawlList(outlinks,url)
            text = getDisplayText(soup,url)
            writeToFile(outlinks,doc,text,header,fl,url)
            visited.append(url)
            print 'Time to FETCH PAGE: ' + str(time.time() - start)
        else:
            error_url[curr_url] = 'invalid content type or lang : ' + ctype + ' ' + header['Content-Language']
        
    except(mechanize.HTTPError, mechanize.URLError) as e:
        ErrorPrint(e,'Mechanize error ',url)

    except Exception as inst:
        ErrorPrint(inst,'Exception in fetching ',url)

    except:
        ErrorPrint(sys.exc_info()[0],'Error while URL Fetch ',url)

def getLinks(soup,base_url):
    links =[]
    for link in soup.find_all('a'):
        href = link.get("href")
        if(href is not None and not(href.startswith('#') or href.startswith('mailto') \
                                    or href.startswith('javascript'))):
            links.append(canonise(href.encode("utf8"),base_url))
    return links

def canonise(url,base_url):
    newurl=''
    tmp = []
    url = urlparse.urljoin(base_url,url)
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

def getDisplayText(soup,url):
    try:
        #[scrp.extract() for scrp in soup('script')]
        #[scrp.extract() for scrp in soup('style')]
        for e in soup.findAll(['script', 'style']):
            e.extract()
        data = soup.get_text()
        data = data.encode("utf8")
        data = data.replace('\n\n',"")
    except:
        ErrorPrint(sys.exc_info()[0],'Error while retrieving text from soup ',url)
    return data

def writeToFile(links,doc,text,header,fl,url):
    global doc_count
    try:
        fl.write('<DOC> \n<DOCNO>\n' + str(id(url))+ '\n</DOCNO>\n<URL>\n')
        fl.write(str(url)+'\n</URL>\n<HEADERS>\n' +str(header)+'\n')
        fl.write('</HEADERS>\n<RAW>\n' +str(doc)+ '\n</RAW>\n<TEXT>\n'+text)
        fl.write('\n</TEXT>\n<OUTLINKS>\n')
        for l in links:
            fl.write(l + '\n')
        fl.write('</OUTLINKS>\n</DOC>\n')
        doc_count += 1
        
    except:
        ErrorPrint(sys.exc_info()[0],'Error while writing File ',url)

def addToCrawlList(links,url):
    global url_count
    try:
        for l in links:
            if( not(l in visited) and not(l in error_url)):
                if (not l.endswith(inval_ext)):
                    if(url_incnt.has_key(l)):
                        url_incnt[l] = url_incnt[l] +1
                    else:
                        url_incnt[l] = 1
                        url_id[l] = url_count
                        url_count += 1
            updateInLink(l,url)
    except:
        ErrorPrint(sys.exc_info()[0],'Error while updating crawl list ',url)

def updateInLink(url,base_url):
    try:
        if(in_links.has_key(url)):
            in_links[url] = in_links[url] + '$$;$$' + base_url
        else:
            in_links[url] = curr_url;
    except:
         ErrorPrint(sys.exc_info()[0],'Error while updating inliks ',url)

def ErrorPrint(e,src,url):
    error_url[url] = e
    print '\n' + src
    print str(e)

def dumpLogsToFile():
    fl = open('error.txt','w')
    for x in error_url: fl.write(str(x)+ ' :' +str(error_url[x])  + '\n')
    fl.close()
    fl = open('in-link.txt','w')
    for x in in_links: fl.write(str(x)+ ' :' +str(in_links[x]) + '\n')
    fl.close()

main() # Fn call
