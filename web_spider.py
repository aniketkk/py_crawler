#!/usr/bin/python


from bs4 import BeautifulSoup
import mechanize
import urllib
import threading
from sets import Set
import Queue
import re
import MySQLdb
class MultiThreadedSpider(threading.Thread):
	varLock = threading.Lock()

	def __init__(self, link_queue):
		threading.Thread.__init__(self)
		self.link_queue = link_queue
		self.db = MySQLdb.connect("localhost", “user”, “password”, “dbname”)
		self.cursor = db.cursor()
	def run(self):
		try:
			while True:
				browser_object = mechanize.Browser()
				browser_object.set_handle_robots(False)
				current_link = self.link_queue.get()
				page = browser_object.open(current_link)
				page_html = page.read()	
					
				crawled_links.add(current_link)
				#MultiThreadedSpider.varLock.release()
				for links_on_current_page in browser_object.links():
					if links_on_current_page.url.startswith('http') and len(re.compile("http[s]?://").split(links_on_current_page.url)[1].split('/')) <= int(depth_of_spidering) + 1:
						if links_on_current_page.url not in crawled_links:
							print links_on_current_page.url
							self.link_queue.put(links_on_current_page.url)
							try:
								self.cursor.execute("""Insert into crawldata (website) values (%s)""",(links_on_current_page.url,))
								self.db.commit()
							except:
								self.db.rollback()
					#print self.link_queue.qsize()
				self.link_queue.task_done()
				

		except Queue.Empty:
			print 'Queue is empty'
			pass


#Take the input from the user
website = raw_input("Please Enter the website link that you want to crawl >")
depth_of_spidering = raw_input("Please enter the depth of spidering >")

#website = 'https://www.google.com/'

link_queue = Queue.Queue()
link_queue.put(website)
crawled_links = Set()

db = MySQLdb.connect("localhost", "root", "pikachus", "py_crawl")
cursor = db.cursor()


#############################
# Create the spider threads #
#############################
for i in range(10):
	spider_thread = MultiThreadedSpider(link_queue)
	spider_thread.setDaemon(True)
	spider_thread.start()

link_queue.join()

print "Done..."
