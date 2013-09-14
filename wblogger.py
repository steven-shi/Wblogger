import wblogin
from celery import Celery
from celery import group
from pymongo import MongoClient
from datetime import datetime, timedelta
import requests
from bson.binary import Binary
import pytz
import math
import logging
import sys

config_module_name = "config"

celery = Celery('tasks')
celery.config_from_object(config_module_name)
config = __import__(config_module_name)

logger = logging.getLogger('WbLogger')
logger.addHandler(logging.FileHandler("./wblogger.log"))

client = wblogin.get_client(config)
db_client = MongoClient(config.MONGO_SERVER).WbLogger
count = 100

@celery.task
def read_wb(page,screen_name, count):
	print "Calling page " + str(page)

	statuses = client.statuses.user_timeline.get(screen_name=screen_name,page=page, count=count)
	weibo = db_client.weibo
	for post in statuses.statuses:
		entry = dict(post)
		wb = weibo.find_one({"id":entry["id"]})
		if wb is not None:
			if entry["comments_count"] == 0:
				continue
			# check if comments are up2date
			if entry["comments_count"] != wb["comments_count"]:
				wb["comments_count"] = entry["comments_count"]
				weibo.save(entry)
			if (entry["comments_count"]>0 and "comments" not in wb) or (len(wb["comments"]) < entry["comments_count"]):
				add_comments.apply_async((entry["id"],))
			continue

		if len(entry["pic_urls"]) > 0:
			try:
				print "Found image " + str(entry["pic_urls"])
				logger.debug([url["thumbnail_pic"] for url in entry["pic_urls"]])
				entry["pic_binary"] = get_image_BSON([url["thumbnail_pic"] for url in entry["pic_urls"]])
			except Exception as e:
				print "Error to encode image"
				logger.error(e.message)
		if entry["comments_count"] >0:
			add_comments.apply_async((entry["id"],))
		entry["created_at"] = convert_dt(post.created_at)
		try:
			weibo.insert(entry)
			print "insert "+str(entry["id"]) + " " + entry["text"]
		except Exception as e :
			logger.error(e.message)

@celery.task
def add_comments(id):
	try:
		cmts = client.comments.show.get(id=id, count=50)["comments"]
		weibo = db_client.weibo
		entry = weibo.find_one({"id":id})
		if entry is None:
			return
		print "Insert comments " + str(entry["comments_count"])
		entry['comments']=cmts
		weibo.save(entry)
	except Exception as e:
		logger.error(e.message + " Error wb id: "+str(id))

def cleanup_wb():
	old_wb = db_client.weibo.find({'created_at':{'$lt': datetime.now()-timedelta(90)},"deleted": {"$ne": True} })
	for wb in old_wb:
		delete_task.apply_async((wb["id"], wb))

@celery.task
def delete_task(id, old_wb):
	post = client.statuses.destroy.post(id=id)
	old_wb["deleted"] = True
	db_client.weibo.save(old_wb)
	print "delete wb: "+ str(id)


def get_image_BSON(url_list):
	print "download image number " + str(len(url_list))
	images = [requests.get(url).content for url in url_list]
	return {"pic_binary":[Binary(img) for img in images]}

def convert_dt(dt_str):
	dt=datetime.strptime(dt_str[:20]+dt_str[26:],"%c")
	return pytz.timezone('Asia/Shanghai').localize(dt)

def print_man():
	print "weibo2db sync [screenname] \n weibo2db cleanup"

def launch(argv):
	if len(argv) <2:
		print_man()
		sys.exit()

	if argv[1] == "sync":
		if len(argv) != 3:
			print_man()
			sys.exit()
		screen_name = argv[2]
		total = client.statuses.user_timeline.get(screen_name=screen_name).total_number
		pages = int(math.ceil(total/float(100)))
		result = group(read_wb.s(x, screen_name, count) for x in range(1,pages+1))
		result.apply_async()
	elif sys.argv[1] == "cleanup":
		cleanup_wb()

if __name__ == "__main__":
	launch(sys.argv)