# -*- encoding: utf-8 -*-
import json
import requests
import MySQLdb
from netaddr import IPNetwork
from config import *

def get_feed_info(file):
    fd = open(file)
    feed = json.load(fd)
    return feed

def get_feed_file(url,file):
    res = requests.get(url)
    file_name = "feed/"+file
    with open(file_name, "w+") as fd:
        fd.write(res.content)
    return file_name

def entry_to_ips(str):
    ips = []
    for ip in IPNetwork(str):
        ips.append(ip)
    return ips

def get_all_ip(file_name):
    ip = []
    print file_name
    with open(file_name) as fd:
        for line in fd:
            line = line.strip()
            if not line.startswith("#"):
                if line.find("/") == True:
                    ip.append(entry_to_ips(line))
                else:
                    ip.append(line)
    return ip

if __name__ == '__main__':
    feeds = dict()
    with open(file_list) as list:
        for file in list:
            info = get_feed_info(file.strip())
            feed = dict()
            feed["maintainer"]      =   info["maintainer"]
            feed["maintainer_url"]  =   info["maintainer_url"]
            feed["category"]        =   info["category"]
            feed["source"]          =   info["source"]
            feed["file_local"]      =   info["file_local"]
            feed["updated"]         =   info["updated"]
            feed["file"]            =   info["file"]
            feeds[feed["maintainer"]] = feed

    for key in feeds.keys():
        print feeds[key]
        if feeds[key]["file_local"] != '' and feeds[key]["file_local"] != '':
            local_file_name = get_feed_file(feeds[key]["file_local"], feeds[key]["file"])
            print "%s is downloaded" % local_file_name
            print "Start parasing %s" % local_file_name
            feeds[key]["ips"] = get_all_ip(local_file_name)
            print "Parase %s Finished\n" % local_file_name

    db = MySQLdb.connect(db_host,db_user,db_pass,db_name)
    cursor = db.cursor()

    for key in feeds.keys():
        try:
            print "Start import %s" % feeds[key]["file"]
            for ip in feeds[key]["ips"]:
                sql = '''insert into records(ip,category,maintainer,maintainer_url,source,updated) VALUES ("%s","%s","%s","%s","%s","%s")''' % (
                ip, feeds[key]["category"], feeds[key]["maintainer"], feeds[key]["maintainer_url"],
                feeds[key]["source"], feeds[key]["updated"])
                print sql
                cursor.execute(sql)
            db.commit()
            print "%s Imported\n" % feeds[key]["file"]
        except:
            print "%s Import failed" % feeds[key]["file"]
            pass
    print "Import Finished"


