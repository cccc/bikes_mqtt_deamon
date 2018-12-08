#!/usr/bin/env python3

import os
import urllib
import configparser
import json
import time
import threading
from time import strftime, localtime

import paho.mqtt.client as mqtt
from geopy.distance import distance
from geopy.geocoders import Nominatim

config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())

class CacheDictionary:
    def __init__(self):
        self.dictionary = dict()
        self.retain = []
    
    def keys(self):
        return self.dictionary.keys()
    
    def refresh(self):
        for key in list(self.dictionary.keys()):
            if key not in self.retain:
                del self.dictionary[key]
        self.retain.clear()
    
    def find(predicate,cache=True):
        for key in self.dictionary:
            value = self.dictionary[key]
            if predicate(key,value):
                self.retain.append(key)
                return key, value, True # third value True to be able to check None, None case
    
    def __contains__(self, key):
        return key in self.dictionary
    
    def __setitem__(self, key, value):
        if key not in self.retain:
            self.retain.append(key)
        self.dictionary[key]=value
    
    def __getitem__(self, key):
        if key not in self.retain:
            self.retain.append(key)
        return self.dictionary[key]
    
    def __delitem__(self, key):
        if key in self.retain:
            self.retain.remove(key)
        del self.dictionary[key]
    
    def __iter__(self):
        return iter(self.dictionary)


class LoopThread(threading.Thread):
    def __init__(self, func, interval=60):
        super().__init__()
        self.func = func
        self.interval = interval
    
    def run(self):
        while True:
            self.func()
            time.sleep(self.interval)

class nextbike:
    def __init__(self):
        self.config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        self.config.read(os.path.dirname(__file__)+"/nextbike.cfg")
        self._init_mqtt()
        self.loop_thread = LoopThread(self.send_update,int(self.config["service"]["interval"]))
        self._roadsCache = CacheDictionary()
        self.geocoder = Nominatim(user_agent="nbMQTT")
        
    def _init_mqtt(self):
        self.client = mqtt.Client("bikes/nextbike")
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
    
    def start(self):
        self.client.connect(self.config["server"]["host"], int(self.config["server"]["port"]), int(self.config["server"]["keepalive"]))
        self.client.loop_start()
        self.loop_thread.start()
    
    def _on_connect(self, client, userdata, flags, rc):
        print("Connected with result code " +str(rc))
    
    def _on_disconnect(self, a, b, c, d):
        z = True
        while z:
            try:
                self.client.reconnect()
                z=False
            except Exception as msg:
                print("Something went wrong while reconnecting! %s" % str(msg))
                

    def send_update(self):
        try:
            response = json.loads(urllib.request.urlopen(self.config["service"]["request"]).read().decode("utf-8"))
            bikes = [
                {
                    "bike_number" : bike_number,
                    "lat" : place["lat"],
                    "lng" : place["lng"],
                    "address" : place["address"] or self.getRoad((place["lat"],place["lng"])),
                    "name" : place["name"],
                    "distance" : distance((self.config["service"]["lat"],self.config["service"]["lng"]), (place["lat"],place["lng"])).meters
                }
                for country in response["countries"]
                for city in country["cities"]
                for place in city["places"] if place["bikes"]>0
                for bike_number in place["bike_numbers"]
            ] if ("countries" in response) else []
            self._roadsCache.refresh()
            #print(F"Sending package with {len(bikes)} entries")
            self.client.publish(self.config["service"]["topic"], json.dumps({"time":strftime("%d.%m. %H:%M:%S", localtime()),"bikes":bikes}), retain=True)
        except Exception as msg:
            print("Something went wrong! %s" % str(msg))

    def getRoad(self, coord):
        try:
            if coord in self._roadsCache:
                return self._roadsCache[coord]
            for cachedCoord in self._roadsCache:
                if distance(coord,cachedCoord).meters <= int(self.config["service"]["road_cache_distance"]):
                    if self._roadsCache.dictionary[cachedCoord] is not None:
                        return self._roadsCache[cachedCoord]
            address=self.geocoder.reverse(coord).raw["address"]
            self._roadsCache[coord] = address["road"] if "road" in address else None
            return self._roadsCache[coord]
        except Exception as msg:
            print("Something went wrong resolving location for %s! %s" % (str(coord),str(msg)))
            return
    
nextbike().start()
