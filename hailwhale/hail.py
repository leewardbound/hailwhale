import json
import datetime
import time
import random

from redis import Redis

from util import curry_instance_attribute
from whale import Whale


class HailRedisDriver(Redis):
    pass

class Hail(object):
    hail_driver_class = HailRedisDriver
    hail_driver_settings = {}
    spy_size = 100

    def __init__(self, *args, **kwargs):
        if hasattr(self, 'id'):
            curry_instance_attribute('id', 'count', self)
            curry_instance_attribute('id', 'spy_pos_key', self)
            curry_instance_attribute('id', 'spy_pos', self)
            curry_instance_attribute('id', 'spy_key', self)
            curry_instance_attribute('id', 'spy_log', self)
            curry_instance_attribute('id', 'spy_at_key', self)
            curry_instance_attribute('id', 'get_spy', self)

    @classmethod
    def hail_driver(cls):
        if not hasattr(cls, '_hail_driver'):
            cls._hail_driver = cls.hail_driver_class(**cls.hail_driver_settings)
        return cls._hail_driver
        
    @classmethod
    def count(cls, pk, dimensions, metrics, at=False):
        try:
            r=cls.hail_driver()
            if not r: return 0
            set_number_name = 'hail_number'
            set_number = r.get(set_number_name) or 0
            if not set_number: set_number = r.set(set_number_name, 0)
            at = at or datetime.datetime.now()
            if isinstance(at, str):
                try: at = datetime.datetime.fromtimestamp(float(at))
                except ValueError: pass # if at is not a float, ignore this
            if isinstance(at, datetime.datetime): at = at.ctime()
            if isinstance(pk, cls): 
                pk = pk.getattr(cls.unique_key)
            hit_key = '%s_%s_%s_%s'%(
                    cls.__name__,pk,at,random.randint(1,1000))
            r.sadd('hail_%s'%(set_number), hit_key)
            r.set(hit_key, json.dumps(
                (cls.__name__, pk, dimensions, metrics, at)))
        except Exception as e: return '%s'%e
        return 'OK'

    @classmethod
    def spy_pos_key(cls, uid):
        if isinstance(uid, cls): uid = uid.getattr(cls.unique_key)
        return 'spy_%s_%s_pos'%(cls.__name__, uid)

    @classmethod
    def spy_pos(cls, uid):
        r = cls.hail_driver() 
        if not r: return 0
        pos_key = cls.spy_pos_key(uid)
        if not r.get(pos_key): r.set(pos_key, 0)
        return int(r.get(pos_key)) % cls.spy_size

    @classmethod
    def spy_key(cls, uid, pos=None):
        pos = pos or cls.spy_pos(uid)
        if isinstance(uid, cls): uid = uid.getattr(cls.unique_key)
        return 'spy_%s_%s_%s'%(cls.__name__, uid, pos)

    @classmethod
    def spy_log(cls, uid, data):
        r = cls.hail_driver()
        if not r or data is None: return None
        if not isinstance(data, str): data = json.dumps(data)
        spy_pos = r.incr(cls.spy_pos_key(uid))
        if spy_pos > cls.spy_size:
            spy_pos = 0
            r.set(cls.spy_pos_key(uid), spy_pos)
        if isinstance(uid, cls): uid = uid.getattr(cls.unique_key)
        hit_key = 'spy_%s_%s_%s_%f'%(cls.__name__,uid,spy_pos,time.time())
        r.set(cls.spy_key(uid, spy_pos), hit_key)
        return r.set(hit_key, data)

    @classmethod
    def spy_at_key(cls, uid, pos=None, r=None):
        r = cls.hail_driver()
        if not r: return None
        spy_key = cls.spy_key(uid, pos)
        hit_key = r.get(spy_key)
        data = r.get(hit_key)
        try: return json.loads(data)
        except: return data

    # Get the whole list
    @classmethod
    def get_spy(cls, uid, max_results=15):
        r = cls.hail_driver()
        if not r: return
        pos_key = cls.spy_pos_key(uid)
        spy_pos = int(r.get(pos_key))
        for i in range(min(cls.spy_size, max_results)):
            n = (spy_pos - i) % cls.spy_size
            entry = cls.spy_at_key(uid,n,r)
            if entry is None: return
            yield entry 

    @classmethod
    def dump_now(cls):
        """ Flush hits to Whale and increment """
        # Get the incoming hits from Hail
        
        whale = Whale()
        r=cls.hail_driver()
        set_number_name = 'hail_number'
        r.setnx(set_number_name, 0)
        set_number = r.incr(set_number_name) - 1
        set_name = 'hail_%s' % set_number
        try: keys_from_hail = r.smembers(set_name)
        except: return
        if not len(keys_from_hail):
            r.delete(set_name)
            return

        def get_keys_from_json(k):
            try: 
                class_name, pk, dimensions, metrics, at = json.loads(r[k])
                #at = datetime.datetime.fromtimestamp(float(t))
                return (pk, dimensions, metrics, at)
            except Exception as e: 
                print e
                return False 

        keys_to_update = map(get_keys_from_json, keys_from_hail)
        for packed in keys_to_update:
            if packed:
                pk, dimensions, metrics, at = packed
                whale.count_now(pk, dimensions, metrics, at=at)

        # Delete the hits
        map(r.delete, keys_from_hail)
        r.delete(set_name)
