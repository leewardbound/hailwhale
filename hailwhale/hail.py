from redis import Redis
import json, datetime
class HailRedisDriver(Redis):
    pass
class Hail():
    driver_class = HailRedisDriver
    driver_settings = {}
    spy_size = 100
    @classmethod
    def driver(cls):
        if not hasattr(cls, '_hail_driver'):
            cls._hail_driver = cls.driver_class(**cls.driver_settings)
        return cls._hail_driver
    @classmethod
    def count(cls, categories, dimensions, metrics, at=False):
        import time, json, random
        try:
            r=cls.driver()
            if not r: return 0
            set_number_name = 'hail_number'
            set_number = r.get(set_number_name) or 0
            if not set_number: set_number = r.set(set_number_name, 0)
            at = at or time.time()
            if isinstance(categories, cls): categories = categories.getattr(cls.unique_key)
            hit_key = '%s_%s_%s_%s'%(
                    cls.__name__,categories,at,random.randint(1,1000))
            r.sadd('hail_%s'%(set_number), hit_key)
            r.set(hit_key, json.dumps(
                (cls.__name__, categories, dimensions, metrics, at)))
        except Exception as e: return '%s'%e
        return 'OK'

    @classmethod
    def spy_pos_key(cls, uid):
        if isinstance(uid, cls): uid = uid.getattr(cls.unique_key)
        return 'spy_%s_%s_pos'%(cls.__name__, uid)

    @classmethod
    def spy_pos(cls, uid):
        r = cls.driver() 
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
        import json, time
        r = cls.driver()
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
        import json
        r = cls.driver()
        if not r: return None
        spy_key = cls.spy_key(uid, pos)
        hit_key = r.get(spy_key)
        data = r.get(hit_key)
        try: return json.loads(data)
        except: return data

    # Get the whole list
    @classmethod
    def get_spy(cls, uid, max_results=15):
        r = cls.driver()
        if not r: return
        pos_key = cls.spy_pos_key(uid)
        spy_pos = int(r.get(pos_key))
        for i in range(0,min(cls.spy_size, max_results)):
            n = (spy_pos - i) % cls.spy_size
            entry = cls.spy_at_key(uid,n,r)
            if entry is None: return
            yield entry 

    @classmethod
    def dump_now(cls):
        """ Flush hits to Whale and increment """
        # Get the incoming hits from Hail
        from whale import Whale
        whale = Whale()
        r=cls.driver()
        _s_n_n = 'hail_number'
        r.setnx(_s_n_n, 0)
        set_number = r.incr(_s_n_n) - 1
        set_name = 'hail_%s'%set_number
        try: keys_from_hail = r.smembers(set_name)
        except: return
        if len(keys_from_hail) is 0:
            r.delete(set_name)
            return
        def get_keys_from_json(k):
            try: 
                class_name, categories, dimensions, metrics, t = json.loads(r[k])
                at = datetime.datetime.fromtimestamp(float(t))
                return (categories, dimensions, metrics, at)
            except Exception as e: 
                print e
                return False 

        keys_to_update = map(get_keys_from_json, keys_from_hail)
        for packed in keys_to_update:
            if not packed: continue
            categories, dimensions, metrics, at = packed
            whale.count_now(categories, dimensions, metrics, at=at)

        # Delete the hits
        map(r.delete, keys_to_update)
        r.delete(set_name)


