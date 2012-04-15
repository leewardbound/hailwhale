from hail import Hail
from whale import Whale


class HailWhale(Hail, Whale):
    def curry_hailwhale_methods(self, attr='id'):
        """ If you want, you can use HailWhale as a mixin, and then call this in
        the __init__ or __new__ of a class relating to your object, thus letting
        you count values on a per-object basis, with the pkey automatically
        passed. Example:
        >>> class Visitor(orm.models, HailWhale):
        ...     def __init__(self):
        ...         self.curry_hailwhale_methods()
        ...
        ...     def visited_site(self):
        ...         # Let's count visits by nested city/state/country --
        ...         self.count_now({'geo': {self.last_visit.country: {self.last_visit.region: self.last_visit.city}}},
        ...             {'visits': 1, 'time_on_site': self.stop_session - self.start_session}) 

        >>> Person(pk=5).count_now(


        """
        self.curry_hail_instance_methods(attr)
        self.curry_whale_instance_methods(attr)
        self.hail = Hail
        self.whale = Whale
    def __init__(self, driver=None):
        """ OK, this is very poor OO here, but I very much wanted Hailwhale to
        be an omnipotent force in your python lib. E.g. no instances, just
        symple syntax Hail.foo() and Whale.bar() [or HailWhale.foobar(), too!]
        So if you try to initialize it, it'll let you reconfigure the global
        redis server by passing a single parameter, and then return an instance
        that only calls class methods anyway. This means you can't have two
        drivers pointing at two seperate hosts unless you subclass.
        Yes, I thoroughly thought this through. Yes, it's a pretty bad idea. 
        But FUCK YES! It's easy to use and easy enough to understand!
        And until you patch it for me, you have no room to talk shit.
        Good day to you, sir. 
        """
        if driver:
            self.set_driver(driver)
    @classmethod
    def set_driver(cls, driver):
            cls._hail_driver = driver
            cls._whale_driver = driver
            return cls
