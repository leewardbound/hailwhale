from hail import Hail
from whale import Whale


class HailWhale(Hail, Whale):
    def curry_hailwhale_methods(self, attr='id'):
        self.curry_hail_instance_methods(attr)
        self.curry_whale_instance_methods(attr)
        self.hail = Hail
        self.whale = Whale
