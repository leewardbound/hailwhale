import unittest
import urllib
import json
import time

class TestHailWhaleHTTP(unittest.TestCase):
    def setUp(self):
        self.service_url = 'http://localhost:8085'
    def getURL(self, url):
        data = urllib.urlopen(self.service_url + url).read()
        try: return json.loads(data)
        except: return data
    def getCountURL(self, **args):
        return self.getStandardParamsURL('/count', **args)
    def getCountNowURL(self, **args):
        return self.getStandardParamsURL('/count_now', **args)
    def getPlotpointsURL(self, **args):
        return self.getStandardParamsURL('/plotpoints', **args)
    def getTotalsURL(self, **args):
        return self.getStandardParamsURL('/totals', **args)
    def getStandardParamsURL(self, stub='/', **kwargs):
        pk = json.dumps(kwargs.pop('pk', 'test'))
        dimensions = json.dumps(kwargs.pop('dimensions', ['empty',]))
        metrics = json.dumps(kwargs.pop('metrics', {}))
        params = (stub, pk,dimensions,metrics)
        url = '%s?pk=%s&dimensions=%s&metrics=%s'%params
        return self.getURL(url)
    def testCountService(self):
        """ /count should return 'OK' for successful hits """
        self.assertEqual(self.getCountURL(), 'OK')
    def testCountNowService(self):
        """ /count_now should return 'OK' for successful hits """
        self.assertEqual(self.getCountNowURL(), 'OK')
    def testResetCategory(self):
        self.getStandardParamsURL('/reset')
    def testCountingNowCorrectly(self):
        counting = lambda n: n['alltime']['["empty"]']['counting_now']
        totals = self.getTotalsURL(metrics=['counting_now',])
        for i in range(3):
            self.assertEqual(self.getCountNowURL(metrics={'counting_now': 5}), 'OK')
        new_totals = self.getTotalsURL(metrics=['counting_now',])
        self.assertEqual(counting(new_totals), counting(totals) + 15)
    def testCountingCorrectly(self):
        counting = lambda n: n['alltime']['["empty"]']['counting']
        totals = self.getTotalsURL(metrics=['counting',])
        for i in range(3):
            self.assertEqual(self.getCountURL(metrics={'counting': 5}), 'OK')
        self.assertEqual(self.getURL('/flush_hail'), 'OK')
        new_totals = self.getTotalsURL(metrics=['counting',])
        self.assertEqual(counting(new_totals), counting(totals) + 15)
        
class TestHailWhale(unittest.TestCase):
    def setUp(self):
        from hail import Hail
        from whale import Whale
        self.hail = Hail()
        self.whale = Whale()
    def testGetSubdimensions(self):
        self.whale.count_now('test', {'a': 1, 'b': 2})
        subs = self.whale.get_subdimensions('test')
        assert(['a',] in subs)
        assert(['b',] in subs)
    def testGetAllSubdimensions(self):
        self.whale.count_now('test', {'a': 1, 'b': 2})
        subs = self.whale.all_subdimensions('test')
        assert(['a',] in subs)
        assert(['a', '1'] in subs)
        assert(['b',] in subs)
        assert(['b', '2'] in subs)

    def testPlotpoints(self):
        t = str(time.time())

        for i in range(5):
            self.whale.count_now('test_plotpoints', t, {'hits': 1, 'values': 5})
        plotpoints = self.whale.plotpoints('test_plotpoints', t, ['hits', 'values'], points_type=list)

        self.assertEqual(plotpoints[t]['hits'][-1][1], 5)    
        self.assertEqual(plotpoints[t]['values'][-1][1], 25)

        
    def testRatioPlotpoints(self):
        t = str(time.time())

        for i in range(5):
            self.whale.count_now('test_ratio', t, {'hits': 1, 'values': 5})

        plotpoints = self.whale.plotpoints('test_ratio', t, ['hits', 'values'], points_type=list)
        ratio_plotpoints = self.whale.ratio_plotpoints('test_ratio', 'values', 'hits', t, points_type=list)
        
        self.assertEqual(plotpoints[t]['hits'][-1][1], 5)    
        self.assertEqual(plotpoints[t]['values'][-1][1], 25)    

        self.assertEqual(ratio_plotpoints[t][-1][1], 5)

    def testRankSubDimensionsScalar(self):
        t = str(time.time())
        self.whale.count_now('test_ranksubdimensions_scalar')
        self.whale.rank_subdimensions_scalar('test_ranksubdimensions_scalar')

    def testCrunch(self):
        pass
        
if __name__ == '__main__':
    unittest.main()
