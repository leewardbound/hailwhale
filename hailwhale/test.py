import unittest, urllib, json, time

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
class TestHailWHale(unittest.TestCase):
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

    def testRatioPlotpoints(self):
        self.whale.count_now('test_ratio', '_', {'hits': 1, 'values': 5})
        self.whale.ratio_plotpoints('test_ratio', 'values')
    def testCrunch(self):
        return False # No longer in use
        # Unique key for every test
        t = str(time.time())
        # Do it 5 times so we can test values / hit
        for i in range(5):
            self.whale.count_now('test_crunch', [t, 'a'],
                    {'value': 15})
            self.whale.count_now('test_crunch', [t, 'b'],
                    {'value': 10})
            self.whale.count_now('test_crunch', [t, 'c'],
                    {'value': 25})

        data = self.whale.crunch('test_crunch', [t], ('value', 'hit'))
        # Data should be:
        # { [t,'a']: {'value': 15, 'weight': .30},
        #   [t,'b']: {'value': 10, 'weight': .20},
        #   [t,'c']: {'value': 25, 'weight': .50}}
        assert data[[t,'a']]['weight'] == .30
        assert data[[t,'b']]['weight'] == .20
        assert data[[t,'c']]['weight'] == .50
        
if __name__ == '__main__':
    unittest.main()
