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
        categories = json.dumps(kwargs.pop('categories', 'test'))
        dimensions = json.dumps(kwargs.pop('dimensions', ['empty',]))
        metrics = json.dumps(kwargs.pop('metrics', {}))
        params = (stub, categories,dimensions,metrics)
        url = '%s?categories=%s&dimensions=%s&metrics=%s'%params
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
        counting = lambda n: n['["empty"]']['counting_now']
        totals = self.getTotalsURL(metrics=['counting_now',])
        for i in range(3):
            self.assertEqual(self.getCountNowURL(metrics={'counting_now': 5}), 'OK')
        new_totals = self.getTotalsURL(metrics=['counting_now',])
        self.assertEqual(counting(new_totals), counting(totals) + 15)
    def testCountingCorrectly(self):
        counting = lambda n: n['["empty"]']['counting']
        totals = self.getTotalsURL(metrics=['counting',])
        for i in range(3):
            self.assertEqual(self.getCountURL(metrics={'counting': 5}), 'OK')
        self.assertEqual(self.getURL('/flush_hail'), 'OK')
        new_totals = self.getTotalsURL(metrics=['counting',])
        self.assertEqual(counting(new_totals), counting(totals) + 15)
        
if __name__ == '__main__':
    unittest.main()
