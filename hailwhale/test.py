import unittest, urllib, json, time
from collections import defaultdict
from whale import maybe_dumps

class TestHailWhaleHTTP(unittest.TestCase):
    def setUp(self):
        self.service_url = 'http://localhost:8085'

    def getURL(self, url):
        data = urllib.urlopen(self.service_url + url).read()
        try:
            return json.loads(data)
        except:
            return data

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
        dimensions = json.dumps(kwargs.pop('dimensions', ['empty']))
        metrics = json.dumps(kwargs.pop('metrics', {}))
        params = (stub, pk, dimensions, metrics)
        url = '%s?pk=%s&dimensions=%s&metrics=%s' % params
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
        counting = lambda n: n['10x300']['empty']['counting_now']
        totals = self.getTotalsURL(metrics=['counting_now'])
        for i in range(3):
            self.assertEqual(self.getCountNowURL(metrics={'counting_now': 5}), 'OK')
        new_totals = self.getTotalsURL(metrics=['counting_now'])
        self.assertEqual(counting(new_totals), counting(totals) + 15)

    def testCountingCorrectly(self):
        counting = lambda n: n['10x300']['empty']['counting']
        totals = self.getTotalsURL(metrics=['counting'])
        for i in range(3):
            self.assertEqual(self.getCountURL(metrics={'counting': 5}), 'OK')
        self.assertEqual(self.getURL('/flush_hail'), 'OK')
        new_totals = self.getTotalsURL(metrics=['counting'])
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
        assert(['a'] in subs)
        assert(['b'] in subs)

    def testGetAllSubdimensions(self):
        self.whale.count_now('test', {'a': 1, 'b': 2})
        subs = self.whale.all_subdimensions('test')
        assert(['a'] in subs)
        assert(['a', '1'] in subs)
        assert(['b'] in subs)
        assert(['b', '2'] in subs)

    def testPlotpoints(self):
        t = str(time.time())

        for i in range(5):
            self.whale.count_now('test_plotpoints', t, {'hits': 1, 'values': 5})
        plotpoints = self.whale.plotpoints('test_plotpoints', t, ['hits', 'values'], points_type=list)

        self.assertEqual(plotpoints[t]['hits'][-1][1], 5)
        self.assertEqual(plotpoints[t]['values'][-1][1], 25)

    def testPlotpointsDepth(self):
        t = str(time.time())
        self.whale.count_now('test_depth', {t: 'a'})
        self.whale.count_now('test_depth', {t: 'b'})
        self.whale.count_now('test_depth', {t: {'c': 'child'}})
        # Test 1 level deep
        plotpoints = self.whale.plotpoints('test_depth', t, points_type=list, depth=1)
        self.assertEqual(plotpoints[maybe_dumps([t, 'a'])]['hits'][-1][1], 1)
        self.assertEqual(plotpoints[maybe_dumps([t, 'b'])]['hits'][-1][1], 1)
        self.assertEqual(plotpoints[maybe_dumps([t, 'c'])]['hits'][-1][1], 1)
        self.assertEqual(False, maybe_dumps([t, 'c', 'child']) in plotpoints)
        # Test 2 levels deep
        plotpoints = self.whale.plotpoints('test_depth', t, points_type=list, depth=2)
        self.assertEqual(True, maybe_dumps([t, 'c', 'child']) in plotpoints)
        self.assertEqual(plotpoints[maybe_dumps([t, 'c', 'child'])]['hits'][-1][1], 1)

    def testRatioPlotpoints(self):
        t = str(time.time())

        for i in range(5):
            self.whale.count_now('test_ratio', t, {'hit': 1, 'value': 5})

        plotpoints = self.whale.plotpoints('test_ratio', t, ['hit', 'value', 'value/hit'], points_type=list)

        self.assertEqual(plotpoints[t]['hit'][-1][1], 5)
        self.assertEqual(plotpoints[t]['value'][-1][1], 25)

        self.assertEqual(plotpoints[t]['value/hit'][-1][1], 5)

    def testRankSubdimensionsScalar(self):
        t = str(time.time())
        self.whale.count_now('test_rank', [t, 'a', 'asub1'], {'value': 1})
        self.whale.count_now('test_rank', [t, 'a', 'asub2'], {'value': 30})
        self.whale.count_now('test_rank', [t, 'b'], {'value': 80})
        self.whale.count_now('test_rank', [t, 'c'], {'value': 10})
        ranked = self.whale.rank_subdimensions_scalar('test_rank', t, 'value')
        self.assertEqual(ranked[maybe_dumps([t, 'a'])]['important'], False)
        self.assertEqual(ranked[maybe_dumps([t, 'a', 'asub1'])]['important'], False)
        self.assertEqual(ranked[maybe_dumps([t, 'a', 'asub2'])]['important'], True)
        self.assertEqual(ranked[maybe_dumps([t, 'b'])]['important'], True)
        self.assertEqual(ranked[maybe_dumps([t, 'c'])]['important'], False)

    def testRankSubdimensionsRatio(self):
        t = str(time.time())
        pk = 'test_ratio_rank'
        # OVERALL STATS: 529,994 value, 50,000 visitors, 10.6 value per visitor
        # Not important, too close to overall
        self.whale.count_now(pk, [t, 'a', 'asub1'],
            {'value': 54989, 'visitors': 4999})  # 11 value per visitor
        # Important, high relative ratio
        self.whale.count_now(pk, [t, 'a', 'asub2'],
            {'value': 375000, 'visitors': 25000})  # 15 value per visitor
        # Important, low relative ratio
        self.whale.count_now(pk, [t, 'b'],
            {'value': 100000, 'visitors': 20000})  # 5 value per visitor
        # Not important, not enough visitors
        self.whale.count_now(pk, [t, 'c'],
            {'value': 5, 'visitors': 1})  # 5 value per visitor

        one_level = self.whale.rank_subdimensions_ratio('test_rank_ratio', 'value', 'visitors',
            t, recursive=False)

        all_levels = self.whale.rank_subdimensions_ratio(pk, 'value', 'visitors', t)
        self.assertNotIn(maybe_dumps([t, 'a', 'asub1']), one_level)
        self.assertEqual(all_levels[maybe_dumps([t, 'a', 'asub1'])]['important'], False)
        self.assertEqual(all_levels[maybe_dumps([t, 'a', 'asub2'])]['important'], True)
        self.assertEqual(all_levels[maybe_dumps([t, 'b'])]['important'], True)
        self.assertEqual(all_levels[maybe_dumps([t, 'c'])]['important'], False)

    def testBasicDecision(self):
        pk = 'test_basic_decision'
        decision = str(time.time())
        # Make a decision, any decision, from no information whatsoever
        any_one = self.whale.decide(pk, 'random', [1, 2, 3])
        self.assertIn(any_one, [1, 2, 3])

        # OK, now how about something somewhat informed?
        # This will be easy. Slogan A makes us huge profit. Products B and C suck.
        # D looks promissing but isn't yet significant
        opts = ['a', 'b', 'c', 'd']
        self.whale.count_now([pk, decision, 'a'], None, dict(dollars=5000, visitors=1000))
        self.whale.count_now([pk, decision, 'b'], None, dict(dollars=0, visitors=2000))
        self.whale.count_now([pk, decision, 'c'], None, dict(dollars=0, visitors=2000))
        self.whale.count_now([pk, decision, 'd'], None, dict(dollars=50, visitors=10))

        reasons = self.whale.reasons_for([pk, decision, 'a'], formula='dollars/visitors')
        self.assertEqual(reasons['good'], {})
        self.assertEqual(reasons['bad'], {})
        good, bad, test = self.whale.reasons(pk, decision, opts, formula='dollars/visitors')
        self.assertIn('a', good.keys())
        self.assertIn('b', bad.keys())
        self.assertIn('c', bad.keys())
        self.assertIn('d', test.keys())
        which_one = self.whale.decide(pk, decision, opts, formula='dollars/visitors',
            bad_idea_threshold=0, test_idea_threshold=0)
        self.assertEqual(which_one, 'a')

    def testInformedDecision(self):
        pk = 'test_informed_decision'
        decision = str(time.time())

        # A is the clear winner, except when country=UK, in which case B wins
        opts = ['a', 'b', 'c', 'd']
        self.whale.count_now([pk, decision, 'a'], None, dict(dollars=50000, visitors=10000))
        self.whale.count_now([pk, decision, 'b'], None, dict(dollars=0, visitors=4000))
        self.whale.count_now([pk, decision, 'b'], {'country': 'uk'}, dict(dollars=5000, visitors=500))
        self.whale.count_now([pk, decision, 'c'], None, dict(dollars=0, visitors=7500))
        self.whale.count_now([pk, decision, 'd'], None, dict(dollars=5, visitors=1))

        # Here's a visitor with no info -- 'A' should win by far.
        good, bad, test = self.whale.weighted_reasons(pk, decision, opts, formula='dollars/visitors')
        self.assertIn('a', good.keys())
        self.assertIn('b', bad.keys())
        self.assertIn('c', bad.keys())
        self.assertIn('d', test.keys())

        # How about when we know the country is "UK"?
        good, bad, test = self.whale.weighted_reasons(pk, decision, opts, formula='dollars/visitors',
            known_data={'country': 'uk'})
        self.assertIn('a', good.keys())
        self.assertIn('b', good.keys())
        self.assertIn('c', bad.keys())
        self.assertIn('d', test.keys())
        chosen = defaultdict(int)
        for k in range(50):
            choose = self.whale.decide(pk, decision, opts, formula='dollars/visitors',
                known_data={'country': 'uk'}, bad_idea_threshold=0, test_idea_threshold=0)
            chosen[choose] += 1
        self.assertEqual(True, chosen['b'] > 35,
            "A decision between weights .15 vs .85 should have around 43 votes for 'b', we got less than 35")


def _print_reasons(good, bad, test):
    print 'Good:', [(k, v['weight']) for k, v in good.items()]
    for opt, r in good.items():
        if r['good']:
            print 'Pro reason: ', opt, r['good']['dimension'], r['good']
        if r['bad']:
            print 'Con reason: ', opt, r['bad']['dimension'], r['bad']

    print 'Bad :', [(k, v['weight']) for k, v in bad.items()]
    for opt, r in bad.items():
        if r['good']:
            print 'Pro reason: ', opt, r['good']['dimension'], r['good']
        if r['bad']:
            print 'Con reason: ', opt, r['bad']['dimension'], r['bad']

    print 'Test:', test.keys()


if __name__ == '__main__':
    unittest.main()
