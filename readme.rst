HailWhale
=========
If you like hacking, testing and reading source code, start with tests.py and
then catch up on the last few weeks of commit logs. You'll be smoking the good
stuff in no time.

**What Is It?**

Laymen-ish: Real-time counting of rolled-up multi-dimensional metric data via HTTP service.

I needed a timeseries database that could count a lot of structured data, but
also help to find what factors might cause different metrics to change.

Specifically, I needed to be able to say "Give me all the counts of Impressions,
Clicks, Visitors, Sales and Revenue, grouped by Country, sorted by
Revenue/Visitors."

That last part is the key, sorting by a ratio of two graphs, because it lets us
do recommendation services. And I needed the decisions to be delivered quickly,
in just milliseconds, so everything had to be kept mostly denormalized.

HailWhale uses Redis, instead of cassandra, riak or hbase (I tried to build adapters,
but like my dad always says, "fuck'em!"), and with tests on Redis Cluster it has been
horizontally scalable and performant.

**Why?**

The primary application has been of course the advertising industry, but I've re-applied
hailwhale to billing software (choosing best merchant processors for a given
card), to design LED lighting algorithms (optimize for upvotes like ElectricSheep), 

It is not an attempt to out-perform other TSDBs, but an attempt to provide novel
features and an emphasis on the recommendation services and keeping the service
simple and easy to use (integrating tightly to python and django, but still
available over HTTP).

It is here because I need it and not because I really want you to enjoy it, I
promise. I don't care if you don't like it or me or my lack of documentation.


**To use it**

Fire HTTP GET requests to log **Events** from any language, as image or javascript pixels, from your API endpoints, from collectd or anywhere else.

Events can be optionally tagged with **Dimensions**, which are like properties (and can be nested!), and each Event has some **Metrics**, or measurable counting data.

For example, let's say you need to count today's revenue from various income streams and put a fancy graph in your admin panel. From an image tag on your ThankYou page, you trigger these URLs (actually loaded by the browser client, in this case, WHICH IS FINE) -- ::

    # Sold $200 in services
    http://.../count?pk=test&dimensions=services&metrics={"dollars": 200} 
    # Bought $2000 in advertising
    http://.../count?pk=test&dimensions=advertising&metrics={"dollars": -2000} 
    # Sold a $product_id for $500
    http://.../count?pk=test&dimensions={"sales": $product_id}&metric={"dollars": 500}

Notice that in the third example, the dimensions are nested. Now, using the jQuery widget, you can add a graph to your admin panel that will show "Overall Dollars", as well as any dimensions that exceed 10% of the total revenue stream (10% is the default threshold). Additionally, you can get a graph of "Sales Overall", which would also show any $product_id that represented 10% or more of the sales. **More additionally still**, you can get a graph of the average revenue per sale,
because hailwhale adds an extra metric {hits: 1} to each event. Then we can ask
for metric="dollars/hits" and we'll have a graph of dollar-value-per-transaction.

For each dimension/metric combination, hailwhale provides graphs and summary data, at whatever roll-up intervals you want, via HTTP/JSON or with the provided jQuery plugin.

On the backend, Hailwhale is composed of two servers --

+ The hail server is optional, and designed to quickly collect incoming events in high-traffic scenarios. Hail depends on Redis and Bottle.py.
  **UPDATE** Hail server has not actually really been used in years! It's
  probably still fine to use it, but I've switched to mostly using rabbitmq to
  handle incoming traffic. Whale still works the same without it, in this regard.

+ The whale server is required. It provides TSDB read/write wrappers, and allows for directly counting data when used without a Hail server.

**Making Decisions**
    Hey, we have all these graphs for Visitors by Country,
    now can we choose the best of our 3 page variations ['a', 'b', 'c'] to recommend one
    based on this visitors dimensions? Yes, yes we can -- ::
    # Log our decisions
    http://.../count_decision?pk=PageVariation&option=b&dimensions={"country": "US"}&metric={"spent": .50}
    # And log our successes, of course ;)
    http://.../count_decision?pk=PageVariation&option=b&dimensions={"country": "US"}&metric={"earned": 25}
    # Now choose one from our historical data
    http://.../decide?pk=PageVariation&options=['a', 'b', 'c']&dimensions={"country": "US"}&by_metric=spent/earned

**Python Integration**

Grab the source ::
    git clone github.com/linked/hailwhale

Include it ::
     # This class is the main entry point into all the hailwhale functions
     # You can track counts, retrieve counts as totals, or retrieve counts as
     # plotpoints
     from hailwhale.whale import Whale

     # Optional, but includes some extra helpers for working with our periods
      from hailwhale import periods

     # Let's log some data
     Whale.count_now('my_counter', metrics={'hits': 1})

     # How many hits today? 1. 1 hit. Gooood dooog.
     assert(Whale.total('my_counter', metric='hits', period='today') == 1)

     # Count something else probably
     Whale.count_now('my_counter', metrics={'sales': 1, 'revenue': 5})

     # Now we want some graph. The men in suits always want more graph :(
     pps = Whale.plotpoints('my_counter', metrics=['hits','sales','revenue/hits'], period='mtd')

     # we all gonna be out the job
     print pps['my_counter']['revenue/hits'].keys()[:3]
     { '1/1/11': 0, '1/2/11': 0, '1/3/11': 5.0}

     # you can also nest your PKs, and include a dimension to further subdivide your stats
     Whale.count_now(['Product', '123'], metric={'revenue': 10.0, 'sales': 1}, dimensions={'country': 'US', 'device': 'Mac OS X'})

     # Get some numbers for a dashboard
     totals = Whale.totals(['Product', '123'], metric=['revenue', 'sales'], period='1d:1mo')

     # Now get plotpoints for graphs of all countries, by revenue/visitor
     plotpoints = Whale.plotpoints(['Product', '123'], dimensions='country', metric=['revenue/visitor'], depth=1)
     
     # And I believe you mentioned some recommendation utilities?
     # What if we showed US visitor an english page?
     Whale.count_decision(['Product', '123'], decided={'language': 'EN'},
          metric={'revenue': 500, 'visitors': 100}, dimensions={'country': 'US', 'device': 'Mac OS X'})
     # Then US visitors a spanish page?
     Whale.count_decision(['Product', '123'], decided={'language': 'SP'},
          metric={'revenue': 0, 'visitors': 100}, dimensions={'country': 'US', 'device': 'Mac OS X'})

     # Hey we have a guy from US, which page should we show him?
     Whale.decide(['Product', '123'], options={'language': ['EN', 'SP']},
          known_dimensions={'country': 'US', 'device': 'Mac OS X'})





Deployment
==========
Just run a Redis server like normal. HailWhale creates all it's own keys. If you run into scaling issues,
add more machines, and use the Redis Cluster "{mustache}" clustering key notation to shard your primary keys.

License and Terms
=================
I do not suggest that anyone should try to use this, unless they are cautious and experimental and adventerous.
They would be at their own risk, and going against my advice in proceeding.
