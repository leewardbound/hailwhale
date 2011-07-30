HailWhale
=========
**What Is It?**
Real-time counting of rolled-up multi-dimensional metric data via HTTP service.

**OK, now in english?** Live graphs of events happening in real-time, for any measurable things you want to measure, grouped by any properties you want to define about these events.

Fire GET requests to log **Events**. Events can be optionally tagged with **Dimensions**, which are like properties (and can be nested!), and each Event has some **Metrics**, or measurable counting data.

For example, let's say you need to count today's revenue from various income streams and put a fancy graph in your admin panel. From the proper points in your sales and services software, you insert HTTP calls to send Events like these::

    # Sold $200 in services
    http://.../count?dimension=services&metric={"dollars": 200} 
    # Bought $2000 in advertising
    http://.../count?dimension=advertising&metric={"dollars": -2000} 
    # Sold a $product_id for $500
    http://.../count?dimensions={"sales": $product_id}&metric={"dollars": 500}

Notice that in the third example, the dimensions are nested. Now, using the jQuery widget, you can add a graph to your admin panel that will show "Overall Dollars", as well as any dimensions that exceed 10% of the total revenue stream (10% is the default threshold). Additionally, you can get a graph of "Sales Overall", which would also show any $product_id that represented 10% or more of the sales. **More additionally still**, you can get a graph of the average revenue per sale,
because hailwhale adds an extra metric {hits: 1} to each event. Since hailwhale
lets you perform transformations on metrics in real-time, graphing e.g. the
click-through ratio on a page is as simple as tracking pageviews with one
metric, and clickthroughs with another

For each dimension/metric combination, hailwhale provides graphs (flot) and summary data, at whatever roll-up intervals you want, via HTTP/JSON or with the provided jQuery plugin.

On the backend, Hailwhale is composed of two servers --

+ The hail server is optional, and designed to quickly collect incoming events in high-traffic scenarios. Hail depends on Redis and Bottle.py.

+ The whale server is required. It provides graphs, and allows for directly counting data when used without a Hail server. The whale stores data into a large datastore. Currently Redis is supported, MongoDB and cassandra coming soon.
              
About
=====
I built this after studying a presentation on Rainbird by Brian Weil 
(of Twitter), and re-using a lot of recent work I've done in
parameterized hit counting.

Full credit to Twitter for the inspiriation, and my project name (a pun 
on both the name "Rainbird" and their classic downtime logo).

Rainbird looked awesome I knew I had to have it, but after 3 months
of waiting on release, I proceeded to roll my own solution. Now I
can count things at webscale without losing my mind, if you know what I mean.

I'm using this in production at http://series.oftubes.com which is still
pre-beta. In addition to benchmarks, I'm trusting it to count my own live 
data for a (unrelated) marketing campaign, and it's good enough for me.
Use at your own risk.
