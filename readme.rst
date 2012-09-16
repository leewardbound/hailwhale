HailWhale
=========
**STATUS UPDATE**
HailWhale is still in development. But for the most part, the readme and the core docs are TERRIBLY out of date. These things move fast.

If you need hand holding, get out and go home. This project isn't for you yet.

If you like hacking, testing and reading source code, start with tests.py and
then catch up on the last few weeks of commit logs. You'll be smoking the good
stuff in no time.

**What Is It?**
Real-time counting of rolled-up multi-dimensional metric data via HTTP service.

**OK, now in english?** Live graphs of events happening in real-time, for any measurable things you want to measure, grouped by any properties you want to define about these events.


**To use it**

Fire HTTP GET requests to log **Events** from any language, as embedded image pixels, API callback URLs or anything you please. Events can be optionally tagged with **Dimensions**, which are like properties (and can be nested!), and each Event has some **Metrics**, or measurable counting data.

For example, let's say you need to count today's revenue from various income streams and put a fancy graph in your admin panel. From an image tag on your ThankYou page, you trigger these URLs (actually loaded by the browser client, in this case) -- ::

    # Sold $200 in services
    http://.../count?dimensions=services&metrics={"dollars": 200} 
    # Bought $2000 in advertising
    http://.../count?dimensions=advertising&metrics={"dollars": -2000} 
    # Sold a $product_id for $500
    http://.../count?dimensions={"sales": $product_id}&metric={"dollars": 500}

Notice that in the third example, the dimensions are nested. Now, using the jQuery widget, you can add a graph to your admin panel that will show "Overall Dollars", as well as any dimensions that exceed 10% of the total revenue stream (10% is the default threshold). Additionally, you can get a graph of "Sales Overall", which would also show any $product_id that represented 10% or more of the sales. **More additionally still**, you can get a graph of the average revenue per sale,
because hailwhale adds an extra metric {hits: 1} to each event. Then we can ask
for metric="dollars/hits" and we'll have a graph of dollar-value-per-transaction.

For each dimension/metric combination, hailwhale provides graphs and summary data, at whatever roll-up intervals you want, via HTTP/JSON or with the provided jQuery plugin.

On the backend, Hailwhale is composed of two servers --

+ The hail server is optional, and designed to quickly collect incoming events in high-traffic scenarios. Hail depends on Redis and Bottle.py.

+ The whale server is required. It provides graphs, and allows for directly counting data when used without a Hail server. The whale stores data into a large datastore. Currently Redis is supported, MongoDB and cassandra coming soon.

To sweeten the deal, we support a couple peices of magic, though most of them
are still being tested and tuned --
  + "Spy Logs" -- A rotating list of (default 1000) events that have passed
    through the Event, so you can show recent actions with their dimensions. To
    use Spy Logs, take a look at the source for Hail.py
  + Encrypted Pixels -- It's not secure to put all those parameters in a forward
    facing URL; if you're trying to put a counting pixel on a public page, you
    can encrypt the URL so that nobody can mess with it.
  + DECISION MAKING -- My crown jewel, still being tuned heavily, see the unit
    tests. Hey, we have all these graphs for Visitors by Country,
    now can we choose the best of our 3 page variations ['a', 'b', 'c'] to serve
    them based on this visitors dimensions? Yes, yes we can -- ::
    # Choose from our historical data
    http://.../decide?pk=PageVariation&options=['a', 'b', 'c']&dimensions={"country": "US"}&by_metric=spent/earned
    # Log our decisions
    http://.../count_decision?pk=PageVariation&option=b&dimensions={"country": "US"}&metric={"spent": .50}
    # And log our successes, of course ;)
    http://.../count_decision?pk=PageVariation&option=b&dimensions={"country": "US"}&metric={"earned": 25}

Test Server
===========
OSX::

    brew install redis
    git clone git://github.com/linked/hailwhale.git
    cd hailwhale
    sudo python setup.py
    python hailwhale/wsgi.py

Ubuntu 11.10::
 
    sudo apt-get install redis
    git clone git://github.com/linked/hailwhale.git
    cd hailwhale
    sudo python setup.py
    python hailwhale/wsgi.py

Ubuntu 10.04 i386::

        wget -O redis.deb http://ftp.us.debian.org/debian/pool/main/r/redis/redis-server_2.4.5-1_i386.deb
        wget -O libjemalloc-dev.deb http://ftp.us.debian.org/debian/pool/main/j/jemalloc/libjemalloc-dev_2.2.5-1_i386.deb
        wget -O libjemalloc1.deb http://ftp.us.debian.org/debian/pool/main/j/jemalloc/libjemalloc1_2.2.5-1_i386.deb
        sudo dpkg -i libjemalloc1.deb
        sudo dpkg -i libjemalloc-dev.deb
        sudo dpkg -i redis.deb
        # Continue 11.10 instructions

Ubuntu 10.04 amd64::

        wget -O redis.deb http://ftp.us.debian.org/debian/pool/main/r/redis/redis-server_2.4.5-1_amd64.deb
        wget -O libjemalloc-dev.deb http://ftp.us.debian.org/debian/pool/main/j/jemalloc/libjemalloc-dev_2.2.5-1_amd64.deb
        wget -O libjemalloc1.deb http://ftp.us.debian.org/debian/pool/main/j/jemalloc/libjemalloc1_2.2.5-1_amd64.deb
        sudo dpkg -i libjemalloc1.deb
        sudo dpkg -i libjemalloc-dev.deb
        sudo dpkg -i redis.deb
        # Continue 11.10 instructions

Deployment
==========

Ubuntu::

    pip install supervisor
    sudo vim /etc/supervisord.conf
    ADD THESE LINES, TWEAK TO FIT:
      [program:hailwhale]
        command=/usr/bin/python /path/to/hailwhale/hailwhale/wsgi.py
        numprocs=1
        user=www-data
        autostart=true
        autorestart=true
        stdout_logfile=/var/log/hailwhale.log
        redirect_stderr=true
        startsecs = 5
        stopwaitsecs = 5

Done :) if port 8085 is exposed, you can access hailwhale from it.
If 8085 is not exposed, you should setup a local reverse proxy. I like to use
the following nginx config inside my server {} block::

      upstream hailwhale {
          server 127.0.0.1:8085 fail_timeout=1;
      }
      server {
          listen 80; 
          server_name  hw.lwb.co;
          proxy_redirect off;
          location / { 
            // Fix the host name for hailwhale
            proxy_set_header Host $host;
            // Sites you want to be able to include cross-domain hailwhale graphs from
            proxy_set_header Access-Control-Allow-Origin http://hw.lwb.co;
            proxy_set_header Access-Control-Allow-Origin http://lwb.co;
            // If you set too many sites above, you have to increase these numbers below
            proxy_headers_hash_max_size 1024;
            proxy_headers_hash_bucket_size 256;
            proxy_pass http://hailwhale;
            break;
          }   
       }

              
About
=====
I built this after studying a presentation on Rainbird by Brian Weil 
(of Twitter), and re-using a lot of recent work I've done in
parameterized hit counting.

Full credit to Twitter for the inspiriation, and my project name (a pun 
on both the name "Rainbird" and their classic downtime logo).

Rainbird looked awesome I knew I had to have it, but after 5 months
of waiting on release, I proceeded to roll my own solution. Now I
can count things at webscale without losing my mind, if you know what I mean.

I'm using this in production on lots of sites.
In addition to benchmarks and performance data, I'm trusting it to count my own live 
data for marketing campaigns, meaning I'm trusting dollars on it, and it's good enough for me.
Use at your own risk.

Credits
=======
HailWhale was almost entirely coded by yours truly, Leeward Bound, with very
little outside assistance. But some names need mentioning and thanks need giving

  + Mike and WhatRunsWhere.com, for paying me cash for some custom mods
  + Mattseh, for assisting in some of the early WSGI code
  + Every deadbeat client that still owes me money, shit's fuel for my fire.
