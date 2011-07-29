### HailWhale : Open-source, web-scale, nested-parameter hit counting. Whew. ###

HailWhale is a package for quickly counting things with many properties.
Modeled after Twitter's Rainbird demo, logging servers count events
and denormalize them into summary data for user-defined time periods
that can be quickly turned into graphs of key performance metrics.

It has two primary components -- 
  + One or more Hail servers, collects inbound events
    - uWSGI server collects incoming JSON data
    - process hits with Celery + RabbitMQ
    - runs independantly of other Hail servers, scales linearly with nodes
    - hailwail.py cronjob dumps hits to Whale server
  + The Whale server, which is a connector to any database suitable for storing your data
    - Currently only Redis supported
    - Hbase, Mongo, Cassandra, MySQL planned

The best examples are code; here's counting hits to a webpage:
  # settings.py
  # Define our graph periods
  PERIODS = [
    (15, 3600*3), # Every 15 seconds for the last 3 hours
    (3600, 3600*24*7), # Every hour for the last week
    (3600*24, 3600*24*365), # Every day for the last year
  ]
  HAIL_SERVER='redis://localhost:11211'
  WHALE_STORAGE='hbase://localhost:9090'
  
  # Table must have column families 'meta', 'values'
  DEFAULT_TABLE='analytics'


  # my_site.wsgi
  def main_page(request):
    visitor_id = request.user.id

    # A hierarchy of traits to log
    update_params = ['pageviews', 'site.com', 'site.com/main', visitor_id]
    
    # The metrics we'll be tracking
    update_values = {
      hits = 1, # Increment a value
      time_on_page = 25, # Or store things to aggregate
      visitors = 'is_unique', # Magic strings get replaced
      }

    HailWhale.log(update_params, update_values)
    return "<h1>Hello, Whale</h1>"

In effect, this will increment counts for each of:
  [
    ('pageviews', )
    ('pageviews', 'site.com'),
    ('pageviews', 'site.com', full_url),
    ('pageviews', 'site.com', full_url, visitor_id),
  ]

And we could then get plotpoints for a graph for each with the following:
  
  total_hits_this_month = HailWhale.dataset(
              ['pageviews'],
              value='hits',
              period=settings.PERIOD[-1],
              start_at=datetime.now() - timedelta(months=1),
      )
  #=> [(datetime, value), (datetime, value), (datetime, value), ... ]

  timeonpage_per_page_for_sitecom_this_week = \
    HailWhale.dataset( ['pageviews', 'site.com'],
              depth=1, # include 1 layer beneath 'site.com'
              value='time_on_page',
              period=settings.PERIOD[1])
  #=> {'site.com/main': [(datetime,value),(datetime,value) ... ],
  #    'site.com/about': [(datetime,value),(datetiem,value) ...] }

              
              
=== About The Project ===
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
