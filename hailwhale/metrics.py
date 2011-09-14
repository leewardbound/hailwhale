class Metric:
    stub = 'abstract_metric'
    name = False
    value_type = int
    def __init__(self, value=0):
        self.value = self.cast_value(value) 
    def cast_value(self, value=False):
        if not value and hasattr(self, 'value'):
            value = self.value
        return self.value_type(value)
    def format_value(self, value=False):
        return self.cast_value(value)
    @staticmethod
    def class_from_stub(stub):
        for c in [Value,Hits,Visitors]:
            if c.stub == str(stub): return c
    @staticmethod
    def parse(stub, value):
        k = Metric.class_from_stub(stub)
        if k: return k(value)
    def to_stub(self): pass
    def __unicode__(self): return self.__str__()
    def __str__(self):
        return self.name or self.stub
        
class Hits(Metric):
    stub = 'hits'
    name = 'Hits'
class Visitors(Metric):
    stub = 'visitors'
    name = 'Unique Visitors'
class Value(Metric):
    stub = 'value'
    name = 'USD'
    value_type = float
    def format_value(self, value=False):
        return '$%.2f'%self.cast_value(value)
class MetricDict(dict):
    def itermetrics(self):
        for stub, val in self.iteritems():
            m = Metric.parse(stub, val)
            if m: yield m
    def metrics(self):
        return dict([(m.name,m.value) for m in self.itermetrics()])
