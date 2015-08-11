import collections
import json
import os
import redis

# Used so only one thread can access the values at the same time

# Used to return the value ordered (not necessary byt for consistency useful)
decoder = json.JSONDecoder(object_pairs_hook=collections.OrderedDict)

RESTRICTED_LABELS_NAMES = ('job',)
RESTRICTED_LABELS_PREFIXES = ('__',)


class Metric:
    """Base class for all metric types"""
    REPR_STR = 'untyped'

    def __init__(self, name, help_text, base_labels=None, buckets=[0.1, 0.25, 0.5, 0.75, 1, 1.5, 2]):
        if " " in name:
            raise ValueError("metric name can not contain spaces")
        if self.REPR_STR not in ['untyped', 'gauge', 'counter', 'summary', 'histogram']:
            raise ValueError("metric type %s is unsupported" % self.REPR_STR)

        self.name = name
        self.help_text = help_text
        self.base_labels = base_labels
        self.buckets = buckets  # only used by histograms
        # changing the key_prefix can break things that rely on it
        self.key_prefix = "PROM:{pid}:{type}:{name}".format(pid=os.getpid(), name=self.name, type=self.REPR_STR)
        self.redis_host = 'localhost'
        self.redis_db = 0
        self.r = redis.StrictRedis(host=self.redis_host, db=self.redis_db)
        self.register_metric()

    def register_metric(self):
        # Add our key to a redis set so that a collector can come by later and collect them
        self.r.sadd("PROM:metric_keys", "{key} {type} {name} {help}".format(key=self.key_name(),
                                                                            help=self.help_text,
                                                                            type=self.REPR_STR,
                                                                            name=self.name))
        # And remove any ttl that may be set on our redis key - this way the collector can set all keys to expire
        # and we should automatically un-expire them
        self.r.expire(self.key_name(), -1)

    def key_name(self, labels=None):
        if not labels:
            return self.key_prefix
        else:
            return "{prefix}:{labels}".format(prefix=self.key_prefix, labels=labels)

    def set_value(self, labels, value):
        """ Sets a value in the container"""

        if labels:
            self._label_names_correct(labels)

        r = redis.StrictRedis(host=self.redis_host, db=self.redis_db)
        # with r.pipeline() as pipe:
        # put a watch on the key we're wanting to change, to prevent clobbering other process's data
        # TODO:  pipe.watch doesn't work for hash fields, what to do?
        # pipe.watch(key_name)
        r.hset(self.key_name(), self._labels(labels), value)

    def inc(self, labels, increment=1):
        """ Increment counter's value by increment. """

        # Redis's incr command is atomic so we don't need to get/set this on our own
        # self.r.incr(self.key_name(labels), increment)
        # self.r.zincrby(self.key_name(), labels, increment)
        if increment < 0:
            raise ValueError("increment can not be negative")
        self.r.hincrby(self.key_name(), self._labels(labels), increment)

    def get_value(self, labels):
        """ Gets a value in the container, exception if isn't present"""

        if not self.r.hexists(self.key_name(), self._labels(labels)):
            raise KeyError(labels)
        else:
            return self.r.hget(self.key_name(), self._labels(labels))

    def get(self, labels):
        """Handy alias"""
        return self.get_value(labels)

    def set(self, labels, value):
        """Handy alias"""
        return self.set_value(labels, value)

    def _labels(self, labels):
        """Returns labels json encoded/sorted + the global labels"""
        labels.update(**self.base_labels)
        self._label_names_correct(labels)
        return json.dumps(collections.OrderedDict(**labels), sort_keys=True)

    def _label_names_correct(self, labels):
        """Raise exception (ValueError) if labels not correct"""

        for k, v in labels.items():
            # Check reserved labels
            if k in RESTRICTED_LABELS_NAMES:
                raise ValueError("Labels not correct")

            # Check prefixes
            if any(k.startswith(i) for i in RESTRICTED_LABELS_PREFIXES):
                raise ValueError("Labels not correct")

        return True

    def get_all(self):
        """ Returns a list populated by tuples of 2 elements, first one is
            a dict with all the labels and the second elemnt is the value
            of the metric itself
        """
        items = self.r.hgetall(self.key_name())

        result = []
        for k, v in items.items():
            # Check if is a single value dict (custom empty key)
            if not k or k == MetricDict.EMPTY_KEY:
                key = None
            else:
                key = decoder.decode(k)
            result.append((key, self.get(k)))

        return result


class Counter(Metric):
    """ Counter is a Metric that represents a single numerical value that only
        ever goes up.
    """

    REPR_STR = "counter"


class Gauge(Metric):
    """ Gauge is a Metric that represents a single numerical value that can
        arbitrarily go up and down.
    """
    REPR_STR = "gauge"
    # TODO: incr/decr methods


class Summary(Metric):
    """
    The sample sum for a summary or histogram named x is given as a separate sample named x_sum.
The sample count for a summary or histogram named x is given as a separate sample named x_count.
Each quantile of a summary named x is given as a separate sample line with the same name x and a label {quantile="y"}.
The buckets of a histogram and the quantiles of a summary must appear in increasing numerical order of their label values (for the le or the quantile label, respectively).


    """
    REPR_STR = "summary"


class Histogram(Metric):
    REPR_STR = "histogram"

    def observe(self, labels, value):
        for bucket in self.buckets:
            if float(value) < float(bucket):
                labels.update(le=bucket)
                self.inc(labels, 1)