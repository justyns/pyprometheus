import collections
import json
import os
import redis

"""
A lot/some of the original code in these metrics classes were taken from/inspired by
    https://github.com/slok/prometheus-python and https://github.com/prometheus/client_python
"""

RESTRICTED_LABELS_NAMES = ('job',)
RESTRICTED_LABELS_PREFIXES = ('__',)


class Metric:
    """Base class for all metric types"""
    metric_type = 'untyped'

    def __init__(self, name, help_text,
                 base_labels=None, buckets=None):
        """
        This is the base class that all other metric types are inherited from.

        :param str name: Name of metric.  Must be alphanumeric and can contain underscores.  See http://prometheus.io/docs/practices/naming/
        :param str help_text: A short description of what this metric contains.
        :param dict base_labels: A dictionary of labels that will automatically be merged into any additional labels
                                 provided later when adding samples.
        :param list buckets: Only used for histograms.  Determines what buckets to place requests in.
        :return:
        """
        if " " in name:
            raise ValueError("metric name can not contain spaces")
        if self.metric_type not in ['untyped', 'gauge', 'counter', 'summary', 'histogram']:
            raise ValueError("metric type %s is unsupported" % self.metric_type)

        self.name = name
        self.help_text = help_text
        self.base_labels = base_labels
        if not buckets:
            # default values for the buckets
            buckets = [0.1, 0.25, 0.5, 0.75, 1, 1.5, 2]
        self.buckets = buckets  # only used by histograms
        # changing the key_prefix can break things that rely on it
        self.key_prefix = "PROM:{pid}:{type}:{name}".format(pid=os.getpid(),
                                                            name=self.name,
                                                            type=self.metric_type)
        self.redis_host = 'localhost'
        self.redis_db = 0
        self.r = redis.StrictRedis(host=self.redis_host, db=self.redis_db)
        self.register_metric()

    def register_metric(self):
        """
        Add our key to a redis set so that a collector can come by later and collect them
        """
        self.r.sadd("PROM:metric_keys", "{key} {type} {name} {help}".format(key=self.key_name(),
                                                                            help=self.help_text,
                                                                            type=self.metric_type,
                                                                            name=self.name))
        # And remove any ttl that may be set on our redis key
        # so that the collector can set all keys to expire
        # and we can automatically un-expire them
        self.r.expire(self.key_name(), -1)

    def key_name(self, labels=None):
        """
        Generates the string we should use as the redis key name.

        :param dict labels: If provided, the labels are appended to the key name
        :return str:
        """
        if not labels:
            return self.key_prefix
        else:
            return "{prefix}:{labels}".format(prefix=self.key_prefix, labels=labels)

    def set_value(self, labels, value):
        """
        Stores a sample value in redis.   If there is an existing sample with the same labels,
        it will be overwritten with the value.

        :param dict labels: Labels that will be stored for this sample
        :param int|float value: Value to be set for this sample
        :return:
        """

        if labels:
            self._label_names_correct(labels)

        r = redis.StrictRedis(host=self.redis_host, db=self.redis_db)
        r.hset(self.key_name(), self._labels(labels), value)

    def inc(self, labels, increment=1):
        """
        Increments a counter or gauge's value.   Redis's incr and hincrby commands are atomic,
        so this can safely be used between processes.

        :param dict labels: Labels that will be stored for this sample
        :param int|float increment: Amount to decrease the value by.  Must be positive.
        :return:
        :raises ValueError: If increment is negative
        """

        # Redis's incr command is atomic so we don't need to get/set this on our own
        if increment < 0:
            raise ValueError("increment can not be negative")
        # print "hincrbyfloat(%s, %r, %r)" % (self.key_name(), self._labels(labels), float(increment))
        self.r.hincrbyfloat(self.key_name(), self._labels(labels), float(increment))

    def dec(self, labels, increment=1):
        """
        Decreases the value of a gauge by increment, using redis's hincrby command.

        :param dict labels: Labels that will be stored for this sample
        :param int|float increment: Amount to increase the value by.  Must be positive.
        :return:
        :raises ValueError: If increment is negative
        """

        if increment < 0:
            raise ValueError("increment can not be negative")
        self.r.hincrby(self.key_name(), self._labels(labels), -increment)

    def get_value(self, labels):
        """
        Returns the value of a metric that matches the labels

        :param dict labels: Dictionary of labels that the metric should match
        :return:
        :raises KeyError: If there is no stored sample matching the metric name and labels
        """

        if not self.r.hexists(self.key_name(), self._labels(labels)):
            raise KeyError(labels)
        else:
            return self.r.hget(self.key_name(), self._labels(labels))

    def _labels(self, labels):
        """
        Returns a string that is json-encoded and contains the labels + global labels in a sorted dictionary.

        :param dict labels: Dictionary of labels to transform
        :return:
        """
        labels.update(**self.base_labels)
        self._label_names_correct(labels)
        return json.dumps(collections.OrderedDict(**labels), sort_keys=True)

    @staticmethod
    def _label_names_correct(labels):
        """
        Validates label names to ensure they'll be accepted by prometheus.

        :param dict labels: Dictionary of labels to check
        :return:
        :raises ValueError: If labels start with a restricted prefix, or matches a restricted name
        """

        for k, v in labels.items():
            # Check reserved labels
            if k in RESTRICTED_LABELS_NAMES:
                raise ValueError("Cannot use restricted label name %s" % k)

            # Check prefixes
            if any(k.startswith(i) for i in RESTRICTED_LABELS_PREFIXES):
                raise ValueError("Label cannot start with %r" % RESTRICTED_LABELS_PREFIXES)

        return True


class Counter(Metric):
    """
    A counter is a cumulative metric that represents a single numerical value that only ever goes up. A counter is
    typically used to count requests served, tasks completed, errors occurred, etc. Counters should not be used to
    expose current counts of items whose number can also go down, e.g. the number of currently running goroutines.
    Use gauges for this use case.
    """
    metric_type = "counter"

    def dec(self, labels, increment=1):
        """
        A counter can only go up, or be reset.

        :param labels:
        :param increment:
        :return:
        """
        raise TypeError("counters can not be decremented")


class Gauge(Metric):
    """
    A gauge is a metric that represents a single numerical value that can arbitrarily go up and down.
    Gauges are typically used for measured values like temperatures or current memory usage, but also "counts" that
    can go up and down, like the number of running goroutines.
    """
    metric_type = "gauge"


class Summary(Metric):
    """
    TODO: NOT CURRENT IMPLEMENTED

    Similar to a histogram, a summary samples observations (usually things like request durations and response sizes).
    While it also provides a total count of observations and a sum of all observed values, it calculates configurable
    quantiles over a sliding time window.
    """
    metric_type = "summary"


class Histogram(Metric):
    """
    A histogram samples observations (usually things like request durations or response sizes) and counts them in
    configurable buckets. It also provides a sum of all observed values.
    """
    metric_type = "histogram"

    def observe(self, labels, value):
        """
        Adds a new sample to this histogram.  The value will be checked against all of the configured buckets,
        and if it is less than or equal to that bucket threshold then that bucket count will be increased.
        The overall count for the metric is also increased, in addition to the sum which is the sum of all values

        :param dict labels: Labels that will be stored for this sample
        :param float value: Value to store for this sample, typically a time in microseconds or seconds.  Could also
                            be a size in bytes, kilobytes, etc
        :return:
        """
        for bucket in self.buckets:
            if float(value) < float(bucket):
                labels.update(le=bucket)
                self.inc(labels, 1)
        labels.update(le="+Inf")  # the overall hisogram+labels count
        self.inc(labels, 1)
        labels.update(le="_sum")  # the overall sum of this histogram+labels
        self.inc(labels, float(value))
        labels.pop('le', None)
