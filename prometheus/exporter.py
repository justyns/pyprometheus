from collections import OrderedDict
import json
import os
import redis


class Exporter:
    def get_all_metrics(self):
        return {}

    def format_labels(self, labels):
        if isinstance(labels, str):
            # try to json decode
            labels = json.loads(labels, object_pairs_hook=OrderedDict)
        if not labels:
            return ""
        # TODO: escape label values
        # metric_name and label_name have the usual Prometheus expression language restrictions. label_value can be any sequence of UTF-8 characters, but the backslash, the double-quote, and the line-feed characters have to be escaped as \\, \", and \n, respectively.\
        labelstr = ",".join(["{k}=\"{v}\"".format(k=k, v=v) for k, v in labels.items()])
        return "{%s}" % labelstr

    def format_value(self, value):
        """returns a float acceptable by prometheus / go"""
        if value is None:
            return "NaN"
        else:
            return float(value)

    def print_metrics(self):
        """returns text formatted metrics"""
        output = ""
        metrics = self.get_all_metrics()
        for k, v in metrics.items():
            # Print the help line
            output += "\n# HELP {name} {help}\n".format(name=v['name'], help=v['help'])
            # and the type line
            output += "# TYPE {name} {type}\n".format(name=v['name'], type=v['type'])
            for sample in v['values']:
                output += "{name}{labels} {value}\n".format(name=v['name'],
                                                          labels=self.format_labels(sample),
                                                          value=self.format_value(v['values'][sample]))
        return output


class RedisExporter(Exporter):
    def __init__(self):
        self.redis_host = 'localhost'
        self.redis_db = 0
        self.metrics_key = 'PROM:metric_keys'
        self.r = redis.StrictRedis(host=self.redis_host, db=self.redis_db)

    def list_metrics(self):
        """
        returns a list of metrics we know about (without labels or their values)
        """
        results = []
        if self.r.exists(self.metrics_key):
            keys = self.r.smembers(self.metrics_key)
            for k in keys:
                # metric_key, metric_type, metric_name, metric_help = keys.split(" ", 3)
                results.append(k.split(" ", 3))
        return results

    def get_metric(self, metric, existing_dict=None):
        """metric must be a list of 0 - redis key, 1 - type, 2 - name, 3 - help text """
        metric_key, metric_type, metric_name, metric_help = metric
        metric_dict = {
            'name': metric_name,
            'type': metric_type,
            'help': metric_help,
            'values': OrderedDict()
        }
        values = self.r.hgetall(metric_key) # new values
        print "values: %r" % values
        metric_dict['values'] = values

        if existing_dict:
            # we're updating a metric we've already seen
            print "existing dict: %r" % existing_dict
            for value in values:
                print "checking value: %r" % value
                # value = json.loads(value)
                if value in existing_dict['values']:
                    if metric_type == 'counter' or metric_type == 'histogram':
                        # Combine the values if it's a counter or histogram
                        # TODO: sort histogram buckets
                        # TODO: append _bucket to histogram bucket names
                        existing_dict['values'][value] = float(values[value]) + float(existing_dict['values'][value])
                    elif metric_type == 'gauge':
                        # use the last value we see for a gauge - # TODO: is there a better way? we could average it
                        existing_dict['values'][value] = float(values[value])
                else:
                    existing_dict['values'][value] = float(values[value])
            metric_dict['values'] = existing_dict['values']

        if metric_type == 'histogram':
            # we need to sort the values by the bucket labeled "le"
            sorted_keys = sorted([json.loads(x) for x in metric_dict['values']], key=lambda k: k['le'])
            # and then we need to store the values again json encoded
            vals = metric_dict['values']
            metric_dict['values'] = OrderedDict()
            for k in sorted_keys:
                kn = json.dumps(k, sort_keys=True)
                metric_dict['values'][kn] = vals[kn]
            # metric_dict['values'] = sorted([(json.loads(x), metric_dict['values'][x]) for x in metric_dict['values']], key=lambda k: k['le'])
            # metric_dict['values'] = [json.dumps(x) for x in metric_dict['values']]
            # metric_dict['values'] = sorted(metric_dict['values'], key=lambda k: k['le'])

        return metric_dict


    def get_all_metrics(self):
        """returns all metrics with their labels and values"""
        metrics = {}
        for item in self.list_metrics():
            metric_name = item[2]
            metric = self.get_metric(item, existing_dict=metrics.get(metric_name, None))
            metrics[metric_name] = metric
        return metrics
