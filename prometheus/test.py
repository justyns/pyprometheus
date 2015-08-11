import os
import metrics as m
import exporter
import json

test_counter = m.Counter("test_counter", "this is a test counter", {'host': os.uname()[1]})
labels = {'somelabel': 123123}
test_counter.inc(labels)
test_counter.inc(labels, 44)
labels = {'someOtherLabel with a space': 402}
test_counter.inc(labels)
test_counter.inc(labels)
test_counter.inc(labels)
print test_counter.get(labels)

test_http = m.Counter("test_counter_http_requests", "this is a test http request counter", {'host': os.uname()[1]})
test_http.inc({
    'endpoint': '/',
    'code': 200
})
test_http.inc({
    'endpoint': '/someurl/that/doesnt/exist',
    'code': 404
})
test_http.inc({
    'endpoint': '/someurl/that/requires/auth',
    'code': 401
})


test_gauge = m.Gauge("test_gauge", "this is a test gauge", {'host': os.uname()[1]})
labels['label2'] = "something that is a string"
test_gauge.set(labels, 59000)
print test_gauge.get(labels)

test_gauge = m.Gauge("test_gauge_twice", "this is another test gauge", {'host': os.uname()[1]})
labels['label2'] = "stillastring"
labels['label3'] = "blah!!!"
test_gauge.set(labels, 9000)
test_gauge.set(labels, 9001)
test_gauge.set(labels, 1000)
test_gauge.set(labels, 3030)
print test_gauge.get(labels)


test_hist = m.Histogram("test_histogram", "this is a test histogram thing", base_labels={'label1': "some value here"})
labels = {'endpoint': '/', 'code': 200}
test_hist.observe(labels, 200)
test_hist.observe(labels, 1)
test_hist.observe(labels, 0.0001)
test_hist.observe(labels, 0.51)
print test_hist.get(labels)

test_hist2 = m.Histogram("test_histogram2", "this is another test histogram", base_labels={'label50': "some value here"})
labels = {'endpoint': '/login', 'code': 401}
test_hist2.observe(labels, 34)
test_hist2.observe(labels, 1)
test_hist2.observe(labels, 0.0001)
test_hist2.observe(labels, 0.005)
test_hist2.observe(labels, 0.01)
test_hist2.observe(labels, 0.51)
print test_hist2.get(labels)

exp = exporter.RedisExporter()
# print "We know about these keys:"
# print exp.list_metrics()
print "and these are the values"
print json.dumps(exp.get_all_metrics(), indent=1)

print "\n\nand this is the text output\n"
print exp.print_metrics()
