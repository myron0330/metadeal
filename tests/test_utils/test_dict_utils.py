# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Dicts test file
# **********************************************************************************#
import unittest

from nose_parameterized import param, parameterized

from utils.dicts import DefaultDict, CompositeDict, OrderedDict, AttributeDict


class TestDefaultDict(unittest.TestCase):

    @parameterized.expand(
        [
            param(set, (lambda x: x.add(1))),
            param(list, (lambda x: x.append(1))),
            param(dict, (lambda x: x.update({'key': 1})))
        ]
    )
    def test_default_dict(self, default, operate):
        obj = DefaultDict(default)
        operate(obj['test'])
        print '{} {} {}'.format('#' * 25, 'Default dict', '#' * 25)
        print obj, '\n'


class TestCompositeDict(unittest.TestCase):

    def test_composite_dict(self):
        obj = CompositeDict()
        obj['this']['is']['a']['fantastic']['dict'] = True
        print '{} {} {}'.format('#' * 25, 'Composite dict', '#' * 25)
        print obj, '\n'


class TestOrderedDict(unittest.TestCase):

    def test_ordered_dict(self):
        obj = OrderedDict()
        for key in xrange(5):
            obj['test{}'.format(key)] = key
        print '{} {} {}'.format('#' * 25, 'Ordered dict', '#' * 25)
        print 'Obj: ', obj
        print 'Keys: ', obj.keys()
        print 'Values: ', obj.values()
        print 'Items before popitem: ', obj.items()
        obj.popitem()
        print 'Items after popitem: ', obj.items()
        obj.pop('test3')
        print 'Obj: ', obj
        print 'Keys: ', obj.keys()
        print 'Values: ', obj.values()
        obj += {'added': 'item'}
        print 'Obj: ', obj, '\n'


class TestAttributeDict(unittest.TestCase):

    def test_attribute_dict(self):
        obj = AttributeDict()
        obj['fantasy'] = 1
        print '{} {} {}'.format('#' * 25, 'Attribute dict', '#' * 25)
        print obj['fantasy'], obj.fantasy, '\n'
