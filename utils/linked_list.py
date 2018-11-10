# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Data structure utils
# **********************************************************************************#


def recursive(formula, formatter=(lambda x: None)):
    def decorator(func):
        def _recursive(node, *args, **kwargs):
            if node.tail:
                return formula(_recursive(node.tail, *args, **kwargs), formatter(node))

            return func(node, *args, **kwargs)

        return _recursive

    return decorator


def forward(formula, formatter=(lambda x: None)):

    def decorator(func):

        def _forward(node, *args, **kwargs):
            result = func(node, *args, **kwargs) if node else None
            while node:
                tail_node = node.tail
                if node.tail:
                    result = formula(result, formatter(tail_node))
                node = tail_node
            return result

        return _forward

    return decorator


def traversal(returnable=True):

    def decorator(func):
        def _traversal(node, *args, **kwargs):
            while node:
                func(node, *args, **kwargs)
                node = node.tail

        def _traversal_returnable(node, *args, **kwargs):
            result = list()
            while node:
                result.append(func(node, *args, **kwargs))
                node = node.tail
            return result
        return _traversal_returnable if returnable else _traversal
    return decorator


class Node(object):
    def __init__(self, obj, head=None, tail=None):
        self.head = head
        self.tail = tail
        self.obj = obj

    def append(self, node):
        self.tail = node
        node.head = self.tail


class LinkedList(object):

    def __init__(self, link_head=None, link_tail=None):
        self.link_head = link_head
        self.link_tail = link_tail

    def __add__(self, other):
        assert isinstance(other, LinkedList), 'Invalid item to add.'
        self.link_tail = self.link_head if self.link_tail is None else self.link_tail
        self.link_tail.tail = other.link_head
        self.link_tail = other.link_tail
        return self

    def __radd__(self, other):
        return self.__add__(other)

    def append(self, node):
        if not self.link_head:
            self.link_head = self.link_tail = node
        else:
            node.head = self.link_tail
            self.link_tail.tail = self.link_tail = node

    def extend(self, nodes):
        for node in nodes:
            self.append(node)

    def delete(self, node):
        if not node.head and not node.tail:
            self.link_head = self.link_tail = None
        elif not node.head and node.tail:
            self.link_head = node.tail
            node.tail.head = None
        elif node.head and not node.tail:
            node.head.tail = None
            self.link_tail = node.head
        else:
            node.head.tail = node.tail
            node.tail.head = node.head

    def recursive(self, formula, formatter):

        @recursive(formula, formatter)
        def executor(node):
            return formatter(node)

        return executor(self.link_head)

    def forward(self, formula, formatter):

        @forward(formula, formatter)
        def executor(node):
            return formatter(node)

        return executor(self.link_head)

    def traversal(self, func, *args, **kwargs):

        @traversal(returnable=True)
        def executor(node, *inner_args, **inner_kwargs):
            return func(node, *inner_args, **inner_kwargs)

        return executor(self.link_head, *args, **kwargs)

    def get_length(self):
        if not self.link_head:
            return 0
        return self.forward(formula=(lambda x, y: x + y), formatter=(lambda x: 1))

    def get_length_by_recursive(self):
        if not self.link_head:
            return 0
        return self.recursive(formula=(lambda x, y: x + y), formatter=(lambda x: 1))


if __name__ == '__main__':
    a = Node('000001.XSHE')
    b = Node('600000.XSHG')
    c = Node('000002.XSHE')
    d = Node('IFM0')
    e = Node('IHM0')
    link = LinkedList()
    link.extend([a, b, c, d, e])
    print link.get_length_by_recursive(), link.get_length()
    recursive_value = link.recursive(formula=(lambda x, y: x | y), formatter=(lambda x: {x.obj}))
    forward_value = link.forward(formula=(lambda x, y: x | y), formatter=(lambda x: {x.obj}))
    print recursive_value == forward_value
