# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
import re
from string import Template


VALUE_OBJECT_TEMPLATE = Template("""class ${object_name}(${bases}):

    __slots__ = [
${slots_list}
    ]

    def __init__(self, ${parameters}):
${attributes}
""")


def generate_class_object(object_name, attributes,
                          template=None, bases=None,
                          with_dump=False, dump_file=None):
    """
    Generate class object.

    Args:
        object_name(string): object name
        attributes(list or dict): attribute
        template(string): class object template
        bases(string or iterable): object bases
        with_dump(boolean): whether to dump code
        dump_file(string): dump file

    Returns:
        string: code sample
    """
    template = template or VALUE_OBJECT_TEMPLATE
    attributes = attributes if isinstance(attributes, dict) else {_: None for _ in attributes}
    slots_list = '\n'.join(["        '{}',".format(_) for _ in attributes])
    parameters = ', '.join(['{}={}'.format(key, value) for key, value in attributes.iteritems()])
    attributes = '\n'.join(['        self.{} = {}'.format(_, _) for _ in attributes])
    if isinstance(bases, (str, unicode)):
        bases = [bases]
    elif isinstance(bases, (list, tuple, set)):
        bases = list(bases)
    bases = ', '.join(bases) if bases is not None else 'object'
    kwargs = {
        'object_name': object_name,
        'slots_list': slots_list,
        'parameters': parameters,
        'attributes': attributes,
        'bases': bases,
    }
    template = template.safe_substitute(**kwargs)
    if with_dump:
        dump_code_to_file(template, file_name=dump_file)
    return template


def dump_code_to_file(code, file_name=None, open_style='a'):
    """
    Dump code to file.

    Args:
        code(string): code
        file_name(string): file name
        open_style(string): file open style,
                            'a': append content;
                            'w': cover content.
    """

    with open(file_name, open_style) as temp_file:
        temp_file.write('\n\n')
        temp_file.write(code)


def hump_to_underline(hump_str):
    """
    Transfer hump to underline

    Args:
        hump_str(string): hump code string
    """
    p = re.compile(r'([a-z]|\d)([A-Z])')
    sub = re.sub(p, r'\1_\2', hump_str).lower()
    return sub


def underline_to_hump(underline_str):
    """
    Transfer underline to hump

    Args:
        underline_str(string): underline code string
    """
    sub = re.sub(r'(_\w)', lambda x: x.group(1)[1].upper(), underline_str)
    return sub


__all__ = [
    'VALUE_OBJECT_TEMPLATE',
    'generate_class_object',
    'dump_code_to_file',
    'hump_to_underline',
    'underline_to_hump'
]


if __name__ == '__main__':
    test_object_name = 'Tracker'
    test_attribute_list = ['basic_info', 'interface_info', 'error_info']
    bases_list = 'object'
    test_code = generate_class_object(test_object_name, test_attribute_list,
                                      bases=bases_list, with_dump=True,
                                      dump_file='test.py')
