import re

def register_accessor(cls):
    reg_accessors[cls.hook] = { 
        'class': cls,
        'regex': re.compile(cls.regex)
    }   
    return cls 

reg_accessors = {}
