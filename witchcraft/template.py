#-*- coding: utf-8 -*-

import re
import os
import binascii
import json
import uuid
from itertools import starmap
from pyparsing import *
from decimal import Decimal
from datetime import datetime, date, time
from witchcraft.utils import coalesce, chainlist, find_query_template, __query_paths

try:
    import itertools.imap as map
except ImportError:
    pass


try:
    text = unicode
except NameError:
    text = str 

try:
    long
except NameError:
    long = int

from hy.lex import parser, lexer
from hy import HyList
from hy.importer import hy_eval
from psycopg2.extensions import QuotedString as SqlString


reserved_words = []
rw_path = os.path.join(os.path.dirname(__file__), 'reserved_psql.txt')


with open(rw_path, 'r') as rwfd:
    reserved_words = rwfd.read().split(',\n')


def string_to_quoted_expr(s):
  return HyList(parser.parse(lexer.lex(s)))

    

def quote_param(value, dialect='psql'):
    #print(str(value)[0:70], type(value))

    if value is None:
        return "NULL"

    if isinstance(value, bytes):
        return "decode('%s', 'hex')::bytea" % binascii.hexlify(value).decode('ascii')

    if isinstance(value, memoryview):
        return "decode('%s', 'hex')::bytea" % binascii.hexlify(bytes(value)).decode('ascii')

    if isinstance(value, int) or isinstance(value, long):
        return str(value)

    if isinstance(value, float):
        return str(value)

    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, text):
        #value = value.replace(':',"\:")
        value = value.replace('%','%%')
        value = value.replace('\x00',' ')
        sql_string_value = SqlString(value)
        sql_string_value.encoding = 'utf-8'
        return sql_string_value.getquoted().decode("utf-8")

    if isinstance(value, str):
        #value = value.replace(':',"\:")
        value = value.replace('%','%%')
        value = value.replace('\x00',' ')
        sql_string_value = SqlString(value)
        sql_string_value.encoding = 'utf-8'
        return sql_string_value.getquoted().decode("utf-8")

    if isinstance(value, datetime):
        if dialect == 'oracle':
            return "timestamp '%s'" % value.isoformat(' ').split('.')[0]
        else:
            return "'%s'" % value.isoformat(' ')

    if isinstance(value, date):
        return "'%s'" % value.isoformat()

    if isinstance(value, time):
        return "'%s'" % str(value)

    if isinstance(value, uuid.UUID):
        return "'%s'" % str(value)

    if isinstance(value, dict):
        sql_string_value = SqlString(json.dumps(value))
        sql_string_value.encoding = 'utf-8'
        value = sql_string_value.getquoted().decode("utf-8")
        value = value.replace('%','%%')
        return value

    if isinstance(value, set):
        quote_func = lambda p: quote_param(p, dialect)
        return "(" + ','.join(map(quote_func, value)) + ")"

    if isinstance(value, tuple):
        quote_func = lambda p: quote_param(p, dialect)
        return "(" + ','.join(map(quote_func, value)) + ")"


    if isinstance(value, list):
        quote_func = lambda p: quote_param(p, dialect)

        try:
            return "(" + ','.join(map(quote_func, value)) + ")"
        except Exception as e:
            print(e)
            raise ValueError(value)

    raise ValueError("unhandled type: %s, %s" % (type(value), value))


class EscapeKeywords(object):

    def __init__(self, dialect):

        if dialect == 'mysql':
            self.quote = '`'
        else:
            self.quote = '"'

    def __call__(self, value):

        if value in reserved_words:
            return text(self.quote + value + self.quote)
        else:
            return value

class QuoteName(object):

    def __init__(self, dialect):

        if dialect == 'mysql':
            self.quote = '`'
        else:
            self.quote = '"'

    def __call__(self, value):
    
        if str(value).lower() != str(value) or re.match('[A-Za-z_][A-Za-z0-9_]\s+', value) is None:
            return text(self.quote + value + self.quote)
        else:
            return value

class Parameter(object):

    def __init__(self, token):
        self.quote = (token[0][0] == '?')
        self.name = token[0][1]

    def evaluate(self, context, dialect='psql'):
        value = context.get(self.name)

        if self.name not in context:
            raise ValueError("variable %s or not found" % self.name)

        quote_func = lambda p: quote_param(p, dialect)

        conv_func = quote_func if self.quote else EscapeKeywords(dialect)

        if isinstance(value, list) or isinstance(value, map) or isinstance(value, starmap):
            value = ', '.join(list(map(conv_func, value)))
        else:
            value = conv_func(value)

        return value


class EvalExpression(object):

    def __init__(self, token):
        self.quote = (token[0][0] == '?')
        self.expression = token[0][1]

    def evaluate(self, context, dialect='psql'):
        ctx = dict(globals())
        ctx['esckwd'] = EscapeKeywords(dialect)
        ctx['quotename'] = QuoteName(dialect)
        ctx['coalesce'] = coalesce
        ctx['chainlist'] = chainlist
        ctx['template'] = template
        ctx['starmap'] = starmap
        ctx.update(context)
        result = string_to_quoted_expr(self.expression)
        result = hy_eval(result, ctx, 'inline_hy')[0]
        quote_func = lambda p: quote_param(p, dialect)

        conv_func = quote_func if self.quote else EscapeKeywords(dialect)

        if result is None:
            result = ''

        elif isinstance(result, list) or isinstance(result, map) or isinstance(result, starmap):
            #result = list(result)
            try:
                result = ', '.join(list(map(conv_func, result)))
            except TypeError:
                raise ValueError('TypeError %s' % list(result))

            except:
                raise ValueError('Unhandled Error %s' % list(result))
        else:
            result = conv_func(result)

        return result


class Template(object):

    substatement = Combine(OneOrMore(White() | Regex("[^?:'\"]+") | Literal('??') | Literal("::") | QuotedString("'", '\\', None, False, False) | QuotedString('"', '\\', None, False, False)))

    named_parameter = Group((Literal(':') | Literal('?')) + Word(alphas+"_-", alphas+nums+"_-"))
    named_parameter.setParseAction(lambda s,l,t: Parameter(t))

    anything = Regex("[^()]*")

    lisp_expression = Forward()
    lisp_expression << Combine(Literal('(') + (Optional(anything) + ZeroOrMore (anything + lisp_expression + anything)) + Literal(')'))

    eval_expression = Group((Literal(':') | Literal('?')) + lisp_expression)
    eval_expression.setParseAction(lambda s,l,t: EvalExpression(t))

    parameter = named_parameter | eval_expression

    statement = Optional(substatement) + ZeroOrMore( parameter + substatement) + Optional(parameter)
    statement.leaveWhitespace()

    def __init__(self, tpl, dialect = 'psql'):
        self.tpl = tpl
        first_line = self.tpl.splitlines()[0] #TODO: To get dialect
        self.dialect = dialect

    def substitute(self, **context):
        acc = ''
        vec = self.statement.parseString(self.tpl)
        for e in vec:

            if isinstance(e, str):
                acc += text(e)

            else:
                acc += text(e.evaluate(context, self.dialect))

        #escape replace
        return acc.replace('??','?')


__template_cache = {}

def conv_symbol_name(s):
    s = s.lstrip(u'\ufdd0:')
    return s.replace('-','_')


def template(template_name, context = None, dialect = None):
    cache_key = template_name

    if context is not None:
        
        conv_context = {}
        for k,v in context.items():
            conv_context[conv_symbol_name(k)] = v
    else:
        conv_context = {}

    found = __template_cache.get(cache_key)

    if found is not None:
        #TODO: handle dialect when teplate is used
        return Template(found, dialect).substitute(**conv_context)

    query_tpl = find_query_template(template_name)

    if query_tpl is not None:
        __template_cache[cache_key] = query_tpl
        return Template(query_tpl, dialect).substitute(**conv_context)
    else:
        raise ValueError('Template not found')

