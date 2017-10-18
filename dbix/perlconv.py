
from __future__ import print_function

import os, sys, re

header = """
from dbix import builder

"""

footer = ''

re_comment = re.compile(
	r"""(?P<nl>[\r\n])=\S+(?P<comment>.*?)\n=cut""", re.UNICODE|re.DOTALL)
re_comment_tag = re.compile(r"^=\S+", re.UNICODE|re.MULTILINE)
re_use = re.compile(r'^use.+$', re.UNICODE|re.MULTILINE)
re_our = re.compile(r'^our\s+\$(.+?);$', re.UNICODE|re.MULTILINE)
re_my = re.compile(r'^my\s+(.+?)$', re.UNICODE|re.MULTILINE)
re_if = re.compile(r'^\s*if\s+(.+?);', re.UNICODE|re.MULTILINE)
re_qw = re.compile(r'qw\s*\((.+?)\)', re.UNICODE|re.MULTILINE)
re_package = re.compile(
	r'^package\s+(?P<package_name>.+?);$', re.UNICODE|re.MULTILINE)
re_package_invoke = re.compile(r'__PACKAGE__\s*->\s*', re.UNICODE|re.MULTILINE)
re_one = re.compile(r'^\d\s*;\s*$', re.UNICODE|re.MULTILINE)
re_member = re.compile(r'=>', re.UNICODE|re.MULTILINE)
re_funcref = re.compile(r'\\\&(\S+)', re.UNICODE|re.MULTILINE)
re_array = re.compile(r'@(\S+)', re.UNICODE|re.MULTILINE)
re_hash = re.compile(r'%(\S+)', re.UNICODE|re.MULTILINE)
#re_dict = re.compile(r'\bdict\b', re.UNICODE)
re_dict_only = re.compile(r'\b(dict\()', re.UNICODE)
re_dict_start = re.compile(r'{', re.UNICODE|re.MULTILINE)
dict_par = u'dict('
add_columns_par = u'add_columns('
re_dict_end = re.compile(r'}', re.UNICODE|re.MULTILINE)
re_add_columns = re.compile(r'add_columns\s*?\(', re.UNICODE|re.DOTALL)
re_add_columns_only = re.compile(r'\b(add_columns\()', re.UNICODE)
re_fix_kw = re.compile(
	r"""(?P<func>has_many|belongs_to)\s*\(\s*['"]?(?P<variable>\S+?)['"]?\s*=\s*['"]?(?P<cls>.*?)['"]?\s*,(?P<rem>.*?)\);""", 
	re.UNICODE|re.DOTALL)

re_string_quoted = re.compile(r'"(?:\\.|[^"\\])*"', re.UNICODE|re.DOTALL)
re_string_aposed = re.compile(r"'(?:\\.|[^'\\])*'", re.UNICODE|re.DOTALL)
re_string_id = re.compile(r'[a-z_A-Z][a-z_A-Z0-9]*', re.UNICODE|re.DOTALL)
re_string_expr = re.compile(r'[^=,)]+?', re.UNICODE|re.DOTALL)

saved_words = ['class', 'type']

#re_saved_words = dict([
#	(word, re.compile(r'\b%s\b' % word, re.UNICODE)) for word in saved_words
#])

cls = None
__sourcepath__ = None
__load_namespaces__ = 1

def perlconvert(sourcepath, text, with_dict=False, in_tree=0):
	global __sourcepath__
	__sourcepath__ = sourcepath
	text = re.sub(re_comment, clean_comment, text)
	text = re.sub(re_add_columns, add_columns_par, text)
	text = re.sub(re_package, class_build, text)
	text = re.sub(re_use, u'#\g<0>', text)
	text = re.sub(re_our, u'\g<1>', text)
	text = re.sub(re_package_invoke, u'%s.' % cls, text)
	text = re.sub(re_one, u'', text)
	text = re.sub(re_funcref, u'\g<1>', text)
	text = re.sub(re_array, u'\g<1>', text)
	text = re.sub(re_hash, u'\g<1>', text)
	text = re.sub(re_member, u'=', text)
	if with_dict:
		text = re.sub(re_dict_start, dict_par, text)
		text = re.sub(re_dict_end, u')', text)
		text = dict_parse(text, dict_par, re_dict_only)
		text = dict_parse(
			text, add_columns_par, re_add_columns_only, add_dict=True)

	text = re.sub(re_my, u'\g<1>', text)
	text = re.sub(re_if, u'', text)
	text = re.sub(re_qw, qw_sub, text)
	text = re.sub(re_fix_kw, fix_kw, text)

	return text + '\n' + footer


def qw_sub(matchObj):
	return ', '.join([
		'"%s"' % word.replace('"', '\"') for word in matchObj.group(1).split()
	])


def dict_parse(text, dict_par, spliter, add_dict=False):
	exprs = [
		(re_string_quoted, 2), 
		(re_string_aposed, 2), 
#		(spliter, 2), 
		(re_string_id, 0), 
		(re_string_expr, 1), 
	]
	blocks = {
		u'[': (u']', [], u''), 
		u'(': (u')', [], u''), 
		u'{': (u'}', [], u''), 
		dict_par: (u')', [dict_par, u')', u'=', u','], u'=,)'), 
	}
#	print('+++++++++ blocks', blocks, file=sys.stderr)
	dict_end, dict_skip, dict_popper = blocks[dict_par]
	dict_chunks = re.split(spliter, text)
	text1 = u''
	need_parse = True
	while need_parse:
		need_parse = 0
		dict_style = None
		for c, chunk in enumerate(dict_chunks):
			if chunk == dict_par and c + 1 < len(dict_chunks):
				chunk = dict_chunks[c + 1]
				dict_parts = list()
				offset = 0
				kv = list()
				kv_is_id = 0
				block_stack = [dict_par]
				parsed = False
				while offset < len(chunk):
					if chunk[offset].isspace():
						offset += 1
						continue
					ch = None
					for d, (expr, is_id) in enumerate(exprs):
						m = expr.match(chunk, offset)
						if m is not None:
							part = m.group(0)
							offset += len(part)
							part = part.strip()
							if part in saved_words:
								is_id = 1
							ch = (part, is_id)
							parsed = ch[0] != dict_par
							break
					else:
						if offset < len(chunk):
							ch = (chunk[offset], 1)
							offset += 1
#							print('+++++++++ other', kv[-1], file=sys.stderr)

					if ch[0] in blocks:
						block_stack.append(ch[0])

					blocks_top = block_stack[-1]
					block_end, block_skip, block_popper = blocks[blocks_top]

					if ch[0] not in block_skip:
						kv.append(ch)
#						print('====== append', ch, kv, file=sys.stderr)

					if kv and ch[0] in block_popper:
						kv1 = list(kv[0])
#						print('====== part', kv, file=sys.stderr)
						for ch1 in kv[1:]:
							kv1[0] += ch1[0]
							if kv1[1] != ch1[1]:
								kv1[1] = 1
						dict_parts.append(kv1)
						kv = list()

					if ch[0] == block_end:
						block_stack.pop()
#						print('====== pop', ch, kv, file=sys.stderr)

					if not block_stack:
#						print('====== parsed', ch, kv, file=sys.stderr)
						parsed = True
						break

				if parsed:
					try:
						_dict = dict_render(
							dict_parts, dict_par, style=dict_style, 
							add_dict=add_dict)
#						if dict_style == True:
#							print('====== _dict', text1[-100:], '|||', _dict, '|||', chunk[offset:offset+60], file=sys.stderr)
						text1 += _dict + chunk[offset:]
					except:
						parsed = False
#						print('====== except', dict_parts, chunk[-60:], file=sys.stderr)
				if not parsed:
					text1 += dict_par + chunk
					need_parse += 1
					dict_style = True # for next chunk
					#print('====== retrying', file=sys.stderr)
				#print('======', block_stack, file=sys.stderr)
			elif c == 0:
				text1 += chunk
		if need_parse:
			dict_chunks = re.split(spliter, text1)
			text1 = u''
	return text1


def dict_render(dict_parts, dict_par, style=None, add_dict=False):
#	print('======', dict_parts, dict_par, style, add_dict, file=sys.stderr)
	_dict = [
		(dict_parts[c*2], dict_parts[c*2+1]) \
		for (c, part) in enumerate(dict_parts[::2])
	]
	default_style = sum([part[0][1] for part in _dict])
	style = default_style if style is None else style
#	print('======', style, default_style, file=sys.stderr)

	def part_render(part):
		sep = ':' if style else '='
		#formater = '"%s"%s%s' if (style and part[0][1] < 2) else '%s%s%s'
		formater = '%s%s%s'
#		print('====== part, formater', part, formater, file=sys.stderr)
		key = part[0][0]
		if style and part[0][1] < 2:
			formater = '"%s"%s%s'
			key = key.replace('"', '\\"')
		return formater % (key, sep, part[1][0])
		#return formater % (part[0][0].replace('"', '\\"'), sep, part[1][0])

	content = ','.join([part_render(part) for part in _dict])
	if style:
		if add_dict:
			return u'%s\n**{\n%s\n}\n)' % (dict_par, content)
		else:
			return u'{\n%s\n}' % content
	else:
		return u'%s\n%s\n)' % (dict_par, content)


def clean_comment(matchObj):
	text = matchObj.group('comment')
	text = re.sub(re_comment_tag, '', text)
	text = re.sub(re_dict_only, dict_par.upper(), text)
	return u'"""%s"""' % text


def class_build(matchObj):
	text = matchObj.group('package_name')
	global cls, footer, __sourcepath__
	cls = text.split('::')[-1].strip()
	text = u"""class %s(builder.BuilderMixin):
	__name__ = '%s'
	__sourcepath__ = '%s'
	__load_namespaces__ = %d
	schema = schema
	""" % (cls, cls, __sourcepath__, __load_namespaces__)
	#footer = "print(builder.schema['%s'], file=sys.stderr)" % cls
	return text


def fix_kw(matchObj):
	rep = dict(matchObj.groupdict())
	rep['cls'] = rep['cls'].split('::')[-1].strip()
	text = u"""%(func)s(\n'%(variable)s', "%(cls)s",%(rem)s)""" % rep
	return text


def treeconv(path, exceptpath=None, with_dict=True):
	all_text = list()
	global __load_namespaces__
	__load_namespaces__ = 0
	for root, dirs, files in os.walk(path):
		for file in files:
			fullpath = os.path.join(root, file)
			if fullpath.endswith('.pm'):
				text = open(fullpath, 'r').read()
				if exceptpath and fullpath in exceptpath:
					continue
				text = perlconvert(fullpath, text, with_dict=with_dict)
				##open(fullpath[:-3]+'.py', 'w').write(text)
				##exec(text)
				all_text.append(text)

	return header + '\n\n'.join(all_text)


def oneconv(sourcepath, with_dict=True):
	text = open(sourcepath, 'r').read()
	text = perlconvert(sourcepath, text, with_dict=with_dict)
	##exec(text)
	return header + text


def stdioconv(sourcepath, with_dict=True):
	text = sys.stdin.read()
	text = perlconvert(sourcepath, text, with_dict=with_dict)
	##exec(header + text)
	sys.stdout.write(header + text)


def schemaconv(sourcepath, schema, with_dict=True):

	if os.path.isdir(sourcepath):
		setup_pm = os.path.join(sourcepath, 'Setup.pm')
		if os.path.exists(setup_pm):
			text = oneconv(setup_pm, with_dict=with_dict)
			#open('oneconv.py', 'w').write(text)
		else:
			text = treeconv(sourcepath, exceptpath=None, with_dict=with_dict)
			#open('treeconv.py', 'w').write(text)
	else:
		text = oneconv(sourcepath, with_dict=with_dict)
		#open('oneconv.py', 'w').write(text)

	#open('conv.py', 'w').write(text)
	exec(text, dict(schema=schema))

