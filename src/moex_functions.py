import pandas as pd

def make_new_types(x, dic):
	replace_dict = {'string': 'object',
                  'double': 'float64',
                  'date': 'datetime64[ns]',
                  'datetime': 'datetime64[ns]',
                  'int32': 'float64',
                  'int64': 'float64',
                  'undefined': 'object',
                  'time': 'object',
                  'number': 'float64',
                  'boolean': 'float64'}
	short_dict = {}
	for key, value in dic.items():
		short_dict[key] = value['type']
	new_dict = {}
	for key, value in short_dict.items():
		if value in replace_dict:
			value = replace_dict[value]
			new_dict[key] = value
	date_cols = [key for key, value in new_dict.items()
			   if value == 'datetime64[ns]']
	for col in date_cols:
		x[col] = pd.to_datetime(x[col], errors='coerce')
	int_cols = [key for key, value in new_dict.items() if value == 'float64']
	for col in int_cols:
		x[col] = pd.to_numeric(x[col], errors='coerce')
	x = x.astype(new_dict)
	return x

def convert_variable(variable, variable_type):
	type_convert = {'string': str,
                  'double': lambda x: pd.to_numeric(x),
                  'date': lambda x: pd.to_datetime(x),
                  'int32': lambda x: pd.to_numeric(x),
                  'int64': lambda x: pd.to_numeric(x),
                  'datetime': lambda x: pd.to_datetime(x),
                  'undefined': str,
                  'time': str,
                  'number': lambda x: pd.to_numeric(x),
                  'boolean': lambda x: pd.to_numeric(x)}
	conversion_func = type_convert.get(variable_type)
	if conversion_func:
		return conversion_func(variable)
	else:
			return variable
