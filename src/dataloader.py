import numpy as np
import pandas as pd
from utils import remove_oov, read_json, parse_digital_string


class HRLDataLoader:
    def __init__(self, data_path, const_path, oov_path, vdict_path):
        self.data = pd.read_parquet(data_path)
        self.data['SINCEIMMIG'] = self.data['YEAR'] - self.data['YRIMMIG']
        self.data['AGEIMMIG'] = self.data['AGE'] - self.data['YEAR'] + self.data['YRIMMIG']
        self.const = remove_oov(read_json(const_path), read_json(oov_path))
        self.var_dict = read_json(vdict_path)

    def _check_var(self, graph_name, var_name):
        if not isinstance(var_name, str) or not isinstance(graph_name, str):
            return False
        return var_name.upper() in self.const['variables'].get(graph_name, [])

    def _check_group(self, var_name, group_name):
        if not isinstance(group_name, str) or not isinstance(var_name, str):
            return False
        return group_name.upper() in self.const['filters'].get(var_name, [])

    def _parse_args(self, graph_name, args, val=False):
        if 'var' not in args:
            return None, None, None
        var = args['var']
        if len(var) != 1:
            return None, None, None
        var = var[0]
        if not self._check_var(graph_name, var):
            return None, None, None
        var = var.upper()
        if 'group' not in args:
            group = None
        else:
            group = args['group']
            if len(group) != 1 or not self._check_group(var, group[0]):
                group = None
            else:
                group = group[0].upper()
        filters = {}
        for fil, fvs in args.items():
            if fil.upper() not in self.const['filters'][var] + [var]:
                continue
            filters[fil.upper()] = []
            for fv in fvs:
                filters[fil.upper()] += parse_digital_string(fv)
            filters[fil.upper()] = list(set(filters[fil.upper()]))
        if not filters:
            filters = None
        if val:
            if len(args.get('val', [])) == 1:
                val = int(args['val'][0])
                return var, group, filters, val
            else:
                return None, None, None
        return var, group, filters

    def _filter_by_dict(self, filters):
        if not filters:
            return self.data
        df = self.data
        for fil, fvs in filters.items():
            df = df[df[fil].isin(fvs)]
        return df.reset_index(drop=True)

    def bar(self, args):
        var, group, filters = self._parse_args('bar', args)
        print('bar chart: var, group, filters', var, group, filters)
        if not var:
            return {}
        df = self._filter_by_dict(filters)
        if not group:
            values, counts = np.unique(df[var].values, return_counts=True)
            values = values.tolist()
            counts = counts.tolist()
            return {'all': {'x': values, 'y': counts}}
        re = {}
        groups = df.groupby([group, var]).size()
        for g in groups.index.get_level_values(0).unique():
            subdf = groups[g]
            re[g] = {"x": subdf.index.tolist(), "y": subdf.tolist()}
        return re

    def line(self, args):
        var, group, filters = self._parse_args('line', args)
        print('line chart: var, group, filters', var, group, filters)
        if not var:
            return {}
        df = self._filter_by_dict(filters)
        if not group:
            means = df[[var, 'YEAR']].groupby('YEAR')[var].mean()
            return {'all': {'x': means.index.tolist(), 'y': means.tolist()}}
        re = {}
        if group == 'YEAR':
            groups = df[[var, 'YEAR']].groupby([group, 'YEAR'])[var].mean()
        else:
            groups = df[[var, group, 'YEAR']].groupby([group, 'YEAR'])[var].mean()
        for g in groups.index.get_level_values(0).unique():
            means = groups[g]
            re[g] = {'x': means.index.tolist(), 'y': means.tolist()}
        return re

    def line_count(self, args):
        vgfv = self._parse_args('line_count', args, val=True)
        print(vgfv)
        var = vgfv[0]
        if not var:
            print('line_count chart: var, group, filters val', None, None, None, None)
            return {}
        var, group, filters, val = vgfv
        print('line_count chart: var, group, filters val', var, group, filters, val)
        df = self._filter_by_dict(filters)
        df = df[df[var]==val].reset_index(drop=True)
        if not group:
            counts = df[[var, 'YEAR']].groupby('YEAR')[var].count()
            return {'all': {'x': counts.index.tolist(), 'y': counts.tolist()}}
        re = {}
        if group == 'YEAR':
            groups = df[[var, 'YEAR']].groupby([group, 'YEAR'])[var].count()
        else:
            groups = df[[var, group, 'YEAR']].groupby([group, 'YEAR'])[var].count()
        for g in groups.index.get_level_values(0).unique():
            counts = groups[g]
            re[g] = {'x': counts.index.tolist(), 'y': counts.tolist()}
        return re

    def area(self, args):
        var, _, filters = self._parse_args('area', args)
        print('area chart: var, filters', var, filters)
        if not var:
            return {}
        df = self._filter_by_dict(filters)
        re = {}
        groups = df[[var, 'YEAR']].groupby([var, 'YEAR'])[var].count()
        for g in groups.index.get_level_values(0).unique():
            counts = groups[g]
            re[g] = {'x': counts.index.tolist(), 'y': counts.tolist()}
        return re


if __name__ == "__main__":
    dataloader = HRLDataLoader('../data/ipums_full_count_nyc_census_coded_20210801.parquet',
                               '../metadata/constrains.json', '../metadata/oov.json',
                               '../metadata/variable_dictionary.json')
    tests = [
        {},
        {'var': ['serial']},
        {'var': ['sex']},
        {'var': ['sex', 'year']},
        {'var': ['sex'], 'group': ['']},
        {'var': ['sex'], 'group': ['bpl']},
        {'var': ['sex'], 'group': ['year']},
        {'var': ['sex'], 'group': ['year', 'race']},
        {'var': ['sex'], 'group': ['year', 'race'], 'noise': ['0']},
        {'var': ['sex'], 'group': ['year', 'race'], 'lit': ['1', '2']},
        {'var': ['sex'], 'group': ['year', 'race'], 'lit': ['-1', '2']},
        {'var': ['sex'], 'group': ['year', 'race'], 'lit': ['abc', '2']},
        {'var': ['sex'], 'group': ['year', 'race'], 'lit': ['-2', '2']},
        {'var': ['sex'], 'group': ['year', 'race'], 'lit': ['3,6', '-1']},
    ]
    for i, t in enumerate(tests):
        print(i, t, dataloader._parse_args('bar', t))
