import json

import numpy as np
import pandas as pd
import sanic
from sanic_cors import CORS
from utils import remove_oov

DATA = pd.read_parquet('data/ipums_full_count_nyc_census_coded_20210801.parquet')
DATA['SINCEIMMIG'] = DATA['YEAR'] - DATA['YRIMMIG']
DATA['AGEIMMIG'] = DATA['AGE'] - DATA['YEAR'] + DATA['YRIMMIG']
OOV = ['HISPAN', 'FAMUNIT', 'SCHOOL', 'LABFORCE']
with open('constrains.json', 'r') as f:
    CONST = json.load(f)
CONST = remove_oov(CONST, OOV)
with open('variable_dictionary.json', 'r') as f:
    VAR_DIC = json.load(f)

app = sanic.Sanic("HRLInteractivePortal")
CORS(app)


@app.get("/")
async def hello_world(request):
    return sanic.response.json({"Hello": "world."})


@app.get("/variable-dictionary")
async def variable_dictionary(request):
    """
    variable dictionary
    :param request: variables, names of variables that need to query
            "YEAR, SEX, ..."
    :return: {"var1":
                {name: "...",
                description: "...",
                "codes":{
                    "1": "..."
                    }
                },
                ...
            }
    """
    vars = request.args.get('variables')
    if not vars:
        return sanic.response.json(VAR_DIC)

    res = {}
    vars = vars.split(",")
    for var in vars:
        if var in VAR_DIC.keys():
            res[var] = VAR_DIC[var]

    return sanic.response.json(res)


@app.get('/columns')
async def columns(request):
    """
    available columns
    :return: [col_0, col_1, ...]
    """
    return sanic.response.json(DATA.columns.tolist())


@app.get("/card")
async def card(request):
    """
    cardinality
    :param request: x, variable to return cardinality
    :return: [x_0, x_1, ...]
    """
    x = request.args.get('x')
    if not x:
        return sanic.response.json([], status=400)
    x = x.upper()
    return sanic.response.json(list(sorted(DATA[x].unique().tolist())))


@app.get("/bar")
async def bar(request):
    """
    bar chart :param request: x, variable on the x-axis :param request: group, variable on which x is grouped (
    default 'all') :return:{group0: {"x": [x_0, x_1, ..., x_{n_cat-1}], "y": [count(x_0; group0), count(x_1; group0),
    ..., count(x_{n_cat-1}; group0)]}, group1: {...} ... }
    """
    x = request.args.get('x')
    if not x:
        return sanic.response.json({}, status=400)
    x = x.upper()
    if x not in CONST['variables']['bar']:
        return sanic.response.json({}, status=400)
    group = request.args.get('group')
    if not group:
        values, counts = np.unique(DATA[x].values, return_counts=True)
        values = values.tolist()
        counts = counts.tolist()
        return sanic.response.json({'all': {'x': values, 'y': counts}})
    group = group.upper()
    if group not in CONST['filters'].get(x, []):
        return sanic.response.json({}, status=400)
    response = {}
    groups = DATA.groupby([group, x]).size()
    for g in groups.index.get_level_values(0).unique():
        subdf = groups[g]
        response[g] = {"x": subdf.index.tolist(), "y": subdf.tolist()}
    return sanic.response.json(response)


@app.get("/line")
async def line(request):
    """
    line chart (with mean)
    :param request: y, variable on the y-axis (x-axis is YEAR)
    :param request: group, variable on which y is grouped (default 'all')
    :return:{group0:
                {"x": [year_0, year_1, ..., y_{n_year-1}], "y": [y_0.mean, y_1.mean, ..., y_{n_year-1}.mean]},
            group1: {...}
            ...
            }
    """
    y = request.args.get('y')
    if not y:
        return sanic.response.json({}, status=400)
    y = y.upper()
    if y not in CONST['variables']['line']:
        return sanic.response.json({}, status=400)
    group = request.args.get('group')
    if not group or group.upper() == 'YEAR':
        subdf = DATA.groupby('YEAR')[y].mean()
        return sanic.response.json({'all': {'x': subdf.index.tolist(), 'y': subdf.tolist()}})
    group = group.upper()
    if group not in CONST['filters'].get(y, []):
        return sanic.response.json({}, status=400)
    response = {}
    groups = DATA.groupby([group, 'YEAR'])[y].mean()
    for g in groups.index.get_level_values(0).unique():
        subdf = groups[g]
        response[g] = {'x': subdf.index.tolist(), 'y': subdf.tolist()}
    return sanic.response.json(response)


@app.get("/graph-types")
async def graph_types(request):
    """
    graph types
    :return:[
             {id: 0, name: "Bar Chart", "route":"bar"},
             {id: 1, ...},
            ...
            ]
    """
    return sanic.response.json([{"id": "0", "name": "Bar Chart", "route": "bar"}])
