import numpy as np
import pandas as pd
import json
import sanic
from sanic_cors import CORS

DATA = pd.read_parquet('data/ipums_full_count_nyc_census_coded_20210801.parquet')
DATA['SINCEIMMIG'] = DATA['YEAR'] - DATA['YRIMMIG']
DATA['AGEIMMIG'] = DATA['AGE'] - DATA['YEAR'] + DATA['YRIMMIG']
with open('constrains.json', 'r') as f:
    CONST = json.load(f)
with open('variable_dictionary.json','r') as f:
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
    :return: [col_1, col_2, ..., col_n]
    """
    return sanic.response.json(DATA.columns.values.tolist())

@app.get("/card")
async def card(request):
    """
    cardinality
    :param request: x, variable to return cardinality
    :return: [x_1, x_2, ..., x_n]
    """
    x = request.args.get('x')
    if not x:
        return sanic.response.json([], status=400)
    x = x.upper()
    return sanic.response.json(list(sorted(DATA[x].unique().tolist())))


@app.get("/bar")
async def bar(request):
    """
    bar chart
    :param request: x, variable on the x-axis
    :param request: group, variable on which x is grouped (default 'all')
    :return:{'group1':
                {"x": [x_0, x_1, ...], "y": [count(x_0; group1), count(x_1; group1), ...]},
            'group2': {...}
            ...
            }
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
    for g in np.unique(DATA[group].values):
        subdf = DATA[DATA[group] == g]
        values, counts = np.unique(subdf[x].values, return_counts=True)
        values = values.tolist()
        counts = counts.tolist()
        response[g] = {'x': values, 'y': counts}
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
    return sanic.response.json([{"id":"0","name":"Bar Chart","route":"bar"}])
