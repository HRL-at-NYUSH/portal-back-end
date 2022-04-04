import numpy as np
import pandas as pd
from sanic import Sanic
from sanic.response import json
from sanic_cors import CORS, cross_origin

DATA = pd.read_parquet('data/ipums_full_count_nyc_census_coded_20210801.parquet')
DATA['SINCEIMMIG'] = DATA['YEAR'] - DATA['YRIMMIG']
DATA['AGEIMMIG'] = DATA['AGE'] + DATA['YEAR'] - DATA['YRIMMIG']

app = Sanic("HRLInteractivePortal")
CORS(app)

@app.get("/")
async def hello_world(request):
    return json({"Hello": "world."})


@app.get('/columns')
async def columns(request):
    """
    available columns
    :return: [col_1, col_2, ..., col_n]
    """
    return json(DATA.columns.values.tolist())

@app.get("/card")
async def card(request):
    """
    cardinality
    :param request: x, variable to return cardinality
    :return: [x_1, x_2, ..., x_n]
    """
    x = request.args.get('x')
    if not x:
        return json([], status=400)
    x = x.upper()
    return json(DATA[x].unique().tolist())

@app.get("/bar")
async def bar(request):
    """
    bar chart
    :param request: x, variable on the x-axis
    :param request: group, variable on which x is grouped (default 'all')
    :return:{'group1':
                {x: [], count: []},
            'group2': {...}
            ...
            }
    """
    x = request.args.get('x')
    if not x:
        return json({}, status=400)
    x = x.upper()
    group = request.args.get('group')
    if not group:
        values, counts = np.unique(DATA[x].values, return_counts=True)
        values = values.tolist()
        counts = counts.tolist()
        return json({'all': {'x': values, 'y': counts}})
    group = group.upper()
    response = {}
    for g in np.unique(DATA[group].values):
        subdf = DATA[DATA[group] == g]
        values, counts = np.unique(subdf[x].values, return_counts=True)
        values = values.tolist()
        counts = counts.tolist()
        response[g] = {'x': values, 'y': counts}
    return json(response)
