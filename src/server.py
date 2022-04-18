import sanic
from sanic.response import json
from sanic_cors import CORS
from dataloader import HRLDataLoader
db = HRLDataLoader(
    data_path='../data/ipums_full_count_nyc_census_coded_20210801.parquet',
    const_path='../metadata/constrains.json',
    oov_path='../metadata/oov.json',
    vdict_path='../metadata/variable_dictionary.json'
)

app = sanic.Sanic("HRLInteractivePortal")
CORS(app)


@app.get("/")
async def hello_world(request):
    print(request)
    return json({"Hello": "world."})


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
    variables = request.args.get('variables')
    if not variables:
        return json(db.var_dict)

    res = {}
    variables = variables.split(",")
    for var in variables:
        if var in db.var_dict.keys():
            res[var] = db.var_dict[var]

    return json(res)


@app.get('/columns')
async def columns(request):
    """
    available columns
    :return: [col_0, col_1, ...]
    """
    return json(db.data.columns.tolist())


@app.get("/card")
async def card(request):
    """
    cardinality
    :param request: x, variable to return cardinality
    :return: [x_0, x_1, ...]
    """
    x = request.args.get('x')
    if not x:
        return json([], status=400)
    x = x.upper()
    return json(list(sorted(db.data[x].unique().tolist())))


@app.get("/bar")
async def bar(request):
    """
    bar chart
    :param request: var, variable on the x-axis
    :param request: group, variable on which x is grouped (
    default 'all')
    :param request: *filters, filters on which x is filtered
    :return:{group0: {"x": [x_0, x_1, ..., x_{n_cat-1}], "y": [count(x_0; group0), count(x_1; group0),
    ..., count(x_{n_cat-1}; group0)]}, group1: {...} ... }
    """
    return json(db.bar(request.args))


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
    return json(db.line(request.args))


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
    return json([{"id": "0", "name": "Bar Chart", "route": "bar"}])
