def remove_oov(const, oov):
    va = {}
    fi = {}
    for graph, varlist in const['variables'].items():
        va[graph] = [v for v in varlist if v not in oov]
    for v, vfilist in const['filters'].items():
        if v in oov:
            continue
        fi[v] = [fv for fv in vfilist if fv not in oov]
    return {'variables': va, 'filters': fi}