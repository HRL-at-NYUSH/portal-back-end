import json

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

def read_json(path):
    with open(path, 'r') as f:
        re = json.load(f)
    return re

def check_int(s):
    if (len(s) > 1 and s[0] == '-' and s[1:].isdigit()) or s.isdigit():
        return True
    return False

def parse_digital_string(s):
    if check_int(s):
        return [int(s)]
    sp = s.split(',')
    if len(sp) == 2:
        lo, hi = sp[0], sp[1]
        if check_int(lo) and check_int(hi) and int(lo) < int(hi):
            return list(range(int(lo), int(hi)))
    return []

if __name__ == "__main__":
    tests = [
        "1",
        "0",
        "-1",
        "100",
        "-100",
        "3,4",
        "3,3",
        "abc",
        ",3",
        ""
    ]

    for i, t in enumerate(tests):
        print(i, t, parse_digital_string(t))
