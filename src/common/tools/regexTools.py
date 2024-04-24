import re


def check_regex(row, col, patterns):
    """ checks if a specific column matches at least one specific regular expression """

    val = row[col]
    return any(list(map(lambda p: re.match(p, val) is not None, patterns)))


def extract_single_reason(val):

    res = (re.findall(r'^([A-Za-z]*)\(.*\)$', val), True)

    if len(res[0]) == 0:
        # match another regex
        res = (re.findall(r'([A-Za-z ]*):.*', val), True)

    if len(res[0]) == 0:
        res = ([val], False)

    return res


def extract_reason_of_rejects(val):

    res, match = extract_single_reason(val)

    if not match:
        reasons = list(map(lambda x: x.strip(), val.split(';')))
        extracted = []
        for r in reasons:
            extracted.extend(extract_single_reason(r)[0])
        return str(extracted).replace('[', '').replace(']', '').replace("'", '')

    return res[0]
