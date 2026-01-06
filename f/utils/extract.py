import json


def extract_any_json(s: str):
    cur = 0
    json_list = []
    while cur < len(s):
        s = s[next(idx for idx, c in enumerate(s[cur:]) if c in "{[") :]
        try:
            valid = json.loads(s)
            json_list.append(valid)
            cur += len(s)
        except json.JSONDecodeError as e:
            try:
                valid = json.loads(s[: e.pos])
                json_list.append(valid)
                cur += e.pos
            except json.JSONDecodeError:
                return json_list
    return json_list
