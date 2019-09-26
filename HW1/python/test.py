import json

a = json.dumps("[[a: 1, b: 2], [a: 4, b: 8]]")
print(json.loads(lambda s: s['a']))
