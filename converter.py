import json

from busdatagenerator import Register, JSON_PATH

if __name__ == '__main__':
    with open(JSON_PATH) as fh:
        data = json.load(fh)

    data = [Register(**x) for x in data]
    data = [vars(x) for x in data]

    with open(JSON_PATH, 'wt', encoding='utf-8') as fh:
        json.dump(data, fh, ensure_ascii=False, indent=4, sort_keys=True)
