import json

from busdatagenerator import Dato

if __name__ == '__main__':
    with open('data.json') as fh:
        data = json.load(fh)

    data = [Dato(**x) for x in data]
    data = [vars(x) for x in data]

    with open('data.json', 'wt', encoding='utf-8') as fh:
        json.dump(data, fh, ensure_ascii=False, indent=4, sort_keys=True)
