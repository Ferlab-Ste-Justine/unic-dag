import os
import re

root = 'config'
extract_schema = '(.*)_config.json'

for (r, folders, files) in os.walk(root):
    if r == root:
        print(r)
        print(folders)
        for namespace in folders:
            for configs in os.walk(f'{root}/{namespace}'):
                for f in configs[2]:
                    schema = re.search(extract_schema, f).group(1)
                    print(namespace)
                    print(schema)

