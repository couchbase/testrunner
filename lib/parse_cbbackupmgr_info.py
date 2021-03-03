import re

def sub_group_lines(lines):
    """ Splits lines delimited by empty lines, groups split lines into lists.

    Args:
        lines (list): A list of lines seperated by empty lines.

    Returns:
        A list of lists of grouped lines.

    """
    groups, sub_group = [], []

    for line in lines:
        if len(line.rstrip()) > 0:
            sub_group.append(line.rstrip())
        else:
            groups.append(sub_group)
            sub_group = []

    if len(sub_group) > 0:
        groups.append(sub_group)

    return groups


def line_to_dict(lines):
    """ Transforms a group of lines into a list of dictionary

    Args:
        lines (list): A list of lines, the first line contains the keys and the remaining lines contain the values.
                      [
                       [key1    key2   key3],
                       [val11  val12  val12],
                       [val21  val22  val22],
                      ]

    Returns:
        A list of dictionaries.
        [
         {key1: val11, key2: val12, ..},
         {key1: val21, key2: val22, ..}
        ]

    """
    def strip_and_split(line):
        return [word.strip() for word in re.sub(r"^[*|+|~|#|-]", "", line).split('|')[:-1]]

    start_sym = lines[0][0]
    node_type = \
    {'*': 'repos', '+': 'backups', '-': 'buckets', '~': 'scopes', '#': 'collections'}.get(start_sym, 'archives')

    lines = [strip_and_split(line) for line in lines]

    first_line, remaining_lines = lines[0], lines[1:]
    return node_type, [dict(zip(first_line, line)) for line in remaining_lines]


def transform_key(key):
    """ Changes keys of tabular output to the names used in the JSON output.

    Args:
        key (str): The key to change.

    Returns:
        str: The new key name.
    """
    key = key.lower().strip()
    key_substition = {'uuid'         : 'archive_uuid',
                   'cluster uuid' : 'source_cluster_uuid',
                   '# backups'    : 'count',
                   'backup'       : 'date',
                   'bucket'       : 'name',
                   'aliases'      : 'fts_alias',
                   'indexes'      : 'index_count',
                   'views'        : 'views_count',
                   'fts'          : 'fts_count',
                   'cbas'         : 'analytics_count'
                   }
    return key_substition.get(key, key)

def transform_val(val):
    """ Serialises vals to Python types

    1. Transforms numbers stored as strings to Python ints, floats.
    2. Transforms sizes in bytes, kilobytes, megabytes and gigabytes stored as strings to bytes as ints.
    3. Transforms string bools into Python bools.

    Args:
        val (str): The value to serialize

    Returns:
        str: The serialised Python type..

    """

    # A regular expression to match integers and floats
    num_pat = "^[+-]?([0-9]*[\.])?[0-9]+"

    # Match vals and transform them to their equivalent Python types
    if re.search(num_pat + "B$", val):
        return int(float(val[:-1]))
    elif re.search(num_pat + "KiB$", val):
        return int(float(val[:-3]) * (1024 ** 1))
    elif re.search(num_pat + "MiB$", val):
        return int(float(val[:-3]) * (1024 ** 2))
    elif re.search(num_pat + "GiB$", val):
        return int(float(val[:-3]) * (1024 ** 3))
    elif re.search(num_pat + "$", val):
        try:
            return int(val)
        except ValueError:
            return float(val)
    elif val in ['true', 'false']:
        return val == 'true'

    return val


def traverse_dict(tree, node_type, dictionary):
    """ Appends the dictionary child node to its parent node's list of child nodes

    Args:
        tree (dict): The tree to search
        node_type (str): The node_type (e.g. 'archives', 'repos', 'backups', ...).
        dictionary (dict): The child node of type node_type.

    Returns:
        None

    """
    node_priority = ['archives', 'repos', 'backups', 'buckets', 'scopes', 'collections']
    node_parents = node_priority[:node_priority.index(node_type)]

    conductor = tree

    # Find child_node's point of insertion
    for parent in node_parents:
        if parent not in conductor:
            continue

        conductor[parent] = conductor.get(parent, [])

        if len(conductor[parent]) > 0:
            conductor = conductor[parent][-1]
        else:
            break

    # Append dictionary child node to its parent node's list of child nodes
    conductor[node_type] = conductor.get(node_type, [])
    conductor[node_type].append(dictionary)

def construct_tree(lines):
    """ Construct a tree from the tabular output of 'cbbackupmgr info' that reflects the JSON output.

    Args:
        lines (dict): The tabular output from 'cbbackupmgr info' as a list of lines.

    Returns:
        dict: A dictionary that resembles the JSON output of 'cbbackupmgr info'.

    """
    tree = {}

    for group in sub_group_lines(lines):
        node_type, dictionaries = line_to_dict(group)

        for dictionary in dictionaries:
            dictionary = {transform_key(key): transform_val(val) for key, val in dictionary.items()}
            traverse_dict(tree, node_type, dictionary)

    return tree['archives'][0] if 'archives' in tree else tree


if __name__ == "__main__":
    """Takes 'cbbackupmgr info' tabular output from stdin and emits a tree that resembles the JSON output"""
    import pprint
    import fileinput
    pp = pprint.PrettyPrinter(indent=4)
    print("Tree:")
    lines = [line for line in fileinput.input()]
    pp.pprint(construct_tree(lines))

