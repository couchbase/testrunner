import argparse
import sys
import re

def usage(err=None):
    print("""\
Syntax: query_template_generator.py [options]

Options
    --grammar, -g Path to grammar file. 
    --queries, -q How many queries to generate (optional). All possible queries will be generated if not specified.
    --output, -o Output file 

    --help Show this help

Examples:
  python scripts/query_template_generator.py --grammar grammar.yy --queries 1000
""")
    sys.exit(0)


def generate_template(grammar_file, queries_amount, output):
    f = open(grammar_file)
    tokens = parse_tokens(f)
    f.close()
    prepared_tokens = prepare_tokens(tokens)
    parsed_tokens = find_patterns(prepared_tokens)
    while has_more_patterns(parsed_tokens):
        parsed_tokens = fill_patterns(parsed_tokens)
        #import pdb; pdb.set_trace()
    queries = parsed_tokens.get("query")
    counter = 0

    out = open(output, 'w')
    for query in queries:
        if counter>=queries_amount:
            break
        out.write(query+";"+"\n")
        counter = counter+1
    out.close()
    print(("Total number of queries - "+str(counter)))


def has_more_patterns(parsed_tokens):
    return "::start::" in str(parsed_tokens)



def fill_patterns(filled_tokens):
    for token in list(filled_tokens.keys()):
        token_vals = filled_tokens.get(token)
        for token_val in token_vals:
            if '::start::' in token_val:
                token_vals.remove(token_val)
                new_token_vals = fill_pattern(token_val, filled_tokens)
                for new_val in new_token_vals:
                    token_vals.append(new_val)
                filled_tokens[token] = token_vals
                return filled_tokens


def fill_pattern(token_val, filled_tokens):
    found_token = token_val[token_val.find("::start::")+9:token_val.find("!!end!!")]
    substitution_array = []
    substitution_vals = filled_tokens.get(found_token)
    for substitution_val in substitution_vals:
        if len(token_val) > token_val.find("!!end!!")+8 and token_val[token_val.find("!!end!!")+8] not in ['(', ')']:
            substitution_array.append(token_val[0:token_val.find("::start::")] + substitution_val+ " " + token_val[token_val.find("!!end!!")+7:])
        else:
            substitution_array.append(token_val[0:token_val.find("::start::")] + substitution_val + token_val[token_val.find("!!end!!") + 7:])

    return substitution_array

def is_special_char(ch):
    return ch in [' ', '(', ')', '*', '-', '+', '=', '/', '<', '>']

def is_pattern(word, prepared_tokens):
    return word in list(prepared_tokens.keys())

def find_patterns(prepared_tokens):
    for token in list(prepared_tokens.keys()):
        token_values = prepared_tokens.get(token)
        for ii in range(len(token_values)):
            token_value = token_values[ii]
            if token_value.strip() in list(prepared_tokens.keys()):
                token_values[ii] = "::start::"+token_value+"!!end!!"
            else:
                if token_value!='':
                    word = ''
                    new_token_value = ''
                    for i in range(len(token_value)):
                        ch = token_value[i]
                        if is_special_char(ch) or i==len(token_value)-1:
                            if i == len(token_value) - 1 and not is_special_char(ch):
                                word = word+ch
                            if is_pattern(word, prepared_tokens):
                                new_token_value = new_token_value+"::start::"+word+"!!end!!"
                            else:
                                new_token_value = new_token_value+word

                            if is_special_char(ch):
                                new_token_value = new_token_value + ch
                            word = ''
                        else:
                            word = word+ch
                    token_values[ii] = new_token_value
    return prepared_tokens


def check_and_substitute(word, prepared_tokens):
    if word in list(prepared_tokens.keys()):
        token_values = prepared_tokens.get(word)


def parse_tokens(f, ):
    tokens = {}
    lines = f.readlines()
    current_token = ''
    current_token_value = ''
    for line in lines:
        line = line.strip()
        # comment
        if line.startswith("#") or len(line) == 0:
            continue
        # token line
        if line.endswith(":"):
            if current_token != '':
                tokens[current_token] = current_token_value

            token = line[0:len(line) - 1]
            if token in list(tokens.keys()):
                print("Duplicate token found - " + token)
                sys.exit(1)
            tokens[token] = ''
            current_token = token
            current_token_value = ''
        # token value line
        else:
            current_token_value = current_token_value + line
    tokens[current_token] = current_token_value
    return tokens

def prepare_tokens(tokens):
    prepared_tokens = {}
    for token in list(tokens.keys()):
        token_value = tokens[token]
        token_value = token_value.replace(";", "")
        vals = []
        for v in token_value.split("|"):
            v = v.strip()
            vals.append(v)
        prepared_tokens[token] = vals

    return prepared_tokens


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--grammar', '-g', help="Grammar file", type=str)
    parser.add_argument('--queries', '-q', help="Number of queries", type=str)
    parser.add_argument('--output', '-o', help="Output file", type=str)

    args = parser.parse_args()
    grammar_file = args.grammar
    queries_amount = args.queries
    output = args.output
    if queries_amount is None or queries_amount == '':
        queries_amount = 1000000000

    if grammar_file is None or grammar_file == '' or output is None or output == '':
            usage()

    generate_template(grammar_file, queries_amount, output)


