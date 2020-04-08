import os
import sys
import json

html_head = """
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
  <meta http-equiv="content-type" content="text/html; charset=utf-8" />
  <title>Compare stats</title>
  <script type="text/javascript" src="http://www.google.com/jsapi">
  </script>"""
script_template = """<script type="text/javascript">
    google.load('visualization', '1', {packages: ['table']});
    function %s() {
      // Create and populate the data table.
      var data = new google.visualization.DataTable();
      %s

      // Create and draw the visualization.
      var table = new google.visualization.Table(document.getElementById('%s'));

      var formatter = new google.visualization.TableArrowFormat();
      %s
      table.draw(data, {allowHtml: true, showRowNumber: true});
    }

    google.setOnLoadCallback(%s);
  </script>"""
table_template = """
<div id="{0}" style="width: {1}px; height: {2}px;"></div>
"""


def compare_stats(file1, file2):
    print(file1)
    print(file2)
    json_dict = {}
    files = [file1, file2]

    data = {}
    for f in files:
        json_data = open(f)
        try:
            data[f] = json.load(json_data)
        except ValueError as e:
            print("can't compare: %s" % (json_data))
            return


    temp_ns_server_json = {}

    for data_file in data:
        temp_ns_server_json[data_file] = {}
        for block in data[data_file]:
            if block == "ns_server":
                for sublock in data[data_file][block]:
                    if sublock != '':
                        temp_ns_server_json[data_file][block + "_" + sublock] = {}
                        temp_ns_server_json[data_file][block + "_" + sublock] = data[data_file][block][sublock]


    for data_file in data:
        for bucket in temp_ns_server_json[data_file]:
            data[data_file][bucket] = temp_ns_server_json[data_file][bucket]
        del data[data_file]["ns_server"]

    blocks = {}
    for block in data[files[0]]:
        blocks[block] = {}
        for sublock in data[files[0]][block]:
            if sublock != '':
                blocks[block][sublock] = {}

    content = html_head

    information = '<p style=\"color:red;font-size:18px;\">Differences of more than 10 percent</p>'
    for block in blocks:
        information += "<p>" + block
        json_dict[block] = {}
        table = "data.addColumn('string', '%s');" % (block)
        for sublock in blocks[block]:
            if sublock == '':
                 continue
            json_dict[block][sublock] = {}
            for i in range(len(files)):
                table += ("data.addColumn('string', '%s:f%s');" % (sublock, i + 1)).replace(":8091", "")
            table += "data.addColumn('number', '%');"
        column_counter = 0
        temp = ""
        for sublock in  blocks[block]:
            if sublock == '':
                continue
            for stat in  data[files[0]][block][sublock]:
                for sublock in  blocks[block]:
                    if sublock == '':
                        continue
                    json_dict[block][sublock][stat] = {}
                    if column_counter == 0:
                        temp += "['%s', " % stat
                    else:
                        temp += ", "
                    isInt = True
                    for i in range(len(files)):
                        if stat not in list(data[files[i]][block][sublock].keys()):
                            cell = "NONE"
                        else:
                            cell = str(data[files[i]][block][sublock][stat])
                        if i == 0:
                            json_dict[block][sublock][stat]["new"] = cell
                        else:
                            json_dict[block][sublock][stat]["old"] = cell
                        try:
                            x = float(cell)
                        except ValueError as e:
                            isInt = False;
                        temp += "'%s'," % cell
                        column_counter += 1
                    if isInt:
                        if data[files[0]][block][sublock][stat] != 0 and  data[files[1]][block][sublock][stat]:
                            percentage = int(100 * (data[files[0]][block][sublock][stat] - data[files[1]][block][sublock][stat]) / (data[files[0]][block][sublock][stat]))
                            temp += "%s" % percentage
                            json_dict[block][sublock][stat]["diff"] = ["+", "-"][percentage >= 0] + str(percentage) + "%"
                            if percentage > 10:
                                diff_str = "%s:%s %s - %s=+%s%%" % (sublock, stat, data[files[1]][block][sublock][stat], data[files[0]][block][sublock][stat], percentage)
                                information += "<p style=\"font-family:arial;color:grey;font-size:12px;\">&nbsp;&nbsp;" + diff_str + "</p>"
                                print(diff_str)
                        else:
                            temp += "0"
                    else:
                        temp += "0"
                    if column_counter == len(blocks[block]) * len(files):
                        table += """
                    data.addRow(%s]);""" % (temp);
                        column_counter = 0
                        temp = ""
            break
        formater = ""
        columns_format = 0
        for columns_format in  range(len(blocks[block])):

            formater += """
            formatter.format(data, %s); // Apply formatter to column""" % ((columns_format + 1) * 3)

        content += script_template % (block, table, block, formater, block)

    content += """
    </head>
    <body style="font-family: Arial;border: 0 none;">
    """
    content += "<p>LEGEND</p>"
    content += """<ul>
    <li>f1: %s</li>
    <li>f2: %s</li>
    </ul>""" % (file1, file2)
    for block in blocks:
        if list(data[files[0]][block].keys())[0] == '':
            num_rows = len(list(data[files[0]][block][list(data[files[0]][block].keys())[1]].keys()))
        else:
            num_rows = len(list(data[files[0]][block][list(data[files[0]][block].keys())[0]].keys()))
        num_columns = len(list(data[files[0]][block].keys())) * 3 + 1
        content += "<p style=\"background-color:green;\">%s</p>" % (block)
        content += table_template.format(block, num_columns * 111, 22 * num_rows + 35)
    content += information


    content += """</body>
    </html>"""
    dirs0 = os.path.dirname(files[0]).split(os.sep)
    dirs1 = os.path.dirname(files[1]).split(os.sep)
    fn, fe = os.path.splitext(files[0])
    file_name = dirs0[-1] + "_" + dirs0[-2] + "_" + dirs1[-2] + "_" + fn.split(os.sep)[-1]
    html_path = os.path.dirname(files[0]) + "/" + file_name + ".html"
    json_path = os.path.dirname(files[0]) + "/" + file_name + ".json"
    file1 = open(html_path, 'w')
    file1.write(content)
    file1.close()
    json.dump(json_dict, open(json_path, 'w'), indent=4, sort_keys=True)
    print("""
    the comparison has been saved in %s & %s""" % (html_path, json_path))

def compare_by_folders(folder1, folder2):
    folders = os.walk(folder1).next()[1]
    for folder in folders:
            sub_folder = os.path.join(folder1, folder)
            files = [ f for f in os.listdir(sub_folder) if os.path.isfile(os.path.join(sub_folder, f))]
            for file in files:
                fileName, fileExtension = os.path.splitext(os.path.join(folder1, folder, file))
                if fileExtension != ".txt":
                    print("can't  compare files with %s format: %s" % (fileExtension, os.path.join(folder1, folder)))
                    continue
                try:
                    with open(os.path.join(folder2, folder, file)):
                        print("Generate report for  %s & %s" % (os.path.join(folder1, folder, file), os.path.join(folder2, folder, file)))
                        compare_stats(os.path.join(folder1, folder, file), os.path.join(folder2, folder, file))
                except IOError:
                    print('file was not found: %s. comparison of the file will be skipped' % (os.path.join(folder2, folder, file)))


def main():
    files = []
    for arg in sys.argv[1:]:
        if not os.path.isdir(arg):
            print('folder was not found: %s' % (arg))
            sys.exit()
    compare_by_folders(sys.argv[1], sys.argv[2])



if __name__ == "__main__":
    main()
