from flask import Flask, render_template
import os
import ast
import re

app = Flask(__name__)

# 读取指定路径下所有算子的信息
def get_operator_info():
    operators_info = {}

    # 获取所有子文件夹（每个子文件夹代表一个功能类别）
    base_dir = './base_op'
    for category in os.listdir(base_dir):
        category_path = os.path.join(base_dir, category)
        if os.path.isdir(category_path):
            operators_info[category] = []
            for file_name in os.listdir(category_path):
                if file_name.endswith('.py'):
                    op_info = parse_operator_file(os.path.join(category_path, file_name))
                    operators_info[category].append(op_info)
    return operators_info

# 解析算子的 Python 文件
def parse_operator_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    # 提取类名（算子的名称）
    class_name = re.search(r'class (\w+)\(', content)
    class_name = class_name.group(1) if class_name else "UnknownClass"

    # 提取 docstring
    docstring = re.search(r'\"\"\"(.*?)\"\"\"', content, re.DOTALL)
    docstring = docstring.group(1) if docstring else ""

    # 提取 Attributes 部分
    attributes = {}
    attr_pattern = re.compile(r'^\s*(\w+)\s*\((.*?)\):\s*(.*)', re.M)
    for match in attr_pattern.findall(docstring):
        attr_name, attr_type, attr_desc = match
        attributes[attr_name] = {'type': attr_type, 'desc': attr_desc.strip()}

    # 提取 Hrefs 部分
    hrefs = re.search(r'Href:\s*(\S+)', docstring)
    href = hrefs.group(1) if hrefs else ""  # 如果没有找到 Hrefs，则为空字符串

    # 提取 Examples 部分
    examples = re.search(r'Examples:\s*(.*?)(?=\n\s*\"\"\"|\Z)', docstring, re.DOTALL)
    example_code = examples.group(1).strip() if examples else ""

    # 返回算子信息
    return {
        'name': class_name,
        'attributes': attributes,
        'examples': example_code,
        'href': href
    }

@app.route('/')
def index():
    operators_info = get_operator_info()
    return render_template('index.html', operators_info=operators_info)

if __name__ == '__main__':
    app.run(debug=True)