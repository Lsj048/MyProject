from video_graph.common.client.client_manager import ClientManager
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class GetScriptByKuaiyiOp(Op):
    """
    Function:
        通过快意模型获取脚本，仅支持1.0格式的脚本

    Attributes:
        product_name_column (str): 产品名称列名，默认为 "product_name"。
        description_column (str): 描述列名，默认为 "description"。
        first_industry_name_column (str): 一级行业名称列名，默认为 "first_industry_name"。
        second_industry_name_column (str): 二级行业名称列名，默认为 "second_industry_name"。
        keywords_column (str): 关键词列名，默认为 "keywords"。
        req_num (int): 请求脚本数量，默认为 1。
        script_list_column (str): 脚本列表列名，默认为 "script_list"。

    InputTables:
        request_table: 输入表格。

    OutputTables:
        request_table: 输出表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/text_process_op/get_script_by_kuaiyi_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.text_process_op.get_script_by_kuaiyi_op import *

# 创建输入表格
input_table = DataTable(
    name="TestTable",
    data = {
        "product_name": ["泳镜"],
        "description": ["银色镜片，防起雾处理"],
        "first_industry_name": ["体育器材行业"],
        "second_industry_name": ["游泳设备生产行业"],
        "keywords": ["泳镜"]
    }
)

# 创建操作上下文
op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
op_context.input_tables.append(input_table)

# 配置并实例化算子
get_script_by_kuaiyi_op = GetScriptByKuaiyiOp(
    name="GetScriptByKuaiyiOp",
    attrs={
        "product_name_column": "product_name",
        "description_column": "description",
        "first_industry_name_column": "first_industry_name",
        "second_industry_name_column": "second_industry_name",
        "keywords_column": "keywords",
        "req_num": 1,
        "script_list_column": "script_list"
    }
)

# 执行算子
success = get_script_by_kuaiyi_op.process(op_context)

# 检查输出表格
if success:
    output_table = op_context.output_tables[0]
    display(output_table)
else:
    print("算子执行失败")
    """

    def compute_when_skip(self, op_context: OpContext):
        op_context.output_tables.append(op_context.input_tables[0])
        return True

    def compute(self, op_context: OpContext) -> bool:
        request_table: DataTable = op_context.input_tables[0]
        product_name_column = self.attrs.get("product_name_column", "product_name")
        description_column = self.attrs.get("description_column", "description")
        first_industry_name_column = self.attrs.get("first_industry_name_column", "first_industry_name")
        second_industry_name_column = self.attrs.get("second_industry_name_column", "second_industry_name")
        keywords_column = self.attrs.get("keywords_column", "keywords")
        req_num = self.attrs.get("req_num", 1)
        script_list_column = self.attrs.get("script_list_column", "script_list")

        product_name = request_table.at[0, product_name_column]
        description = request_table.at[0, description_column]
        first_industry = request_table.at[0, first_industry_name_column]
        second_industry = request_table.at[0, second_industry_name_column]
        keywords = request_table.at[0, keywords_column]

        script_client = ClientManager().get_client_by_name("ScriptCreationClient")
        resp = script_client.sync_req(req_id=op_context.request_id,
                                      product_name=product_name,
                                      description=description,
                                      first_industry=first_industry,
                                      second_industry=second_industry,
                                      keyword=keywords,
                                      script_num=req_num)
        result_info = resp.get("resultList", [])
        if len(result_info) == 0:
            return False

        script_list = []
        for script_info in result_info[0].get("scriptList", []):
            script = script_info.get("script")
            if script:
                script_list.append(script)

        request_table[script_list_column] = None
        request_table.at[0, script_list_column] = script_list

        op_context.output_tables.append(request_table)
        return True

"""
        description_column (str): 描述列名，默认为 "description"。
        first_industry_name_column (str): 一级行业名称列名，默认为 "first_industry_name"。
        second_industry_name_column (str): 二级行业名称列名，默认为 "second_industry_name"。
        keywords_column (str): 关键词列名，默认为 "keywords"。
        req_num (int): 请求脚本数量，默认为 1。
        script_list_column (str): 脚本列表列名，默认为 "script_list"。
"""

op_register.register_op(GetScriptByKuaiyiOp) \
    .add_input(name="request_table", type="DataTable", desc="请求表") \
    .add_output(name="request_table", type="DataTable", desc="请求表") \
    .add_attr(name="product_name_column", type="str", desc="产品名称列名") \
    .add_attr(name="description_column", type="str", desc="描述列名") \
    .add_attr(name="first_industry_name_column", type="str", desc="一级行业名称列名") \
    .add_attr(name="second_industry_name_column", type="str", desc="二级行业名称列名") \
    .add_attr(name="keywords_column", type="str", desc="关键词列名") \
    .add_attr(name="req_num", type="int", desc="请求脚本数量") \
    .add_attr(name="script_list_column", type="str", desc="脚本列表列名")
