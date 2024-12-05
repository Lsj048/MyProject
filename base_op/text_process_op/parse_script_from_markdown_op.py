from video_graph.common.utils.tools import parse_script
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class ParseScriptFromMarkDownOp(Op):
    """
    【local】解析 markdown 格式脚本，转换为标准格式脚本

    Attributes:
        markdown_script_column (str): markdown 格式脚本列名，默认为 "markdown_script"。
        standard_script_column (str): 标准格式脚本列名，默认为 "standard_script"。

    InputTables:
        shot_table: markdown 格式脚本所在的表。

    OutputTables:
        shot_table: 添加了标准格式脚本的表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/text_process_op/parse_script_from_markdown_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.text_process_op.parse_script_from_markdown_op import *

        # 创建输入表格
        input_table = DataTable(
            name="TestTable",
            data = {
                "markdown_script": [
                    "镜头1|场景|动作|台词|运镜\n---|---|---|---|---\n1|跨海大桥|走|<角色A>：燕子！还会再见吗燕子！|跟随",
                    "镜头2|场景|动作|台词|运镜\n---|---|---|---|---\n1|跨海大桥|小跑|<角色A>：燕子！没有我你要幸福！燕子！|跟随",
                    "镜头3|场景|动作|台词|运镜\n---|---|---|---|---\n1|跨海大桥|狂奔|<角色A>：燕子！没有你我怎么活啊！燕子！|跟随"
                ]
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="13\u5c81", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        parse_script_from_markdown_op = ParseScriptFromMarkDownOp(
            name="ParseScriptFromMarkDownOp",
            attrs={
                "markdown_script_column":"markdown_script",
                "standard_script_column_column":"standard_script",
            }
        )

        # 执行算子
        success = parse_script_from_markdown_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        markdown_script_column = self.attrs.get("markdown_script_column", "markdown_script")
        standard_script_column = self.attrs.get("standard_script_column", "standard_script")

        status = True
        shot_table[standard_script_column] = None
        for index, row in shot_table.iterrows():
            markdown_script = row.get(markdown_script_column)
            if not markdown_script:
                status = False
                continue
            standard_script = parse_script(markdown_script)
            shot_table.at[index, standard_script_column] = standard_script

        if status is False:
            self.fail_reason = "解析markdown脚本失败"
            self.trace_log.update({"fail_reason": self.fail_reason})
            return False

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(ParseScriptFromMarkDownOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表") \
    .add_attr(name="markdown_script_column", type="str", desc="markdown脚本列名") \
    .add_attr(name="standard_script_column", type="str", desc="标准脚本列名")
