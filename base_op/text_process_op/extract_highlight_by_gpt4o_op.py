from video_graph.common.client.gpt4o_client import get_content_highlight
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class ExtractHighlightByGpt4oOp(Op):
    """
    【remote】通过 gpt4o 提取高光

    Attributes:
        content_column (str): 输入文本列，默认为 "content"。
        highlight_column (str): 字幕列名，默认为 "highlight"。

    InputTables:
        shot_table: 输入表格。

    OutputTables:
        shot_table: 输出表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/text_process_op/extract_highlight_by_gpt4o_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.text_process_op.extract_highlight_by_gpt4o_op import *

        # 创建输入表格
        input_table = DataTable(
            name="TestTable",
            data = {
                "origin_text": [
                    "[{'content': '什么？不可能！！他居然已经到了巅峰仙帝境界！'},\
                      {'content': '围攻落尘仙域的众仙帝目眦俱裂，只感觉后背发凉'},\
                      {'content': '他不是陨落在纪元战场了吗，怎么会出现在这里！有的仙帝几乎想马上逃离这里'} \
                    ]"
                ]
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        extract_highlight_by_gpt4o_op = ExtractHighlightByGpt4oOp(
            name="ExtractHighlightByGpt4oOp",
            attrs={
               "content_column": "origin_text",
                "highlight_column": "extract_highlight"
            }
        )

        # 执行算子
        success = extract_highlight_by_gpt4o_op.process(op_context)

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
        shot_table: DataTable = op_context.input_tables[0]
        content_column = self.attrs.get("content_column", "content")
        highlight_column = self.attrs.get("highlight_column", "highlight")

        status = True
        shot_table[highlight_column] = None
        for index, row in shot_table.iterrows():
            content = row.get(content_column)

            if not content:
                status = False
                continue

            content = content.replace('\\r', '').replace('\\n', '').replace('\\', '')
            content_list = eval(content)
            if len(content_list) < 3:
                status = False
                continue

            content_1 = content_list[0]['content']
            content_2 = content_list[1]['content']
            content_3 = content_list[2]['content']
            subtitle = get_content_highlight([content_1, content_2, content_3])
            if not subtitle:
                status = False
                continue

            shot_table.at[index, highlight_column] = subtitle

        if status is False:
            self.fail_reason = "gpt提取高光失败"
            self.trace_log.update({"fail_reason": self.fail_reason})
            return False

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(ExtractHighlightByGpt4oOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表") \
    .add_attr(name="pay_novel_highlight_extraction_column", type="str", desc="输入文本列名") \
    .add_attr(name="text_column", type="str", desc="高光文本列名")
