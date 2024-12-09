import jieba

from video_graph.common.utils.tools import text_split
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class SubtitleSplitOp(Op):
    """
    Function:
        字幕切分算子，用于将字幕文本切分成词语

    Attributes:
        tts_caption_column (str): 字幕文本所在列的名称，默认为 "tts_caption"。
        subtitle_group_column (str): 切分后的字幕组所在列的名称，默认为 "subtitle_group"。
        append_keyword_column (str): 附加关键词所在列的名称，默认为 "product_name"。
        append_keywords (list): 特殊指定的关键词列表，默认为[]

    InputTables:
        shot_table: 字幕文本所在的表。
        request_table: 附加关键词所在的表。

    OutputTables:
        shot_table: 添加了切分后的字幕组的表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/text_process_op/subtitle_split_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.text_process_op.subtitle_split_op import *

# 创建输入表格
shot_table = DataTable(
    name="TestTable1",
    data = {
        'tts_caption': [
                [("水果的销量很好，特别是卖苹果", 0, 5), ("对的，大多数人都喜欢吃苹果", 5, 10)]
        ]
    }
)
request_table=DataTable(
    name="TestTable2",
    data={
        'product_name': ['水果']
    }
)

# 创建操作上下文
op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
op_context.input_tables.append(shot_table)
op_context.input_tables.append(request_table)

# 配置并实例化算子
subtitle_split_op = SubtitleSplitOp(
    name="SubtitleSplitOp",
    attrs={
        "tts_caption_column": "tts_caption",
        "subtitle_group_column": "subtitle_group",
        "append_keyword_column": "product_name",
        "append_keywords": ['苹果','大多数人']
    }
)

# 执行算子
success = subtitle_split_op.process(op_context)

# 检查输出表格
if success:
    output_table = op_context.output_tables[0]
    display(output_table)
else:
    print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        request_table: DataTable = op_context.input_tables[1]
        tts_caption_column = self.attrs.get("tts_caption_column", "tts_caption")
        subtitle_group_column = self.attrs.get("subtitle_group_column", "subtitle_group")
        append_keyword_column = self.attrs.get("append_keyword_column", "product_name")
        append_keywords = self.attrs.get("append_keywords", [])

        append_keyword = None
        if append_keyword_column in request_table.columns:
            append_keyword = request_table.loc[0, append_keyword_column]
        if append_keyword:
            jieba.add_word(append_keyword)
        for kw in append_keywords:
            jieba.add_word(kw)

        shot_table[subtitle_group_column] = None
        for index, row in shot_table.iterrows():
            tts_caption = row.get(tts_caption_column)
            subtitle_group = []
            for text, start_time, end_time in tts_caption:
                subtitle_group.extend(text_split(text, start_time, end_time))

            shot_table.at[index, subtitle_group_column] = subtitle_group

        if append_keyword:
            jieba.del_word(append_keyword)
        for kw in append_keywords:
            jieba.del_word(kw)
        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(SubtitleSplitOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_input(name="request_table", type="DataTable", desc="请求表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表") \
    .add_attr(name="tts_caption_column", type="str", desc="tts文字列名") \
    .add_attr(name="subtitle_group_column", type="str", desc="字幕分组列名") \
    .add_attr(name="append_keyword_column", type="str", desc="追加关键词列名") \
    .add_attr(name="append_keywords", type="list",  desc="特殊指定的关键词列表")
