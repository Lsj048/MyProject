import re

from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class SubtitleKeywordOp(Op):
    """
    Function:
        字幕关键字算子，用于在字幕文本中添加关键字信息

    Attributes:
        subtitle_group_column (str): 字幕组所在列的名称，默认为 "subtitle_group"。
        subtitle_group_with_keyword_column (str): 添加关键字后的字幕组所在列的名称，默认为 "subtitle_group_with_keyword"。
        product_name_column (str): 产品名称所在列的名称，默认为 "product_name"。
        keyword_set (set): 关键字集合，默认为 {"抢购", "点击", "下载"}

    InputTables:
        shot_table: 字幕组所在的表。
        request_table: 产品名称所在的表。

    OutputTables:
        shot_table: 添加了关键字后的字幕组的表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/text_process_op/subtitle_keyword_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.text_process_op.subtitle_keyword_op import *

        # 创建输入表格
        shot_table = DataTable(
            name="TestTable1",
            data = {
                'subtitle_group': [
                    [
                        (0.0, 9.9, [['这是', '一条', '抢购', '信息', '下载','快手', '助力', '抢购']])
                    ]
                ]
            }
        )
        request_table = DataTable(
            name="TestTable2",
            data={
                'product_name': ['香蕉']
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(shot_table)
        op_context.input_tables.append(request_table)

        # 配置并实例化算子
        subtitle_keyword_op = SubtitleKeywordOp(
            name="SubtitleKeywordOp",
            attrs={
                "subtitle_group_column": "subtitle_group",
                "subtitle_group_with_keyword_column": "subtitle_group_with_keyword",
                "product_name_column": "product_name",
                "keyword_set": ["抢购", "点击", "下载"]
            }
        )

        # 执行算子
        success = subtitle_keyword_op.process(op_context)

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
        subtitle_group_column = self.attrs.get("subtitle_group_column", "subtitle_group")
        subtitle_group_with_keyword_column = self.attrs.get("subtitle_group_with_keyword_column",
                                                            "subtitle_group_with_keyword")
        product_name_column = self.attrs.get("product_name_column", "product_name")
        keyword_set = set(self.attrs.get("keyword_set", ["抢购", "点击", "下载"]))

        if product_name_column in request_table.columns:
            keyword_set.add(request_table.loc[0, product_name_column])

        shot_table[subtitle_group_with_keyword_column] = None
        for index, row in shot_table.iterrows():
            subtitle_group = row.get(subtitle_group_column)
            subtitle_group_with_keyword = []
            for start_time, end_time, items_list in subtitle_group:
                new_items_list = []
                keywords_list = []
                for items in items_list:
                    text = ''.join(items)
                    keywords = [item for item in items if item in keyword_set]
                    if keywords:
                        regex = "|".join(f"({keyword})" for keyword in keywords)
                        segments = re.split(regex, text)  # 按关键词切分
                        segments = [segment for segment in segments if segment]  # 去除空字符串
                    else:
                        segments = [text]
                    new_items_list.append(segments)
                    keywords_list.append(set(keywords))
                subtitle_group_with_keyword.append((start_time, end_time, new_items_list, keywords_list))

            shot_table.at[index, subtitle_group_with_keyword_column] = subtitle_group_with_keyword

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(SubtitleKeywordOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表") \
    .add_attr(name="subtitle_group_column", type="str", desc="字幕分组列名") \
    .add_attr(name="subtitle_group_with_keyword_column", type="str", desc="带关键词信息的字幕分组列名") \
    .add_attr(name="product_name_column", type="str", desc="产品名列名") \
    .add_attr(name="keyword_set", type="set", desc="关键词集合")
