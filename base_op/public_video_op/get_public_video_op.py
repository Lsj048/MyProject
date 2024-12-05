import time

from video_graph.common.client.client_manager import ClientManager
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext
from video_graph.common.utils.redis import RedisManager


class GetPublicVideoOp(Op):
    """
    【remote】获取公域视频算子，请求服务获取公域视频，输出公域视频列表

    Attributes:
        first_industry_name_column (str): 一级行业名称列名，默认为"first_industry_name"。
        second_industry_name_column (str): 二级行业名称列名，默认为"second_industry_name"。
        product_name_column (str): 产品名称列名，默认为"product_name"。
        caption_column (str): 字幕列名，默认为"caption"。
        public_material_column (str): 公域素材列名，默认为"public_material"。
        req_num (int): 请求个数，默认为30。
        duration_upper (int): 公域视频的时长上限，默认为10s。
        duration_lower (int): 公域视频的时长下限，默认为0s。
        ratio (float): 公域视频的分辨率比例，默认为0.5

    InputTables:
        shot_table: 镜号表格。
        request_table: 请求表格。

    OutputTables:
        shot_table: 添加了公域素材的表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/public_video_op/get_public_video_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.public_video_op.get_public_video_op import *

        # 创建输入表格
        shot_table = DataTable(
            name="TestTable1",
            data = {
                "caption": ["Caption"],
            }
        )

        request_table = DataTable(
            name="TestTable2",
            data={
                "first_industry_name": ["体育器材"],
                "second_industry_name": ["游泳体育器材"],
                "product_name": ["泳镜"]
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(shot_table)
        op_context.input_tables.append(request_table)

        # 配置并实例化算子
        get_public_video_op = GetPublicVideoOp(
            name="GetPublicVideoOp",
            attrs={
                "first_industry_name_column": "first_industry_name",
                "second_industry_name_column": "second_industry_name",
                "product_name_column": "product_name",
                "caption_column": "caption",
                "public_material_column": "public_material",
                "req_num": 1,  # 请求 1 个视频
                "duration_upper": 10,
                "duration_lower": 0,
                "ratio": 0.5625
            }
        )

        # 执行算子
        success = get_public_video_op.process(op_context)

        # 检查输出表格，可以结合FileDownlaoerOp算子下载视频查看
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        request_table: DataTable = op_context.input_tables[1]
        first_industry_name_column = self.attrs.get("first_industry_name_column", "first_industry_name")
        second_industry_name_column = self.attrs.get("second_industry_name_column", "second_industry_name")
        product_name_column = self.attrs.get("product_name_column", "product_name")
        caption_column = self.attrs.get("caption_column", "caption")
        public_material_column = self.attrs.get("public_material_column", "public_material")
        req_num = self.attrs.get("req_num", 30)
        duration_upper = self.attrs.get("duration_upper", 0)
        duration_lower = self.attrs.get("duration_lower", 1)
        ratio = self.attrs.get("ratio", 0.5625)

        first_industry_name = request_table.at[0, first_industry_name_column]
        second_industry_name = request_table.at[0, second_industry_name_column]
        product_name = request_table.at[0, product_name_column]

        text_to_video_client = ClientManager().get_client_by_name("TextToVideoClient")
        shot_table[public_material_column] = None
        for index, row in shot_table.iterrows():
            caption = row.get(caption_column)
            if caption is None:
                continue

            if isinstance(req_num, str) and req_num in shot_table.columns:
                req_num = row.get(req_num)
            elif isinstance(req_num, str):
                req_num = 30

            resp = text_to_video_client.sync_req(caption, product_name, first_industry_name, second_industry_name,
                                                 topk=req_num, duration_upper=duration_upper,
                                                 duration_lower=duration_lower, ratio=ratio)

            shot_table.at[index, public_material_column] = resp

        op_context.output_tables.append(shot_table)
        return True

op_register.register_op(GetPublicVideoOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_input(name="request_table", type="DataTable", desc="请求表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表") \
    .add_attr(name="first_industry_name_column", type="str", desc="一级行业名称列名") \
    .add_attr(name="second_industry_name_column", type="str", desc="二级行业名称列名") \
    .add_attr(name="product_name_column", type="str", desc="产品名称列名") \
    .add_attr(name="caption_column", type="str", desc="字幕列名") \
    .add_attr(name="public_material_column", type="str", desc="公域素材列名") \
    .add_attr(name="re_num", type="int", desc="请求个数") \
    .add_attr(name="duration_upper", type="int", desc="公域视频的时长上限") \
    .add_attr(name="duration_lower", type="int", desc="公域视频的时长下限") \
    .add_attr(name="ratio", type="float", desc="公域视频的分辨率比例")
