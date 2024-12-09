# !/usr/bin/env python
# -*-coding:utf-8 -*-

"""
# Author     ：Bo Wang
# File       : add_title_and_end.py
# Time       ：2024/12/2 17:16
"""
import random
import os

from video_graph import logger
from video_graph.common.client.client_manager import ClientManager
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext
from video_graph.common.utils.template import add_title_or_video_end
from video_graph.common.utils.tools import parse_bbs_resource_id

class AddTitleVideoEndOp(Op):
    """
    Function:
        添加标题或者尾贴

    Attributes:
        video_blob_key_column (str): 拼接视频 Blob Key 列名，默认为"video_blob_key_column"。
        resolution_column (str): 分辨率列名，默认为"resolution"。
        first_industry_name_column (str): 一级行业名称列名，默认为"first_industry_name"。
        second_industry_name_column (str): 二级行业名称列名，默认为"second_industry_name"。
        reco_text_column (str): 推荐文案列名，默认为"reco_text"。

    InputTables:
        request_table: 请求表格。
        timeline_table: 源视频时间线表

    OutputTables:
        timeline_table: 添加了模版视频资源和结果的时间线表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/extend_op/template_video_op/add_title_and_end_op.py?ref_type=heads

    Examples:
        #等待作者更新
        """

    def compute(self, op_context: OpContext) -> bool:
        request_table: DataTable = op_context.input_tables[0]
        timeline_table: DataTable = op_context.input_tables[1]
        video_blob_key_column = self.attrs.get("video_blob_key_column", "video_blob_key")
        first_industry_name_column = self.attrs.get("first_industry_name_column", "first_industry_name")
        second_industry_name_column = self.attrs.get("second_industry_name_column", "second_industry_name")
        product_name_column = self.attrs.get("product_name_column", "product_name")
        ocpc_action_type_column = self.attrs.get("ocpc_action_type_column", "ocpc_action_type")
        excycle_link_type_column = self.attrs.get("excycle_link_type_column", "excycle_link_type")
        reco_text_column = self.attrs.get("reco_text_column", "reco_text")
        video_output_blob_key_column = self.attrs.get("video_output_blob_key_column", "video_output_blob_key")
        edit_type_column = self.attrs.get("edit_type_column", "edit_type")

        first_industry_name = request_table.at[0, first_industry_name_column]
        second_industry_name = request_table.at[0, second_industry_name_column]
        product_name = request_table.at[0, product_name_column]
        ocpc_action_type = request_table.at[0, ocpc_action_type_column]
        excycle_link_type = request_table.at[0, excycle_link_type_column]
        reco_text = request_table.at[0, reco_text_column]

        timeline_table[video_output_blob_key_column] = None
        timeline_table[edit_type_column] = ""
        for index, row in timeline_table.iterrows():
            video_blob_key = timeline_table.at[index, video_blob_key_column]

            _db, _table, _key = parse_bbs_resource_id(video_blob_key)
            photo_id = os.path.splitext(_key)[0]
            status, res_resource_id, edit_type = add_title_or_video_end(
                photo_id=photo_id, title=reco_text, ocpc_action_type=ocpc_action_type,
                excycle_link_type=excycle_link_type,
                first_industry_name=first_industry_name,
                second_industry_name=second_industry_name,
                product_name=product_name
            )

            if not status:
                self.fail_reason = f"素材微改失败（标题/尾贴）"
                self.trace_log.update({"fail_reason": self.fail_reason})
                return False

            timeline_table.at[0, video_output_blob_key_column] = res_resource_id
            timeline_table.at[0, edit_type_column] = edit_type

        op_context.output_tables.append(timeline_table)
        return True

op_register.register_op(AddTitleVideoEndOp) \
    .add_input(name="request_table", type="DataTable", desc="混剪镜号表") \
    .add_input(name="timeline_table", type="DataTable", desc="虚拟人表") \
    .add_output(name="timeline_table", type="DataTable", desc="请求表") \
    .add_attr(name='video_blob_key_column', type='str', desc='视频本地路径所在列名') \
    .add_attr(name='first_industry_name_column', type='str', desc='视频时长列名') \
    .add_attr(name='second_industry_name_column', type='int', desc='每切片时长 (单位为秒)') \
    .add_attr(name='ocpc_action_type_column', type='str', desc='原始视频名, 后续如 merge 可能会用') \
    .add_attr(name='excycle_link_type_column', type='str', desc='切片路径所在列') \
    .add_attr(name='reco_text_column', type='str', desc='视频切片索引列名') \
    .add_attr(name='video_output_blob_key_column', type='str', desc='视频切片索引列名')
