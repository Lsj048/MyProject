import random
import os

from video_graph import logger
from video_graph.common.client.client_manager import ClientManager
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext
from video_graph.common.utils.template import add_title_or_video_end
from video_graph.common.utils.tools import parse_bbs_resource_id

class TemplateVideoOp(Op):
    """
    【remote】模版视频渲染算子，请求服务获取模版视频

    Attributes:
        montage_video_blob_key_column (str): 拼接视频 Blob Key 列名，默认为"montage_video_blob_key"。
        resolution_column (str): 分辨率列名，默认为"resolution"。
        first_industry_name_column (str): 一级行业名称列名，默认为"first_industry_name"。
        second_industry_name_column (str): 二级行业名称列名，默认为"second_industry_name"。
        reco_text_column (str): 推荐文案列名，默认为"reco_text"。
        montage_template_id_column (str): 模版ID列名，默认为"montage_template_id"。
        montage_with_template_resource_column (str): 模版视频资源列名，默认为"montage_with_template_resource"。
        montage_with_template_result_column (str): 模版视频结果列名，默认为"montage_with_template_result"。

    InputTables:
        request_table: 请求表格。
        timeline_table: 源视频时间线表

    OutputTables:
        timeline_table: 添加了模版视频资源和结果的时间线表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/extend_op/template_video_op/template_video_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        request_table: DataTable = op_context.input_tables[0]
        timeline_table: DataTable = op_context.input_tables[1]
        montage_video_blob_key_column = self.attrs.get("montage_video_blob_key_column", "video_output_blob_key")
        resolution_column = self.attrs.get("resolution_column", "resolution")
        first_industry_name_column = self.attrs.get("first_industry_name_column", "first_industry_name")
        second_industry_name_column = self.attrs.get("second_industry_name_column", "second_industry_name")
        reco_text_column = self.attrs.get("reco_text_column", "reco_text")
        montage_template_id_column = self.attrs.get("montage_template_id_column", "montage_template_id")

        montage_with_template_resource_column = self.attrs.get("montage_with_template_resource_column",
                                                               "montage_with_template_resource")
        montage_with_template_result_column = self.attrs.get("montage_with_template_result_column",
                                                             "montage_with_template_result")

        job_id = op_context.request_id
        resolution = request_table.at[0, resolution_column]
        first_industry_name = request_table.at[0, first_industry_name_column]
        second_industry_name = request_table.at[0, second_industry_name_column]
        reco_text = request_table.at[0, reco_text_column]
        template_id = int(request_table.at[0, montage_template_id_column])

        montage_video_resources = []
        for index, row in timeline_table.iterrows():
            montage_video_blob_key = row.get(montage_video_blob_key_column)
            if montage_video_blob_key:
                montage_video_resources.append(montage_video_blob_key)

        template_video_client = ClientManager().get_client_by_name("TemplateVideoClient")
        resp = template_video_client.sync_req(job_id, montage_video_resources, {}, [],
                                              {}, first_industry_name, second_industry_name, template_id,
                                              reco_text, resolution[0], resolution[1])

        if resp is None or resp.get("resp_type") != 1:
            self.fail_reason = f"模版视频渲染失败: {resp.get('err_msg') if resp else 'timeout'}"
            self.trace_log.update({"fail_reason": self.fail_reason})
            logger.info(f"template video resp is error, resp:{resp}")
            return False

        request_table[montage_with_template_resource_column] = None
        request_table[montage_with_template_result_column] = None
        request_table.at[0, montage_with_template_result_column] = resp
        request_table.at[0, montage_with_template_resource_column] = resp.get("output_video_resource", "")

        op_context.output_tables.append(request_table)
        return True


op_register.register_op(TemplateVideoOp) \
    .add_input(name="montage_table", type="DataTable", desc="混剪镜号表") \
    .add_input(name="vtuber_table", type="DataTable", desc="虚拟人表") \
    .add_input(name="request_table", type="DataTable", desc="请求表") \
    .add_output(name="request_table", type="DataTable", desc="请求表")

