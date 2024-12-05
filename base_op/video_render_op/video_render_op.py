import json

from video_graph.common.client.client_manager import ClientManager
from video_graph.common.proto.rpc_proto.video_script_pb2 import VideoScriptRequest
from video_graph.common.utils.logger import logger
from video_graph.common.utils.tools import build_bbs_resource_id
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class VideoRenderOp(Op):
    """
    发起视频渲染算子

    Attributes:
        request_column (str): 请求列名，默认为"request"。
        response_column (str): 响应列名，默认为"response"。
        rpc_status_column (str): RPC状态列名，默认为"rpc_status"。
        rpc_message_column (str): RPC消息列名，默认为"rpc_message"。
        video_output_blob_key_column (str): 视频输出Blob Key列名，默认为"video_output_blob_key"。

    InputTables:
        timeline_table: 时间线表。

    OutputTables:
        timeline_table: 添加了视频渲染结果的时间线表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_op/video_render_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        timeline_table: DataTable = op_context.input_tables[0]
        request_column = self.attrs.get("request_column", "request")
        response_column = self.attrs.get("response_column", "response")
        rpc_status_column = self.attrs.get("rpc_status_column", "rpc_status")
        rpc_message_column = self.attrs.get("rpc_message_column", "rpc_message")
        video_output_blob_key_column = self.attrs.get("video_output_blob_key_column", "video_output_blob_key")

        request: VideoScriptRequest = timeline_table.loc[0, request_column]
        logger.debug(f"video render request: {request}")
        video_render_client = ClientManager().get_client_by_name("VideoRenderClient")
        resp = video_render_client.sync_req(request)
        logger.debug(f"video render response: {resp}")

        if resp is None:
            logger.error(f"request_id[{op_context.request_id}] minecraft render failed, resp is None")
            op_context.perf_ctx("video_render_failed", extra1="-1", extra2="resp_is_none")
            return False

        status = resp.get("status")
        message = resp.get("message")
        if status != 'SUCCESS':
            logger.error(f"request_id[{op_context.request_id}] minecraft render failed, message:{message}")
            op_context.perf_ctx("video_render_failed", extra1=status, extra2=message)
            return False

        video_render_output = build_bbs_resource_id(resp.get("output"))
        timeline_table[response_column] = None
        timeline_table[rpc_status_column] = None
        timeline_table[rpc_message_column] = None
        timeline_table[video_output_blob_key_column] = None

        timeline_table.loc[0, response_column] = json.dumps(resp, ensure_ascii=False)
        timeline_table.loc[0, rpc_status_column] = status
        timeline_table.loc[0, rpc_message_column] = message
        timeline_table.loc[0, video_output_blob_key_column] = video_render_output
        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(VideoRenderOp) \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_output(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_attr(name="request_column", type="str", desc="request列名") \
    .add_attr(name="response_column", type="str", desc="response列名") \
    .add_attr(name="rpc_status_column", type="str", desc="rpc状态列名") \
    .add_attr(name="rpc_message_column", type="str", desc="rpc消息列名") \
    .add_attr(name="video_output_blob_key_column", type="str", desc="视频输出blob地址列名")
