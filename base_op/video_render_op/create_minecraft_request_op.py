from video_graph.common.proto.rpc_proto.video_script_pb2 import VideoScriptRequest, ProtoBlobStoreKey
from video_graph.common.utils.tools import get_timestamp
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class CreateMinecraftRequestOp(Op):
    """
    构造minecraft request算子

    Attributes:
        request_column (str): 请求列名，默认为"request"。
        output_blob_db (str): 输出Blob DB，默认为"ad"。
        output_blob_table (str): 输出Blob Table，默认为"nieuwland-material"。
        video_rendering_biz (str): 视频渲染业务，默认为"nieuwland"。
        resolution (list): 分辨率，默认为[720, 1280]。

    InputTables:
        request_table: 请求表。
        timeline_table: 时间线表。

    OutputTables:
        timeline_table: 添加了minecraft request的时间线表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_op/create_minecraft_request_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        #request_table: DataTable = op_context.input_tables[0]
        timeline_table: DataTable = op_context.input_tables[1]
        request_column = self.attrs.get("request_column", "request")
        video_rendering_biz = self.attrs.get("video_rendering_biz", "nieuwland")
        resolution = self.attrs.get("resolution", [720, 1280])
        output_blob_db = self.attrs.get("output_blob_db", "ad")
        output_blob_table = self.attrs.get("output_blob_table", "nieuwland-material")

        request_id = op_context.request_id
        request = VideoScriptRequest()
        video_output = ProtoBlobStoreKey()
        video_output.db = output_blob_db
        video_output.table = output_blob_table
        video_output.key = f"montage_{request_id}_{get_timestamp()}.mp4"
        request.output.CopyFrom(video_output)
        request.request_id = request_id
        request.resolution.extend(resolution)
        request.biz = video_rendering_biz

        if timeline_table.shape[0] == 0:
            timeline_table.loc[0] = None
        timeline_table.loc[0, request_column] = request
        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(CreateMinecraftRequestOp) \
    .add_input(name="request_table", type="DataTable", desc="请求表") \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_output(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_attr(name="request_column", type="str", desc="request列名") \
    .add_attr(name="output_blob_db", type="str", desc="输出视频文件的blob db名") \
    .add_attr(name="output_blob_table", type="str", desc="输出视频文件的blob table名") \
    .add_attr(name="video_rendering_biz", type="str", desc="视频渲染业务名") \
    .add_attr(name="resolution", type="list", desc="分辨率")
