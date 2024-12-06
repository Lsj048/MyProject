from video_graph.common.proto.rpc_proto.video_script_pb2 import VideoScriptRequest, Clip, ClipType
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AddClipOp(Op):
    """
    Function:
        增加镜号算子

    Attributes:
        request_column (str): 请求列名，默认为"request"。
        clip_type (str): 镜号类型，默认为"video"。

    InputTables:
        timeline_table: 时间线表。

    OutputTables:
        timeline_table: 添加了镜号的时间线表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_op/add_clip_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        timeline_table: DataTable = op_context.input_tables[0]
        request_column = self.attrs.get("request_column", "request")
        clip_type = self.attrs.get("clip_type", "video")

        request: VideoScriptRequest = timeline_table.loc[0, request_column]
        cur_start_time = 0
        if len(request.clip) > 0:
            last_clip = request.clip[-1]
            cur_start_time = last_clip.end_time
        new_clip = Clip()
        new_clip.start_time = cur_start_time
        new_clip.end_time = cur_start_time
        if clip_type == "video":
            new_clip.clip_type = ClipType.VIDEO
        else:
            new_clip.clip_type = ClipType.TRANSITION
            # TODO: 补充转场信息

        request.clip.append(new_clip)
        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(AddClipOp) \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_output(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_attr(name="request_column", type="str", desc="request列名") \
    .add_attr(name="clip_type", type="str", desc="镜号类型，转场/视频")
