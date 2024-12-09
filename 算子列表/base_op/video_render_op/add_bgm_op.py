from video_graph.common.proto.rpc_proto.video_script_pb2 import Op as OpPb
from video_graph.common.proto.rpc_proto.video_script_pb2 import VideoScriptRequest, ProtoBlobStoreKey, AddAudioOp, OpType
from video_graph.common.utils.kconf import get_kconf_value
from video_graph.common.utils.tools import parse_bbs_resource_id
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AddBgmOp(Op):
    """
    Function:
        增加BGM算子

    Attributes:
        request_column (str): 请求列名，默认为"request"。
        bgm_blob_key_column (str): BGM Blob Key列名，默认为"bgm_blob_key"。
        bgm_duration_column (str): BGM时长列名，默认为"bgm_duration"。

    InputTables:
        timeline_table: 时间线表。
        shot_table: 镜号表。

    OutputTables:
        timeline_table: 添加了BGM的时间线表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_op/add_bgm_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        timeline_table: DataTable = op_context.input_tables[0]
        shot_table: DataTable = op_context.input_tables[1]
        request_column = self.attrs.get("request_column", "request")
        bgm_blob_key_column = self.attrs.get("bgm_blob_key_column", "bgm_blob_key")
        bgm_duration_column = self.attrs.get("bgm_duration_column", "bgm_duration")

        kconf_params: dict = get_kconf_value("ad.algorithm.nieuwlandGeneration", "json")
        bgm_volume = kconf_params["bgm_volume"]

        request: VideoScriptRequest = timeline_table.loc[0, request_column]
        music_key = shot_table.loc[0, bgm_blob_key_column]
        bgm_duration = shot_table.loc[0, bgm_duration_column]

        clip = request.clip[-1]
        total_duration = clip.end_time

        music_source = ProtoBlobStoreKey()
        db, table, key = parse_bbs_resource_id(music_key)
        music_source.db = db
        music_source.table = table
        music_source.key = key

        loop_num = 1  # BGM循环次数
        if total_duration > bgm_duration:
            loop_num = int(total_duration // bgm_duration)
        for idx in range(loop_num):
            add_audio_op = AddAudioOp()
            add_audio_op.filename.CopyFrom(music_source)
            add_audio_op.volume = bgm_volume
            add_audio_op.start_time = bgm_duration * idx
            add_audio_op.end_time = bgm_duration * (idx + 1)
            op_pb = OpPb()
            op_pb.op_type = OpType.ADD_AUDIO_OP_TYPE
            op_pb.add_audio_op.CopyFrom(add_audio_op)
            request.ops.append(op_pb)

        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(AddBgmOp) \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_attr(name="request_column", type="str", desc="request列名") \
    .add_attr(name="bgm_blob_key_column", type="str", desc="bgm资源列名") \
    .add_attr(name="bgm_duration_column", type="str", desc="bgm时长列名")
