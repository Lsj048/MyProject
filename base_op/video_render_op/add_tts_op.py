from video_graph.common.proto.rpc_proto.video_script_pb2 import Op as OpPb
from video_graph.common.proto.rpc_proto.video_script_pb2 import VideoScriptRequest, ProtoBlobStoreKey, AddAudioOp, OpType
from video_graph.common.utils.tools import parse_bbs_resource_id
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AddTtsOp(Op):
    """
    增加TTS算子

    Attributes:
        request_column (str): 请求列名，默认为"request"。
        tts_blob_key_column (str): TTS Blob Key列名，默认为"tts_blob_key"。
        tts_duration_column (str): TTS时长列名，默认为"tts_duration"。
        tts_volume (float): TTS音量，默认为1。

    InputTables:
        timeline_table: 时间线表。
        shot_table: 镜号表。

    OutputTables:
        timeline_table: 添加了TTS的时间线表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_op/add_tts_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        timeline_table: DataTable = op_context.input_tables[0]
        shot_table: DataTable = op_context.input_tables[1]
        request_column = self.attrs.get("request_column", "request")
        tts_blob_key_column = self.attrs.get("tts_blob_key_column", "tts_blob_key")
        tts_duration_column = self.attrs.get("tts_duration_column", "tts_duration")
        tts_volume = self.attrs.get("tts_volume", 1)

        request: VideoScriptRequest = timeline_table.loc[0, request_column]
        tts_key = shot_table.loc[0, tts_blob_key_column]
        tts_duration = shot_table.loc[0, tts_duration_column]

        tts_source = ProtoBlobStoreKey()
        db, table, key = parse_bbs_resource_id(tts_key)
        tts_source.db = db
        tts_source.table = table
        tts_source.key = key

        add_audio_op = AddAudioOp()
        add_audio_op.filename.CopyFrom(tts_source)
        add_audio_op.volume = tts_volume
        add_audio_op.start_time = 0
        add_audio_op.end_time = tts_duration
        op_pb = OpPb()
        op_pb.op_type = OpType.ADD_AUDIO_OP_TYPE
        op_pb.add_audio_op.CopyFrom(add_audio_op)
        request.clip[-1].ops.append(op_pb)

        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(AddTtsOp) \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="video_render_table", type="DataTable", desc="视频渲染表") \
    .add_attr(name="request_column", type="str", desc="request列名") \
    .add_attr(name="tts_blob_key_column", type="str", desc="tts资源列名") \
    .add_attr(name="tts_duration_column", type="str", desc="tts时长列名") \
    .add_attr(name="tts_volume", type="float", desc="tts音量")
