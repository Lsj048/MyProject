from video_graph.common.proto.rpc_proto.video_script_pb2 import Op as OpPb
from video_graph.common.proto.rpc_proto.video_script_pb2 import ProtoBlobStoreKey, Clip, ClipType, \
    Video, AddAudioOp, OpType, VideoSpeedOp, VideoResizeOp
from video_graph.common.utils.tools import parse_bbs_resource_id, auto_size
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class BuildTimelineFromTableOp(Op):
    """
    根据表数据构建Timeline

    Attributes:
        request_column (str): 请求列名，默认为"request"。
        video_match_res_column (str): 视频匹配结果列名，默认为"video_match_res"。
        video_blob_key_column (str): 视频Blob Key列名，默认为"mask_ocr_blob_key"。
        tts_blob_key_column (str): TTS Blob Key列名，默认为"tts_blob_key"。
        tts_duration_column (str): TTS时长列名，默认为"tts_duration"。
        tts_volume (float): TTS音量，默认为1。

    InputTables:
        shot_table: 镜号表。
        material_table: 物料表。
        timeline_table: 时间线表。

    OutputTables:
        timeline_table: 添加了镜号的时间线表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_op/build_timeline_from_table_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        material_table: DataTable = op_context.input_tables[1]
        timeline_table: DataTable = op_context.input_tables[2]
        request_column = self.attrs.get("request_column", "request")
        video_match_res_column = self.attrs.get("video_match_res_column", "video_match_res")
        video_blob_key_column = self.attrs.get("video_blob_key_column", "mask_ocr_blob_key")
        tts_blob_key_column = self.attrs.get("tts_blob_key_column", "tts_blob_key")
        tts_duration_column = self.attrs.get("tts_duration_column", "tts_duration")
        tts_volume = self.attrs.get("tts_volume", 1)

        request = timeline_table.loc[0, request_column]
        resolution = request.resolution

        cur_end_time = 0
        for index, row in shot_table.iterrows():
            tts_blob_key = row.get(tts_blob_key_column)
            tts_duration = row.get(tts_duration_column)
            new_clip = Clip()
            new_clip.start_time = 0 if len(request.clip) == 0 else request.clip[-1].end_time
            new_clip.clip_type = ClipType.VIDEO

            tts_db, tts_table, tts_key = parse_bbs_resource_id(tts_blob_key)
            tts_source = ProtoBlobStoreKey()
            tts_source.db = tts_db
            tts_source.table = tts_table
            tts_source.key = tts_key

            add_audio_op = AddAudioOp()
            add_audio_op.filename.CopyFrom(tts_source)
            add_audio_op.volume = tts_volume
            add_audio_op.start_time = 0
            add_audio_op.end_time = tts_duration
            op_pb = OpPb()
            op_pb.op_type = OpType.ADD_AUDIO_OP_TYPE
            op_pb.add_audio_op.CopyFrom(add_audio_op)
            new_clip.ops.append(op_pb)
            new_clip.end_time = new_clip.start_time + tts_duration

            video_match_res = row.get(video_match_res_column)
            for line_videos in video_match_res.get('clips_info'):
                tts_start_time = line_videos.get("tts_start_time")
                tts_end_time = line_videos.get("tts_end_time")
                tts_duration = tts_end_time - tts_start_time
                video_clips = line_videos.get("video_clips")
                video_clips_duration = line_videos.get("video_clips_duration")

                # 添加视频片段
                speed = video_clips_duration / tts_duration
                for video_clip in video_clips:
                    video_idx = int(video_clip.get("video_idx"))
                    video_start_time = video_clip.get("video_start_time")
                    video_end_time = video_clip.get("video_end_time")
                    video_duration = video_end_time - video_start_time

                    target_material = material_table.loc[video_idx - 1]
                    video_blob_key = target_material.get(video_blob_key_column)
                    video_db, video_table, video_key = parse_bbs_resource_id(video_blob_key)
                    video_size = (target_material.get("width"), target_material.get("height"))

                    new_video = Video()
                    new_video.filename.CopyFrom(ProtoBlobStoreKey(db=video_db, table=video_table, key=video_key))
                    new_video.time_clip_start = video_start_time
                    new_video.time_clip_end = video_end_time
                    new_video.is_muted = True

                    # 变速对齐时间线
                    video_speed_op = VideoSpeedOp()
                    time_point = video_speed_op.time_point.add()
                    time_point.result_time = 0
                    time_point.clip_cut_time = 0
                    time_point.speed = speed
                    time_point = video_speed_op.time_point.add()
                    time_point.result_time = video_duration / speed
                    time_point.clip_cut_time = video_duration
                    time_point.speed = speed
                    op_pb = OpPb()
                    op_pb.op_type = OpType.VIDEO_SPEED_OP_TYPE
                    op_pb.video_speed_op.CopyFrom(video_speed_op)
                    new_video.start_time = cur_end_time
                    new_video.end_time = new_video.start_time + time_point.result_time

                    cur_end_time = new_video.end_time
                    new_video.ops.append(op_pb)

                    # 适配宽高
                    video_resize_op = VideoResizeOp()
                    if resolution[0] < resolution[1]:
                        target_size = auto_size(video_size, resolution, mode="align_width")
                    else:
                        target_size = auto_size(video_size, resolution, mode="align_height")
                    video_resize_op.width = target_size[0]
                    video_resize_op.height = target_size[1]
                    op_pb = OpPb()
                    op_pb.op_type = OpType.VIDEO_RESIZE_OP_TYPE
                    op_pb.video_resize_op.CopyFrom(video_resize_op)
                    new_video.ops.append(op_pb)

                    new_clip.videos.append(new_video)
            request.clip.append(new_clip)

        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(BuildTimelineFromTableOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_input(name="material_table", type="DataTable", desc="物料表") \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_output(name="video_render_table", type="DataTable", desc="视频渲染表") \
    .add_attr(name="request_column", type="str", desc="request列名") \
    .add_attr(name="video_match_res_column", type="str", desc="视频匹配结果列名") \
    .add_attr(name="video_blob_key_column", type="str", desc="视频blob地址列名") \
    .add_attr(name="tts_blob_key_column", type="str", desc="音频blob地址列名") \
    .add_attr(name="tts_duration_column", type="str", desc="音频时长列名") \
    .add_attr(name="tts_volume", type="float", desc="音频音量")
