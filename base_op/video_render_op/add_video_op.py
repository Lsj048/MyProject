from video_graph.common.proto.rpc_proto.video_script_pb2 import Op as OpPb
from video_graph.common.proto.rpc_proto.video_script_pb2 import VideoScriptRequest, ProtoBlobStoreKey, Clip, ClipType, \
    Video, OpType, VideoResizeOp, VideoPosOp
from video_graph.common.utils.tools import parse_bbs_resource_id, auto_size
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AddVideoOp(Op):
    """
    增加视频片段算子

    Attributes:
        request_column (str): 请求列名，默认为"request"。
        video_index (int): 视频索引，默认为0。
        video_blob_key_column (str): 视频Blob Key列名，默认为"video_blob_key"。
        video_start_time (int): 视频开始时间，默认为0。
        video_duration_column (str): 视频时长列名，默认为"duration"。
        is_muted (bool): 是否静音，默认为True。
        video_resize_ratio (float): 视频缩放比例，默认为0.4。
        video_center_pos (list): 视频中心位置，宽：left/right/center，高：top/bottom/center，默认为["left", "top"]。
        pip_margin (list): 画中画边距，默认为[20, 30]。
        video_width_column (str): 视频宽度列名，默认为"width"。
        video_height_column (str): 视频高度列名，默认为"height"。
        is_append_video (bool): 是否为追加视频，默认为False。

    InputTables:
        timeline_table: 时间线表。
        material_table: 物料表。

    OutputTables:
        timeline_table: 添加了视频片段的时间线表

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_op/add_video_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        timeline_table: DataTable = op_context.input_tables[0]
        material_table: DataTable = op_context.input_tables[1]
        request_column = self.attrs.get("request_column", "request")
        video_index = self.attrs.get("video_index", 0)
        video_blob_key_column = self.attrs.get("video_blob_key_column", "video_blob_key")
        video_start_time = self.attrs.get("video_start_time", 0)
        video_duration_column = self.attrs.get("video_duration_column", "duration")
        is_muted = self.attrs.get("is_muted", True)
        video_resize_ratio = self.attrs.get("video_resize_ratio", 0.4)
        video_center_pos = self.attrs.get("video_center_pos", ["left", "top"])
        pip_margin = self.attrs.get("pip_margin", [20, 30])
        video_width_column = self.attrs.get("video_width_column", "width")
        video_height_column = self.attrs.get("video_height_column", "height")
        is_append_video = self.attrs.get("is_append_video", False)

        video_size = [material_table.loc[video_index, video_width_column],
                      material_table.loc[video_index, video_height_column]]
        video_blob_key = material_table.loc[video_index, video_blob_key_column]
        video_duration = material_table.loc[video_index, video_duration_column]
        video_end_time = video_start_time + video_duration
        db, table, key = parse_bbs_resource_id(video_blob_key)

        request: VideoScriptRequest = timeline_table.loc[0, request_column]
        resolution = request.resolution
        target_resolution = [item * video_resize_ratio for item in resolution]
        # 计算画中画坐标
        if video_center_pos[0] == "left":
            video_center_pos_w = pip_margin[0] + target_resolution[0] / 2
        elif video_center_pos[0] == "right":
            video_center_pos_w = resolution[0] - pip_margin[0] - target_resolution[0] / 2
        else:
            video_center_pos_w = 0.5 * resolution[0]
        if video_center_pos[1] == "top":
            video_center_pos_h = pip_margin[1] + target_resolution[1] / 2
        elif video_center_pos[1] == "bottom":
            video_center_pos_h = resolution[1] - pip_margin[1] - target_resolution[1] / 2
        else:
            video_center_pos_h = 0.5 * resolution[1]
        if len(request.clip) == 0:
            new_clip = Clip()
            new_clip.clip_type = ClipType.VIDEO
            request.clip.append(new_clip)

        clip = request.clip[-1]
        if is_append_video:
            start_time = clip.end_time
            end_time = start_time + video_duration
        else:
            start_time = clip.start_time
            end_time = min(clip.end_time, start_time + video_duration)
            real_duration = end_time - start_time
            video_end_time = video_start_time + real_duration

        new_video = Video()
        new_video.filename.CopyFrom(ProtoBlobStoreKey(db=db, table=table, key=key))
        new_video.start_time = start_time
        new_video.end_time = end_time
        new_video.time_clip_start = video_start_time
        new_video.time_clip_end = video_end_time
        new_video.is_muted = is_muted

        # 调整尺寸
        video_resize_op = VideoResizeOp()
        if target_resolution[0] < target_resolution[1]:
            target_size = auto_size(video_size, target_resolution, mode="align_width")
        else:
            target_size = auto_size(video_size, target_resolution, mode="align_height")
        video_resize_op.width = target_size[0]
        video_resize_op.height = target_size[1]
        op_pb = OpPb()
        op_pb.op_type = OpType.VIDEO_RESIZE_OP_TYPE
        op_pb.video_resize_op.CopyFrom(video_resize_op)
        new_video.ops.append(op_pb)

        # 调整位置
        video_pos_op = VideoPosOp()
        video_pos_op.center_pos_w = video_center_pos_w
        video_pos_op.center_pos_h = video_center_pos_h
        op_pb = OpPb()
        op_pb.op_type = OpType.VIDEO_POS_OP_TYPE
        op_pb.video_pos_op.CopyFrom(video_pos_op)
        new_video.ops.append(op_pb)

        clip.videos.append(new_video)
        if is_append_video:
            clip.end_time += video_duration
        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(AddVideoOp) \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_input(name="material_table", type="DataTable", desc="物料表") \
    .add_output(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_attr(name="request_column", type="str", desc="request列名") \
    .add_attr(name="video_index", type="int", desc="视频编号") \
    .add_attr(name="video_blob_key_column", type="str", desc="视频blob key列名") \
    .add_attr(name="video_start_time", type="int", desc="视频开始时间") \
    .add_attr(name="video_duration_column", type="str", desc="视频时长列名") \
    .add_attr(name="is_muted", type="bool", desc="视频是否静音") \
    .add_attr(name="video_resize_ratio", type="float", desc="视频缩放比例") \
    .add_attr(name="video_center_pos", type="list", desc="视频中心位置") \
    .add_attr(name="pip_margin", type="list", desc="pip边距") \
    .add_attr(name="video_width_column", type="str", desc="视频宽度列名") \
    .add_attr(name="video_height_column", type="str", desc="视频高度列名") \
    .add_attr(name="is_append_video", type="bool", desc="是否为追加视频")
