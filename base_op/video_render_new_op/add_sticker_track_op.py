from ks_editor_kernel.model_builder.video_project_model_builder import EditSceneVideoProjectBuilder, AssetInfo
from ks_editor_kernel.pb.CommonDraftBaseAssetModel_pb2 import TimeRangeModel, AssetTransform

from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AddStickerTrackOp(Op):
    """
    【local】添加贴纸算子，可以设置贴纸的位置、大小、缩放比例等

    Attributes:
        project_model_builder_column (str): 项目模型构建器列名，默认为"project_model_builder"。
        input_file_map_column (str): 输入文件映射列名，默认为"input_file_map"。
        sticker_blob_key_column (str): 贴纸blob key列名，默认为"sticker_blob_key"。
        width_column (str): 宽度列名，默认为"width"。
        height_column (str): 高度列名，默认为"height"。
        resolution_column (str): 分辨率列名，默认为"resolution"。
        scale_ratio (float): 缩放比例，默认为1.0。
        position (str): 贴纸位置，默认为"top_right"。
        position_x (str): 贴纸横坐标位置，默认为-1。
        position_y (str): 贴纸纵坐标位置，默认为-1。

    InputTables:
        timeline_table: 时间线表格。
        request_table: 请求表格。

    OutputTables:
        timeline_table: 添加了贴纸的时间线表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_new_op/add_sticker_track_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute_when_skip(self, op_context: OpContext) -> bool:
        op_context.output_tables.append(op_context.input_tables[0])
        return True

    def compute(self, op_context: OpContext) -> bool:
        timeline_table: DataTable = op_context.input_tables[0]
        request_table: DataTable = op_context.input_tables[1]
        project_model_builder_column = self.attrs.get("project_model_builder_column", "project_model_builder")
        input_file_map_column = self.attrs.get("input_file_map_column", "input_file_map")
        sticker_blob_key_column = self.attrs.get("sticker_blob_key_column", "sticker_blob_key")
        width_column = self.attrs.get("width_column", "width")
        height_column = self.attrs.get("height_column", "height")
        resolution_column = self.attrs.get("resolution_column", "resolution")
        scale_ratio = self.attrs.get("scale_ratio", 1.0)
        position = self.attrs.get("position", "top_right")
        position_x = self.attrs.get("position_x", -1)
        position_y = self.attrs.get("position_y", -1)

        project_model_builder: EditSceneVideoProjectBuilder = timeline_table.at[0, project_model_builder_column]
        input_file_map: dict = timeline_table.at[0, input_file_map_column]
        resolution = timeline_table.at[0, resolution_column]
        sticker_blob_key = request_table.at[0, sticker_blob_key_column]
        width = int(request_table.at[0, width_column] * scale_ratio)
        height = int(request_table.at[0, height_column] * scale_ratio)
        input_file_map.update({sticker_blob_key: sticker_blob_key})
        if position_x > 0 and position_y > 0:
            # 使用传进来的 position_x 和 position_y
            pass
        else:
            if position == "top_right":
                position_x = 100 - ((width / 2) / resolution[0] * 100)
                position_y = (height / 2) / resolution[1] * 100
            elif position == "top_left":
                position_x = (width / 2) / resolution[0] * 100
                position_y = (height / 2) / resolution[1] * 100
            elif position == "bottom_right":
                position_x = 100 - ((width / 2) / resolution[0] * 100)
                position_y = 100 - ((height / 2) / resolution[1] * 100)
            elif position == "bottom_left":
                position_x = (width / 2) / resolution[0] * 100
                position_y = 100 - ((height / 2) / resolution[1] * 100)
            else:
                # 兜底为右上角
                position_x = 100 - ((width / 2) / resolution[0] * 100)
                position_y = (height / 2) / resolution[1] * 100

        project_model_builder.bind_time_model_guru_video_project()
        main_track_duration = project_model_builder.get_project_duration()

        sticker_asset_id = project_model_builder.add_sticker_asset(AssetInfo(
            path=sticker_blob_key,
            duration=main_track_duration,
            width=width,
            height=height,
            clip_range=TimeRangeModel(startTime=0, endTime=main_track_duration),
            file_type=0
        ))
        project_model_builder.set_transform_of_asset(sticker_asset_id, AssetTransform(
            positionX=position_x,
            positionY=position_y,
            scaleX=width / resolution[0] * 100,
            scaleY=height / resolution[1] * 100
        ))
        project_model_builder.set_display_range_of_asset(sticker_asset_id, TimeRangeModel(
            startTime=0,
            endTime=main_track_duration
        ))

        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(AddStickerTrackOp) \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_input(name="request_table", type="DataTable", desc="请求表") \
    .add_output(name="timeline_table", type="DataTable", desc="视频渲染表")
