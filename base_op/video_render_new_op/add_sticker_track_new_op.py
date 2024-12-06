from ks_editor_kernel.model_builder.video_project_model_builder import EditSceneVideoProjectBuilder, AssetInfo
from ks_editor_kernel.pb.CommonDraftBaseAssetModel_pb2 import TimeRangeModel, AssetTransform

from video_graph.common.utils.minecraft_tools import get_asset_clip_range_start_time
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AddStickerTrackNewOp(Op):
    """
    Function:
        添加贴纸算子，sticker_index为-1时，表示批量增加贴纸到一条轨道上，时空上叠加，否则表示增加单个贴纸到轨道上

    Attributes:
        project_model_builder_column (str): 项目模型构建器列名，默认为"project_model_builder"。
        input_file_map_column (str): 输入文件映射列名，默认为"input_file_map"。
        sticker_index (int): 贴纸索引，默认为0。
        sticker_blob_key_column (str): 贴纸blob key列名，默认为"sticker_blob_key"。
        width_column (str): 宽度列名，默认为"width"。
        height_column (str): 高度列名，默认为"height"。
        resolution_column (str): 分辨率列名，默认为"resolution"。
        scale_x_column (str): x轴缩放比例列名，默认为"scale_x"。
        scale_y_column (str): y轴缩放比例列名，默认为"scale_y"。
        position_column (str): 位置列名，默认为"position"。
        position_x_column (str): x坐标列名，默认为"position_x"。
        position_y_column (str): y坐标列名，默认为"position_y"。
        video_asset_id_column (str): 草稿中的视频资源id列名，默认为"video_asset_id"。
        binding_main_track (bool): 是否绑定到主视频轨道，默认为False。

    InputTables:
        timeline_table: 时间线表格。
        sticker_table: 贴纸表格。

    OutputTables:
        timeline_table: 添加了贴纸的时间线表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_new_op/add_sticker_track_new_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """
    def compute_when_skip(self, op_context: OpContext) -> bool:
        op_context.output_tables.append(op_context.input_tables[0])
        return True

    def compute(self, op_context: OpContext) -> bool:
        timeline_table: DataTable = op_context.input_tables[0]
        sticker_table: DataTable = op_context.input_tables[1]
        project_model_builder_column = self.attrs.get("project_model_builder_column", "project_model_builder")
        input_file_map_column = self.attrs.get("input_file_map_column", "input_file_map")
        sticker_index = self.attrs.get("sticker_index", 0)
        sticker_name_column = self.attrs.get("sticker_name_column", "sticker_name")
        sticker_blob_key_column = self.attrs.get("sticker_blob_key_column", "sticker_blob_key")
        width_column = self.attrs.get("width_column", "width")
        height_column = self.attrs.get("height_column", "height")
        resolution_column = self.attrs.get("resolution_column", "resolution")
        scale_x_column = self.attrs.get("scale_x_column", "scale_x")
        scale_y_column = self.attrs.get("scale_y_column", "scale_y")
        position_column = self.attrs.get("position_column", "position")
        position_x_column = self.attrs.get("position_x_column", "position_x")
        position_y_column = self.attrs.get("position_y_column", "position_y")
        video_asset_id_column = self.attrs.get("video_asset_id_column", "video_asset_id")
        binding_main_track = self.attrs.get("binding_main_track", False)

        project_model_builder: EditSceneVideoProjectBuilder = timeline_table.at[0, project_model_builder_column]
        input_file_map: dict = timeline_table.at[0, input_file_map_column]
        resolution = timeline_table.at[0, resolution_column]
        video_asset_ids = timeline_table.at[0, video_asset_id_column]
        binding_video_asset_id = int(video_asset_ids[0]) if len(video_asset_ids) > 0 else 0
        # 如果需要绑定，则DisplayRange的startTime为宿主的ClipRange的startTime
        if binding_video_asset_id != 0 and binding_main_track:
            binding_video_start_time = get_asset_clip_range_start_time(project_model_builder, binding_video_asset_id)
        else:
            binding_video_start_time = 0
        project_model_builder.bind_time_model_guru_video_project()
        main_track_duration = project_model_builder.get_project_duration()

        if 0 <= sticker_index < len(sticker_table):
            row = sticker_table.iloc[sticker_index]
            sticker_name = row.get(sticker_name_column, "sticker")
            sticker_blob_key = row.get(sticker_blob_key_column)
            if str(sticker_blob_key) == 'nan' or sticker_blob_key is None:
                return False
            scale_x = row.get(scale_x_column, 1.0)
            scale_y = row.get(scale_y_column, 1.0)
            position = row.get(position_column, "top_right")
            position_x = row.get(position_x_column, -1)
            position_y = row.get(position_y_column, -1)
            width = int(row.get(width_column) * scale_x)
            height = int(row.get(height_column) * scale_y)
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
                elif position == "center":
                    position_x = 50
                    position_y = 50
                else:
                    # 兜底为右上角
                    position_x = 100 - ((width / 2) / resolution[0] * 100)
                    position_y = (height / 2) / resolution[1] * 100

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
                startTime=binding_video_start_time,
                endTime=binding_video_start_time+main_track_duration
            ))
            timeline_table.at[0, f"{sticker_name}_asset_id"] = str(sticker_asset_id)
            if binding_video_asset_id != 0 and binding_main_track:
                project_model_builder.set_bind_track_id_of_asset(sticker_asset_id, binding_video_asset_id)
        elif sticker_index == -1:
            for index, row in sticker_table.iterrows():
                sticker_name = row.get(sticker_name_column, "sticker")
                sticker_blob_key = row.get(sticker_blob_key_column)
                if str(sticker_blob_key) == 'nan' or sticker_blob_key is None:
                    continue
                scale_x = row.get(scale_x_column, 1.0)
                scale_y = row.get(scale_y_column, 1.0)
                position = row.get(position_column, "top_right")
                position_x = row.get(position_x_column, -1)
                position_y = row.get(position_y_column, -1)
                width = int(row.get(width_column) * scale_x)
                height = int(row.get(height_column) * scale_y)
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
                    elif position == "center":
                        position_x = 50
                        position_y = 50
                    else:
                        # 兜底为右上角
                        position_x = 100 - ((width / 2) / resolution[0] * 100)
                        position_y = (height / 2) / resolution[1] * 100

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
                    startTime=binding_video_start_time,
                    endTime=binding_video_start_time+main_track_duration
                ))
                timeline_table.at[0, f"{sticker_name}_asset_id"] = str(sticker_asset_id)
                if binding_video_asset_id != 0 and binding_main_track:
                    project_model_builder.set_bind_track_id_of_asset(sticker_asset_id, binding_video_asset_id)

        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(AddStickerTrackNewOp) \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_input(name="sticker_table", type="DataTable", desc="贴纸表") \
    .add_output(name="timeline_table", type="DataTable", desc="视频渲染表")
