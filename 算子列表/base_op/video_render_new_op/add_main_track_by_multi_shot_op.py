import json

from ks_editor_kernel.model_builder.video_project_model_builder import EditSceneVideoProjectBuilder, AssetInfo
from ks_editor_kernel.pb.CommonDraftBaseAssetModel_pb2 import TimeRangeModel

from video_graph.common.utils.minecraft_tools import modify_asset_speed, set_asset_muted
from video_graph.common.utils.tools import auto_size
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AddMainTrackByMultiShotOp(Op):
    """
    Function:
        从标准输入表中添加视频主轨，基于一个标准结构render_video_list来生成，其中包含四部分：
        * video_idx：视频表的index
        * start_time：视频片段开始时间
        * end_time：视频片段结束时间
        * speed：播放速度

    Attributes:
        project_model_builder_column (str): 项目模型构建器列名，默认为"project_model_builder"。
        input_file_map_column (str): 输入文件映射列名，默认为"input_file_map"。
        resolution_column (str): 分辨率列名，默认为"resolution"。
        render_video_list_column (str): 渲染视频列表列名，默认为"render_video_list"。
        video_blob_key_column (str): 视频blob key列名，默认为"mask_ocr_blob_key"。
        video_muted_column (str): 视频静音列名，默认为"video_muted"。
        origin_video_blob_key_column (str): 原始视频blob key列名，默认为"video_blob_key"。
        video_from_column (str): 视频来源列名，默认为"video_from"。
        video_list_map_column (str): 视频列表映射列名，默认为"video_list_map"。
        video_clip_list_column (str): 视频剪辑列表列名，默认为"video_clip_list"。
        video_asset_id_column (str): 草稿中的视频资源id列名，默认为"video_asset_id"
        video_width_column (str): 视频的宽度列名，默认为"width"
        video_height_column (str): 视频的高度列名，默认为"height"

    InputTables:
        timeline_table: 时间线表格。
        shot_table: 镜头表格。
        material_table: 素材表格。

    OutputTables:
        timeline_table: 添加了主轨的时间线表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_new_op/add_main_track_by_multi_shot_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """
    def compute_when_skip(self, op_context: OpContext):
        op_context.output_tables.append(op_context.input_tables[0])
        return True

    def compute(self, op_context: OpContext) -> bool:
        timeline_table: DataTable = op_context.input_tables[0]
        shot_table: DataTable = op_context.input_tables[1]
        material_table: DataTable = op_context.input_tables[2]
        project_model_builder_column = self.attrs.get("project_model_builder_column", "project_model_builder")
        input_file_map_column = self.attrs.get("input_file_map_column", "input_file_map")
        resolution_column = self.attrs.get("resolution_column", "resolution")
        render_video_list_column = self.attrs.get("render_video_list_column", "render_video_list")
        video_blob_key_column = self.attrs.get("video_blob_key_column", "mask_ocr_blob_key")
        video_muted_column = self.attrs.get("video_muted_column", "video_muted")
        origin_video_blob_key_column = self.attrs.get("origin_video_blob_key_column", "video_blob_key")
        video_from_column = self.attrs.get("video_from_column", "video_from")
        video_list_map_column = self.attrs.get("video_list_map_column", "video_list_map")
        video_clip_list_column = self.attrs.get("video_clip_list_column", "video_clip_list")
        video_asset_id_column = self.attrs.get("video_asset_id_column", "video_asset_id")
        video_width_column = self.attrs.get("video_width_column", "width")
        video_height_column = self.attrs.get("video_height_column", "height")

        project_model_builder: EditSceneVideoProjectBuilder = timeline_table.at[0, project_model_builder_column]
        input_file_map: dict = timeline_table.at[0, input_file_map_column]
        resolution = timeline_table.at[0, resolution_column]

        video_asset_ids = []
        video_list_map = {}
        video_clip_list = []
        for index, row in shot_table.iterrows():
            video_list = row.get(render_video_list_column)
            video_muted = row.get(video_muted_column)
            for video_idx, start_time, end_time, speed in video_list:
                target_material = material_table.iloc[int(video_idx)]
                video_blob_key = target_material.get(video_blob_key_column)
                video_from = target_material.get(video_from_column, "private")

                # 记录剪辑片段信息
                origin_video_blob_key = target_material.get(origin_video_blob_key_column)
                if video_from not in video_list_map:
                    video_list_map[video_from] = set()
                video_list_map[video_from].add(origin_video_blob_key)
                video_clip_list.append((video_blob_key, start_time, end_time, video_from))

                video_size = (target_material.get(video_width_column), target_material.get(video_height_column))
                video_duration = end_time - start_time

                if resolution[0] < resolution[1]:
                    target_size = auto_size(video_size, resolution, mode="align_width")
                else:
                    target_size = auto_size(video_size, resolution, mode="align_height")
                video_asset_id = project_model_builder.add_main_track_asset(
                    AssetInfo(path=video_blob_key,
                              duration=video_duration,
                              width=target_size[0],
                              height=target_size[1],
                              clip_range=TimeRangeModel(startTime=start_time, endTime=end_time))
                )
                video_asset_ids.append(str(video_asset_id))
                input_file_map.update({video_blob_key: video_blob_key})
                project_model_builder.modify_asset_model(video_asset_id, writer=modify_asset_speed(speed))
                if video_muted:
                    project_model_builder.modify_asset_model(video_asset_id, writer=set_asset_muted)

        project_model_builder.align_main_track()
        timeline_table[video_list_map_column] = None
        timeline_table[video_clip_list_column] = None
        timeline_table.at[0, video_list_map_column] = video_list_map
        timeline_table.at[0, video_clip_list_column] = json.dumps(video_clip_list)

        if video_asset_id_column in timeline_table.columns:
            video_asset_id_list = timeline_table.at[0, video_asset_id_column]
            video_asset_id_list.extend(video_asset_ids)
        else:
            timeline_table[video_asset_id_column] = None
            timeline_table.at[0, video_asset_id_column] = video_asset_ids

        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(AddMainTrackByMultiShotOp) \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_input(name="material_table", type="DataTable", desc="物料表") \
    .add_output(name="timeline_table", type="DataTable", desc="视频渲染表")
