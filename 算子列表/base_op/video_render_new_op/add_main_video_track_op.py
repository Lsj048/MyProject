import json

from ks_editor_kernel.model_builder.asset_model_builder import MCVideoTrackAssetBuilder
from ks_editor_kernel.model_builder.video_project_model_builder import EditSceneVideoProjectBuilder, AssetInfo
from ks_editor_kernel.pb.CommonDraftBaseAssetModel_pb2 import TimeRangeModel

from video_graph.common.utils.minecraft_tools import set_asset_muted
from video_graph.common.utils.tools import auto_size
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AddMainVideoTrackOp(Op):
    """
    Function:
        增加主视频轨算子，video_index为-1时，表示批量增加视频到一条轨道上，按添加时间依次排列，否则表示增加单个视频到轨道上

    Attributes:
        project_model_builder_column (str): 项目模型构建器列名，默认为"project_model_builder"。
        input_file_map_column (str): 输入文件映射列名，默认为"input_file_map"。
        video_index (int): 视频索引，默认为0。
        video_blob_key_column (str): 视频blob key列名，默认为"video_blob_key"。
        start_time_column (str): 开始时间列名，默认为"start_time"。
        end_time_column (str): 结束时间列名，默认为"end_time"。
        width_column (str): 视频宽度列名，默认为"width"
        height_column (str): 视频高度列名，默认为"height"
        is_muted (bool): 是否静音，默认为True。
        origin_video_blob_key_column (str): 原视频blob key列名，默认为"video_blob_key"。
        video_from_column (str): 视频来源列名，默认为"video_from"。
        video_list_map_column (str): 视频列表映射列名，默认为"video_list_map"。
        video_clip_list_column (str): 视频片段列表列名，默认为"video"
        video_asset_id_column (str): 草稿中的视频资源id列名，默认为"video_asset_id"
        head_video (bool): 是否为片头视频，默认为False
        resolution_column (str): 渲染视频的分辨率列名，默认为"resolution"

    InputTables:
        timeline_table: 时间线表格。
        material_table: 素材表格。

    OutputTables:
        timeline_table: 添加了视频轨的时间线表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_new_op/add_main_video_track_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """
    def compute(self, op_context: OpContext) -> bool:
        timeline_table: DataTable = op_context.input_tables[0]
        material_table: DataTable = op_context.input_tables[1]
        project_model_builder_column = self.attrs.get("project_model_builder_column", "project_model_builder")
        input_file_map_column = self.attrs.get("input_file_map_column", "input_file_map")
        video_index = self.attrs.get("video_index", 0)
        video_blob_key_column = self.attrs.get("video_blob_key_column", "video_blob_key")
        start_time_column = self.attrs.get("start_time_column", "start_time")
        end_time_column = self.attrs.get("end_time_column", "end_time")
        width_column = self.attrs.get("width_column", "width")
        height_column = self.attrs.get("height_column", "height")
        is_muted = self.attrs.get("is_muted", True)
        origin_video_blob_key_column = self.attrs.get("origin_video_blob_key_column", "video_blob_key")
        video_from_column = self.attrs.get("video_from_column", "video_from")
        video_list_map_column = self.attrs.get("video_list_map_column", "video_list_map")
        video_clip_list_column = self.attrs.get("video_clip_list_column", "video_clip_list")
        video_asset_id_column = self.attrs.get("video_asset_id_column", "video_asset_id")
        head_video = self.attrs.get("head_video", False)
        resolution_column = self.attrs.get("resolution_column", "resolution")

        input_file_map: dict = timeline_table.loc[0, input_file_map_column]
        project_model_builder: EditSceneVideoProjectBuilder = timeline_table.loc[0, project_model_builder_column]
        resolution = timeline_table.at[0, resolution_column]

        video_asset_ids = []
        if 0 <= video_index < material_table.shape[0]:
            row = material_table.iloc[video_index]
            video_blob_key = row.get(video_blob_key_column)
            start_time = row.get(start_time_column)
            end_time = row.get(end_time_column)
            duration = end_time - start_time
            video_size = (row.get(width_column, 0), row.get(height_column, 0))

            if resolution[0] < resolution[1]:
                target_size = auto_size(video_size, resolution, mode="align_width")
            else:
                target_size = auto_size(video_size, resolution, mode="align_height")

            input_file_map.update({video_blob_key: video_blob_key})
            if head_video:
                builder = MCVideoTrackAssetBuilder()
                builder.path = video_blob_key
                builder.duration = duration
                builder.output_width = target_size[0]
                builder.output_height = target_size[1]
                builder.clip_range = TimeRangeModel(startTime=start_time, endTime=end_time)

                target_asset = builder.fetch_model()
                project_model_builder._model.trackAssets.insert(0, target_asset)
                video_asset_id = target_asset.base.base.id
            else:
                video_asset_id = project_model_builder.add_main_track_asset(AssetInfo(
                    path=video_blob_key,
                    duration=duration,
                    width=target_size[0],
                    height=target_size[1],
                    clip_range=TimeRangeModel(startTime=start_time, endTime=end_time)
                ))
            if is_muted:
                project_model_builder.modify_asset_model(video_asset_id, writer=set_asset_muted)
            video_asset_ids.append(str(video_asset_id))
        elif video_index == -1:
            video_list_map = {}
            video_clip_list = []

            for index, row in material_table.iterrows():
                video_blob_key = row.get(video_blob_key_column)
                start_time = row.get(start_time_column)
                end_time = row.get(end_time_column)
                duration = end_time - start_time
                video_size = (row.get(width_column, 0), row.get(height_column, 0))

                if resolution[0] < resolution[1]:
                    target_size = auto_size(video_size, resolution, mode="align_width")
                else:
                    target_size = auto_size(video_size, resolution, mode="align_height")

                # 记录剪辑片段信息
                video_from = row.get(video_from_column, "private")
                origin_video_blob_key = row.get(origin_video_blob_key_column)
                if video_from not in video_list_map:
                    video_list_map[video_from] = set()
                video_list_map[video_from].add(origin_video_blob_key)
                video_clip_list.append((video_blob_key, start_time, end_time, video_from))

                input_file_map.update({video_blob_key: video_blob_key})
                video_asset_id = project_model_builder.add_main_track_asset(AssetInfo(
                    path=video_blob_key,
                    duration=duration,
                    width=target_size[0],
                    height=target_size[1],
                    clip_range=TimeRangeModel(startTime=start_time, endTime=end_time)
                ))
                video_asset_ids.append(str(video_asset_id))
                if is_muted:
                    project_model_builder.modify_asset_model(video_asset_id, writer=set_asset_muted)

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

        project_model_builder.align_main_track()
        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(AddMainVideoTrackOp) \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_input(name="material_table", type="DataTable", desc="物料表") \
    .add_output(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_attr(name="project_model_builder_column", type="str", desc="项目模型构建列名") \
    .add_attr(name="input_file_map_column", type="str", desc="输入文件映射列名") \
    .add_attr(name="video_index", type="int", desc="视频编号") \
    .add_attr(name="video_blob_key_column", type="str", desc="视频blob key列名") \
    .add_attr(name="start_time_column", type="str", desc="开始时间列名") \
    .add_attr(name="end_time_column", type="str", desc="结束时间列名") \
    .add_attr(name="is_muted", type="bool", desc="视频是否静音")
