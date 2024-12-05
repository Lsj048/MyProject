from ks_editor_kernel.model_builder.video_project_model_builder import EditSceneVideoProjectBuilder, AssetInfo
from ks_editor_kernel.pb.CommonDraftBaseAssetModel_pb2 import TimeRangeModel, AssetTransform

from video_graph.common.utils.minecraft_tools import set_asset_muted
from video_graph.common.utils.tools import auto_size
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AddSubVideoTrackOp(Op):
    """
    【local】增加副视频轨算子

    Attributes:
        project_model_builder_column (str): 项目模型构建器列名，默认为"project_model_builder"。
        input_file_map_column (str): 输入文件映射列名，默认为"input_file_map"。
        video_index (int): 视频索引，默认为0。
        video_blob_key_column (str): 视频blob key列名，默认为"video_blob_key"。
        start_time_column (str): 开始时间列名，默认为"start_time"。
        end_time_column (str): 结束时间列名，默认为"end_time"。
        width_column (str): 宽度列名，默认为"width"。
        height_column (str): 高度列名，默认为"height"。
        resolution_column (str): 分辨率列名，默认为"resolution"。
        is_muted (bool): 是否静音，默认为True。
        target_video_bbox (list): 目标视频bbox，默认为[0, 0, 100, 100]。
        sub_video_asset_id_column (str): 子视频资产id列名，默认为"sub_video_asset_id"。
    
    InputTables:
        timeline_table: 时间线表。
        material_table: 素材表。

    OutputTables:
        timeline_table: 时间线表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_new_op/add_sub_video_track_op.py?ref_type=heads

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
        resolution_column = self.attrs.get("resolution_column", "resolution")
        is_muted = self.attrs.get("is_muted", True)
        target_video_bbox = self.attrs.get("target_video_bbox", [0, 0, 100, 100])
        sub_video_asset_id_column = self.attrs.get("sub_video_asset_id_column", "sub_video_asset_id")

        input_file_map: dict = timeline_table.loc[0, input_file_map_column]
        project_model_builder: EditSceneVideoProjectBuilder = timeline_table.loc[0, project_model_builder_column]
        resolution = timeline_table.at[0, resolution_column]

        position_x = (target_video_bbox[0] + target_video_bbox[2]) / 2.0
        position_y = (target_video_bbox[1] + target_video_bbox[3]) / 2.0
        scale_x = (target_video_bbox[2] - target_video_bbox[0]) / 100.0
        scale_y = (target_video_bbox[3] - target_video_bbox[1]) / 100.0
        target_resolution = (int((target_video_bbox[2] - target_video_bbox[0]) / 100.0 * resolution[0]),
                             int((target_video_bbox[3] - target_video_bbox[1]) / 100.0 * resolution[1]))

        sub_video_asset_ids = []
        if 0 <= video_index < material_table.shape[0]:
            row = material_table.iloc[video_index]
            video_blob_key = row.get(video_blob_key_column)
            start_time = row.get(start_time_column)
            end_time = row.get(end_time_column)
            width = int(row.get(width_column, 0) * scale_x)
            height = int(row.get(height_column, 0) * scale_y)
            duration = end_time - start_time

            if target_resolution[0] < target_resolution[1]:
                target_size = auto_size((width, height), target_resolution, mode="align_width")
            else:
                target_size = auto_size((width, height), target_resolution, mode="align_height")

            input_file_map.update({video_blob_key: video_blob_key})
            sub_video_asset_id = project_model_builder.add_sub_asset(AssetInfo(
                path=video_blob_key,
                duration=duration,
                width=target_size[0],
                height=target_size[1],
                clip_range=TimeRangeModel(startTime=start_time, endTime=end_time)
            ))
            if is_muted:
                project_model_builder.modify_asset_model(sub_video_asset_id, writer=set_asset_muted)
            project_model_builder.set_transform_of_asset(sub_video_asset_id, AssetTransform(
                positionX=position_x,
                positionY=position_y,
                scaleX=width / resolution[0] * 100,
                scaleY=height / resolution[1] * 100
            ))
            sub_video_asset_ids.append(sub_video_asset_id)

        if sub_video_asset_id_column in timeline_table.columns:
            video_asset_id_list = timeline_table.at[0, sub_video_asset_id_column]
            video_asset_id_list.extend(sub_video_asset_ids)
        else:
            timeline_table[sub_video_asset_id_column] = None
            timeline_table.at[0, sub_video_asset_id_column] = sub_video_asset_ids
        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(AddSubVideoTrackOp) \
    .add_input(name="timeline_table", type="DataTable", desc="时间线表") \
    .add_input(name="material_table", type="DataTable", desc="素材表") \
    .add_output(name="timeline_table", type="DataTable", desc="时间线表") \
    .add_attr(name="project_model_builder_column", type="str", desc="项目模型构建器列名") \
    .add_attr(name="input_file_map_column", type="str", desc="输入文件映射列名") \
    .add_attr(name="video_index", type="int", desc="视频索引") \
    .add_attr(name="video_blob_key_column", type="str", desc="视频blob key列名") \
    .add_attr(name="start_time_column", type="str", desc="开始时间列名") \
    .add_attr(name="end_time_column", type="str", desc="结束时间列名") \
    .add_attr(name="width_column", type="str", desc="宽度列名") \
    .add_attr(name="height_column", type="str", desc="高度列名") \
    .add_attr(name="resolution_column", type="str", desc="分辨率列名") \
    .add_attr(name="is_muted", type="bool", desc="是否静音") \
    .add_attr(name="target_video_bbox", type="list", desc="目标视频bbox") \
    .add_attr(name="sub_video_asset_id_column", type="str", desc="子视频资产id列名")
