from ks_editor_kernel.model_builder.model_util import ModelConstants
from ks_editor_kernel.model_builder.video_project_model_builder import EditSceneVideoProjectBuilder, CompTextInfo
from ks_editor_kernel.pb.CommonDraftBaseAssetModel_pb2 import TimeRangeModel, Stroke, AssetTransform
from ks_editor_kernel.pb.CommonDraftTextAssetModel_pb2 import TextInfoModel, AutoWrap, TextResource

from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AddTitleTrackOP(Op):
    """
    Function:
        增加标题轨道算子，可以设置标题的字体、颜色、描边等属性以及标题的位置、对齐方式等

    Attributes:
        project_model_builder_column (str): 项目模型构建器列名，默认为"project_model_builder"。
        input_file_map_column (str): 输入文件映射列名，默认为"input_file_map"。
        title_column (str): 标题列名，默认为"title"。
        duration_column (str): 时长列名，默认为"tts_duration"。
        font_blob_key (str): 字体blob key，默认为"ad_nieuwland-material_文悦新青年体.otf"。
        text_template (str): 文本模板，默认为"ad_nieuwland-material_字幕样式_橙紫.zip"。
        font_index (str): 字体索引，默认为"-3"。
        font_map_column (str): 字体映射列名，默认为"font_map"。
        font_color (str): 字体颜色，默认为"#FFFFFF"。
        stroke_color (str): 描边颜色，默认为"#0F0F0F"。
        stroke_width (int): 描边宽度，默认为4。
        text_scale_ratio (float): 文本缩放比例，默认为1.5。
        position_x (int): x坐标，默认为50。
        position_y (int): y坐标，默认为15。
        auto_wrap (bool): 是否折行，默认为False
        auto_wrap_doc_width (float): 折行时文本占屏幕的最大宽度，默认为0.9
        align_type (str): 对齐方式，默认值为“center”

    InputTables:
        timeline_table: 时间线表格。
        shot_table: 镜头表格。

    OutputTables:
        timeline_table: 添加了标题轨的时间线表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_new_op/add_title_track_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute_when_skip(self, op_context: OpContext) -> bool:
        op_context.output_tables.append(op_context.input_tables[0])
        return True

    def compute(self, op_context: OpContext) -> bool:
        timeline_table: DataTable = op_context.input_tables[0]
        shot_table: DataTable = op_context.input_tables[1]
        project_model_builder_column = self.attrs.get("project_model_builder_column", "project_model_builder")
        input_file_map_column = self.attrs.get("input_file_map_column", "input_file_map")
        title_column = self.attrs.get("title_column", "title")
        duration_column = self.attrs.get("duration_column", "tts_duration")
        font = self.attrs.get("font", "ad_nieuwland-material_文悦新青年体.otf")
        text_template = self.attrs.get("text_template", "ad_nieuwland-material_字幕样式_橙紫.zip")
        font_index = self.attrs.get("font_index", "-3")
        font_map_column = self.attrs.get("font_map_column", "font_map")
        font_color = self.attrs.get("font_color", "#FFFFFF")
        stroke_color = self.attrs.get("stroke_color", "#0F0F0F")
        stroke_width = self.attrs.get("stroke_width", 4)
        text_scale_ratio = self.attrs.get("text_scale_ratio", 1.5)
        position_x = self.attrs.get("position_x", 50)
        position_y = self.attrs.get("position_y", 15)
        auto_wrap = self.attrs.get("auto_wrap", False)
        auto_wrap_doc_width = self.attrs.get("auto_wrap_doc_width", 0.9)
        align_type = self.attrs.get("align_type", "center")

        align_type_map = {"left": 0, "center": 1, "right": 2, "vertical": 3}
        project_model_builder: EditSceneVideoProjectBuilder = timeline_table.at[0, project_model_builder_column]
        input_file_map: dict = timeline_table.at[0, input_file_map_column]
        font_map = {font_index: font}
        if font_map_column not in timeline_table.columns:
            timeline_table[font_map_column] = None
            timeline_table.at[0, font_map_column] = font_map
        else:
            origin_font_map = timeline_table.at[0, font_map_column]
            origin_font_map.update(font_map)  # 表里的数据会被修改
        input_file_map.update({
            font: font,
            text_template: text_template
        })

        cur_duration = 0
        for index, row in shot_table.iterrows():
            title = row.get(title_column)
            duration = row.get(duration_column)
            text_stroke = Stroke(
                color=int(stroke_color[1:], 16),
                width=stroke_width
            )
            text_model = TextInfoModel(
                text=title,
                fontId=font_index,
                textColor=int(font_color[1:], 16),
                alignType=align_type_map.get(align_type, 1),
                textColorAlpha=100,
                stroke=[text_stroke],
                autoWrap=AutoWrap(hadAdjustMaxWidth=True,
                                  currentScale=ModelConstants.SUBTITLE_DEFAULT_SCALE * text_scale_ratio,
                                  docWidth=auto_wrap_doc_width) if auto_wrap else None
            )
            comp_text_info = CompTextInfo(text_model=text_model,
                                          real_range=TimeRangeModel(startTime=cur_duration,
                                                                    endTime=cur_duration + duration),
                                          text_resource=TextResource(resId=1, resType=2, resPath=text_template))
            text_asset_id = project_model_builder.add_comp_text_asset(comp_text_info)
            project_model_builder.set_transform_of_asset(text_asset_id, AssetTransform(
                positionX=position_x,
                positionY=position_y,
                scaleX=ModelConstants.SUBTITLE_DEFAULT_SCALE * 100 * text_scale_ratio,
                scaleY=ModelConstants.SUBTITLE_DEFAULT_SCALE * 100 * text_scale_ratio
            ))
            project_model_builder.set_display_range_of_asset(text_asset_id, TimeRangeModel(
                startTime=cur_duration,
                endTime=cur_duration + duration
            ))
            cur_duration += duration

        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(AddTitleTrackOP) \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="timeline_table", type="DataTable", desc="视频渲染表")
