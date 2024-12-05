from ks_editor_kernel.model_builder.model_util import ModelConstants
from ks_editor_kernel.model_builder.video_project_model_builder import EditSceneVideoProjectBuilder, CompTextInfo
from ks_editor_kernel.pb.CommonDraftBaseAssetModel_pb2 import TimeRangeModel, AssetTransform
from ks_editor_kernel.pb.CommonDraftTextAssetModel_pb2 import TextInfoModel, TextResource

from video_graph.common.utils.minecraft_tools import get_default_subtitle_font_style
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AddSubtitleTrackOp(Op):
    """
    【local】增加字幕轨算子, 可以设置字幕的字体、颜色、描边等属性
    TODO: 输入表顺序调整一下

    Attributes:
        project_model_builder_column (str): 项目模型构建器列名，默认为"project_model_builder"。
        input_file_map_column (str): 输入文件映射列名，默认为"input_file_map"。
        font_map_column (str): 字体映射列名，默认为"font_map"。
        subtitle_group_column (str): 字幕组列名，默认为"subtitle_group_with_keyword"。
        video_asset_id_column (str): 草稿中的视频资源id列名，默认为"video_asset_id"。
        binding_main_track (bool): 是否绑定到主视频轨道，默认为False。
        subtitle_center_pos_width (int): 字幕中心位置宽度，默认为50。
        subtitle_center_pos_height (int): 字幕中心位置高度，默认为76。
        normal_text_width (float): 普通字幕宽度，默认为6.8。
        normal_text_height (float): 普通字幕高度，默认为6.5。
        normal_font (str): 普通字幕字体，默认为"ad_nieuwland-material_文悦新青年体.otf"。
        normal_text_template (str): 普通字幕模板，默认为None。
        normal_font_index (str): 普通字幕字体索引，默认为"-1"。
        keyword_text_width (float): 关键词字幕宽度，默认为9.7。
        keyword_text_height (float): 关键词字幕高度，默认为9.3。
        keyword_text_scale_ratio (float): 关键词字幕缩放比例，默认为1.5。
        keyword_asset_ids_column (str): 关键词字幕asset id列名，默认为"keyword_asset_ids"。
        keyword_font (str): 关键词字幕字体，默认为"ad_nieuwland-material_文悦新青年体.otf"。
        keyword_text_template (str): 关键词字幕模板，默认为"ad_nieuwland-material_字幕样式_橙紫.zip"。
        keyword_font_index (str): 关键词字幕字体索引，默认为"-2"。

    InputTables:
        shot_table: 镜头表格。
        timeline_table: 时间线表格。

    OutputTables:
        timeline_table: 添加了字幕轨的时间线表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_new_op/add_subtitle_track_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """
    def compute_when_skip(self, op_context: OpContext):
        op_context.output_tables.append(op_context.input_tables[1])
        return True

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        timeline_table: DataTable = op_context.input_tables[1]
        project_model_builder_column = self.attrs.get("project_model_builder_column", "project_model_builder")
        input_file_map_column = self.attrs.get("input_file_map_column", "input_file_map")
        font_map_column = self.attrs.get("font_map_column", "font_map")
        subtitle_group_column = self.attrs.get("subtitle_group_column", "subtitle_group_with_keyword")
        tts_asset_id_column = self.attrs.get("tts_asset_id_column", "tts_asset_id")
        binding_main_track = self.attrs.get("binding_main_track", False)
        # 字幕位置
        subtitle_center_pos_width = self.attrs.get("subtitle_center_pos_width", 50)
        subtitle_center_pos_height = self.attrs.get("subtitle_center_pos_height", 76)
        # 默认字体&样式
        default_font_style = get_default_subtitle_font_style()
        # 普通字幕配置
        normal_text_width_ratio = self.attrs.get("normal_text_width", 6.8)
        normal_text_height_ratio = self.attrs.get("normal_text_height", 6.5)
        normal_font = self.attrs.get("normal_font", default_font_style[0])
        normal_text_template = self.attrs.get("normal_text_template", default_font_style[2])
        normal_font_index = self.attrs.get("normal_font_index", "-1")
        # normal_font_color = self.attrs.get("normal_font_color", "#FFFFFF")
        # normal_stroke_color = self.attrs.get("normal_stroke_color", "#FF606B")
        # normal_stroke_width = self.attrs.get("normal_stroke_width", 8)
        # 关键词字幕配置
        keyword_text_width_ratio = self.attrs.get("keyword_text_width", 9.7)
        keyword_text_height_ratio = self.attrs.get("keyword_text_height", 9.3)
        keyword_text_scale_ratio = self.attrs.get("keyword_text_scale_ratio", 1.5)
        keyword_asset_ids_column = self.attrs.get("keyword_asset_ids_column", "keyword_asset_ids")
        keyword_font = self.attrs.get("keyword_font", default_font_style[1])
        keyword_text_template = self.attrs.get("keyword_text_template", default_font_style[3])
        keyword_font_index = self.attrs.get("keyword_font_index", "-2")
        # keyword_font_color = self.attrs.get("keyword_font_color", "#FFFFFF")
        # keyword_stroke_color = self.attrs.get("keyword_stroke_color", "#FEB49F")
        # keyword_stroke_width = self.attrs.get("keyword_stroke_width", 1)

        project_model_builder: EditSceneVideoProjectBuilder = timeline_table.at[0, project_model_builder_column]
        input_file_map: dict = timeline_table.at[0, input_file_map_column]
        binding_tts_asset_id = int(timeline_table.at[0, tts_asset_id_column]) \
            if tts_asset_id_column in timeline_table.columns else 0
        subtitle_group = shot_table.loc[0, subtitle_group_column]
        font_map = {normal_font_index: normal_font, keyword_font_index: keyword_font}
        if font_map_column not in timeline_table.columns:
            timeline_table[font_map_column] = None
            timeline_table.at[0, font_map_column] = font_map
        else:
            origin_font_map = timeline_table.at[0, font_map_column]
            origin_font_map.update(font_map)  # 表里的数据会被修改
        input_file_map.update({
            normal_font: normal_font,
            normal_text_template: normal_text_template,
            keyword_font: keyword_font,
            keyword_text_template: keyword_text_template
        })

        keyword_asset_ids = []
        for start_time, end_time, items_list, keywords_list in subtitle_group:
            line_num = 0
            for one_line, keywords in zip(items_list, keywords_list):
                total_normal_text_num = sum([len(item) for item in one_line if item not in keywords])
                total_keyword_text_num = sum([len(item) for item in one_line if item in keywords])
                cur_start_pos = subtitle_center_pos_width - (total_normal_text_num / 2 * normal_text_width_ratio) - (
                        total_keyword_text_num / 2 * keyword_text_width_ratio)
                for one_item in one_line:
                    is_keyword = one_item in keywords
                    text_scale_ratio = keyword_text_scale_ratio if is_keyword else 1.0
                    text_width_ratio = keyword_text_width_ratio if is_keyword else normal_text_width_ratio
                    # font_color = keyword_font_color if is_keyword else normal_font_color
                    font_index = keyword_font_index if is_keyword else normal_font_index
                    # stroke_color = keyword_stroke_color if is_keyword else normal_stroke_color
                    # stroke_width = keyword_stroke_width if is_keyword else normal_stroke_width
                    text_template = keyword_text_template if is_keyword else normal_text_template
                    text_height_ratio = keyword_text_height_ratio if total_keyword_text_num > 0 \
                        else normal_text_height_ratio
                    text_len = len(one_item)
                    cur_end_pos = cur_start_pos + text_len * text_width_ratio
                    # text_stroke = Stroke(
                    #     color=int(stroke_color[1:], 16),
                    #     width=stroke_width
                    # )
                    text_model = TextInfoModel(
                        text=one_item,
                        fontId=font_index,
                        # textColor=int(font_color[1:], 16),
                        textColorAlpha=100,
                        # stroke=[text_stroke]
                    )
                    text_resource = None
                    if text_template:
                        text_resource = TextResource(resId=1, resType=2, resPath=text_template)
                    comp_text_info = CompTextInfo(text_model=text_model,
                                                  real_range=TimeRangeModel(startTime=start_time, endTime=end_time),
                                                  text_resource=text_resource)
                    text_asset_id = project_model_builder.add_comp_text_asset(comp_text_info)
                    project_model_builder.set_transform_of_asset(text_asset_id, AssetTransform(
                        positionX=(cur_start_pos + cur_end_pos) / 2,
                        positionY=subtitle_center_pos_height + line_num * text_height_ratio,
                        scaleX=ModelConstants.SUBTITLE_DEFAULT_SCALE * 100 * text_scale_ratio,
                        scaleY=ModelConstants.SUBTITLE_DEFAULT_SCALE * 100 * text_scale_ratio
                    ))
                    project_model_builder.set_display_range_of_asset(text_asset_id, TimeRangeModel(
                        startTime=start_time,
                        endTime=end_time
                    ))
                    cur_start_pos = cur_end_pos
                    if binding_tts_asset_id != 0 and binding_main_track:
                        project_model_builder.set_bind_track_id_of_asset(text_asset_id, binding_tts_asset_id)
                    if is_keyword:
                        keyword_asset_ids.append(str(text_asset_id))
                line_num += 1

        timeline_table[keyword_asset_ids_column] = None
        timeline_table.at[0, keyword_asset_ids_column] = keyword_asset_ids
        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(AddSubtitleTrackOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_output(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_attr(name="project_model_builder_column", type="str", desc="项目模型构建列名") \
    .add_attr(name="input_file_map_column", type="str", desc="输入文件映射列名") \
    .add_attr(name="subtitle_group_column", type="str", desc="字幕列表列名") \
    .add_attr(name="normal_text_width_ratio", type="float", desc="普通字幕宽度比例") \
    .add_attr(name="normal_text_height_ratio", type="float", desc="普通字幕高度比例") \
    .add_attr(name="keyword_text_width_ratio", type="float", desc="关键字字幕宽度比例") \
    .add_attr(name="subtitle_center_pos_width", type="int", desc="字幕中心位置-横向") \
    .add_attr(name="subtitle_center_pos_height", type="int", desc="字幕中心位置-纵向") \
    .add_attr(name="font", type="str", desc="字幕字体") \
    .add_attr(name="font_index", type="str", desc="字幕字体索引") \
    .add_attr(name="font_map_column", type="str", desc="字幕字体映射列名") \
    .add_attr(name="font_color", type="str", desc="字幕颜色") \
    .add_attr(name="stroke_color", type="str", desc="字幕描边颜色") \
    .add_attr(name="stroke_width", type="int", desc="字幕描边宽度") \
    .add_attr(name="keyword_text_scale_ratio", type="float", desc="关键字字幕缩放比例") \
    .add_attr(name="keyword_asset_ids_column", type="str", desc="关键字资源id列名")
