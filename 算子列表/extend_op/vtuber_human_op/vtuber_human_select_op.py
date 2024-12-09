from video_graph.common.utils.tools import get_vtuber_human_id
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class VtuberHumanSelectOp(Op):
    """
    Function:
        虚拟人形象选择算子

    Attributes:
        first_industry_name_column (str): 一级行业名称列名，默认为"first_industry_name"。
        second_industry_name_column (str): 二级行业名称列名，默认为"second_industry_name"。
        product_name_column (str): 产品名称列名，默认为"product_name"。
        account_id_column (str): 账号 ID 列名，默认为"account_id"。
        vtuber_name_column (str): 虚拟人名称列名，默认为"vtuber_name"。
        reading_track_column (str): 阅读轨迹列名，默认为"reading_track"。
        vtuber_id_colomn (str): 虚拟人 ID 列名，默认为"vtuber_id"。
        vtuber_age_column (str): 虚拟人年龄列名，默认为"vtuber_age"。
        vtuber_gender_column (str): 虚拟人性别列名，默认为"vtuber_gender"。
        vtuber_extra_info_column (str): 虚拟人额外信息列名，默认为"vtuber_extra_info"。

    InputTables:
        request_table: 请求表格。
        shot_table: 视频列表表格。

    OutputTables:
        shot_table: 添加了虚拟人信息的表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/extend_op/vtuber_human_op/vtuber_human_select_op.py?ref_type=heads
    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        request_table: DataTable = op_context.input_tables[0]
        shot_table: DataTable = op_context.input_tables[1]
        first_industry_name_column = self.attrs.get("first_industry_name_column", "first_industry_name")
        second_industry_name_column = self.attrs.get("second_industry_name_column", "second_industry_name")
        product_name_column = self.attrs.get("product_name_column", "product_name")
        account_id_column = self.attrs.get("account_id_column", "account_id")
        vtuber_name_column = self.attrs.get("vtuber_name_column", "vtuber_name")
        reading_track_column = self.attrs.get("reading_track_column", "reading_track")
        vtuber_id_column = self.attrs.get("vtuber_id_colomn", "vtuber_id")
        vtuber_tts_column = self.attrs.get("vtuber_tts_column", "tts_style")
        vtuber_age_column = self.attrs.get("vtuber_age_column", "vtuber_age")
        vtuber_gender_column = self.attrs.get("vtuber_gender_column", "vtuber_gender")
        vtuber_extra_info_column = self.attrs.get("vtuber_extra_info_column", "vtuber_extra_info")

        first_industry_name = request_table.at[0, first_industry_name_column]
        second_industry_name = request_table.at[0, second_industry_name_column]
        product_name = request_table.at[0, product_name_column]
        account_id = request_table.at[0, account_id_column]
        used_vtuber_id_list = set()

        # 输入的数字人id和音色
        if vtuber_id_column in request_table:
            input_vtuber_id = request_table.at[0, vtuber_id_column]
        else:
            input_vtuber_id = None
        if vtuber_tts_column in request_table:
            input_vtuber_tts = request_table.at[0, vtuber_tts_column]
        else:
            input_vtuber_tts = None
        vtuber_name_to_vtuber_info = {}
        shot_table[reading_track_column] = None
        shot_table[vtuber_id_column] = None
        shot_table[vtuber_age_column] = None
        shot_table[vtuber_gender_column] = None
        shot_table[vtuber_extra_info_column] = None
        for index, row in shot_table.iterrows():
            # 一个脚本中，相同的角色用同一种虚拟人配置
            vtuber_name = row.get(vtuber_name_column)
            if vtuber_name in vtuber_name_to_vtuber_info:
                reading_track, vtuber_id, vtuber_age, vtuber_gender, vtuber_extra_info = vtuber_name_to_vtuber_info[
                    vtuber_name]
                shot_table.at[index, reading_track_column] = reading_track
                shot_table.at[index, vtuber_id_column] = vtuber_id
                shot_table.at[index, vtuber_age_column] = vtuber_age
                shot_table.at[index, vtuber_gender_column] = vtuber_gender
                shot_table.at[index, vtuber_extra_info_column] = vtuber_extra_info
                continue

            vtuber_id, reading_track, vtuber_age, vtuber_gender, vtuber_extra_info = get_vtuber_human_id(
                account_id, product_name, second_industry_name, first_industry_name, used_vtuber_id_list, vtuber_name,
                input_vtuber_id, input_vtuber_tts
            )

            used_vtuber_id_list.add(vtuber_id)
            vtuber_name_to_vtuber_info.update({
                vtuber_name: [reading_track, vtuber_id, vtuber_age, vtuber_gender, vtuber_extra_info]
            })
            shot_table.at[index, reading_track_column] = reading_track
            shot_table.at[index, vtuber_id_column] = vtuber_id
            shot_table.at[index, vtuber_age_column] = vtuber_age
            shot_table.at[index, vtuber_gender_column] = vtuber_gender
            shot_table.at[index, vtuber_extra_info_column] = vtuber_extra_info

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(VtuberHumanSelectOp) \
    .add_input(name="request_table", type="DataTable", desc="请求表") \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表")
