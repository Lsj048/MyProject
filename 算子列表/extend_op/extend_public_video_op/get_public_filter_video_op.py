import time

from video_graph.common.client.client_manager import ClientManager
from video_graph.common.utils.redis import RedisManager
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class GetPublicFilterVideoOp(Op):
    """
    Function:
        获取公域视频算子，请求服务获取公域视频，输出公域视频列表

    Attributes:
        first_industry_name_column (str): 一级行业名称列名，默认为"first_industry_name"。
        second_industry_name_column (str): 二级行业名称列名，默认为"second_industry_name"。
        product_name_column (str): 产品名称列名，默认为"product_name"。
        account_id (int): 账户ID列名，默认为"account_id"。
        caption_column (str): 字幕列名，默认为"caption"。
        public_material_column (str): 公域素材列名，默认为"public_material"。
        req_num (int): 请求个数，默认为30。
        duration_upper (int): 公域视频的时长上限，默认为10s。
        duration_lower (int): 公域视频的时长下限，默认为0s。
        ratio (float): 公域视频的分辨率比例，默认为0.5

    InputTables:
        shot_table: 镜号表格。
        request_table: 请求表格。

    OutputTables:
        shot_table: 添加了公域素材的表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/extend_op/public_video_op/get_public_filter_video_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        request_table: DataTable = op_context.input_tables[1]
        first_industry_name_column = self.attrs.get("first_industry_name_column", "first_industry_name")
        second_industry_name_column = self.attrs.get("second_industry_name_column", "second_industry_name")
        product_name_column = self.attrs.get("product_name_column", "product_name")
        account_id_column = self.attrs.get("account_id_column", "account_id")
        caption_column = self.attrs.get("caption_column", "caption")
        public_material_column = self.attrs.get("public_material_column", "public_material")
        req_num = self.attrs.get("req_num", 30)
        duration_upper = self.attrs.get("duration_upper", 0)
        duration_lower = self.attrs.get("duration_lower", 1)
        ratio = self.attrs.get("ratio", 0.5625)

        first_industry_name = request_table.at[0, first_industry_name_column]
        second_industry_name = request_table.at[0, second_industry_name_column]
        product_name = request_table.at[0, product_name_column]
        account_id = request_table.at[0, account_id_column]

        text_to_video_client = ClientManager().get_client_by_name("TextToVideoClient")
        shot_table[public_material_column] = None
        redis = RedisManager().get_client('creativeCenterCache')

        for index, row in shot_table.iterrows():
            caption = row.get(caption_column)
            if caption is None:
                continue

            if isinstance(req_num, str) and req_num in shot_table.columns:
                req_num = row.get(req_num)
            elif isinstance(req_num, str):
                req_num = 30

            resp = text_to_video_client.sync_req(caption, product_name, first_industry_name, second_industry_name,
                                                 topk=req_num, duration_upper=duration_upper,
                                                 duration_lower=duration_lower, ratio=ratio)
            if resp is None:
                continue

            valid_resp = []
            resp_items = [_item for _item in resp if _item]
            for resp_item in resp_items:
                if len(resp_item) != 3:
                    continue
                resource_id = resp_item[-1]

                mat_timestamps = redis.get(resource_id)
                mat_acc_timestamps = redis.get(f"{resource_id}_{account_id}")
                current_timestamp = int(time.time())

                # 同 1 个 clip，被同 1 个账户使用，最多 X 次/天，最多 Y 次/7天
                if mat_acc_timestamps:
                    mat_acc_timestamps = mat_acc_timestamps.decode('utf-8', errors='ignore').split(',')
                    one_day_used_cnt = 0
                    one_week_used_cnt = 0
                    for material_time_stamp in mat_acc_timestamps:
                        material_time_stamp = int(material_time_stamp)
                        time_difference = current_timestamp - material_time_stamp
                        days_difference = time_difference / (24 * 60 * 60)
                        if days_difference <= 1:
                            one_day_used_cnt += 1
                        if days_difference <= 7:
                            one_week_used_cnt += 1
                    if one_day_used_cnt > 10 or one_week_used_cnt > 50:
                        continue

                # 同 1 个 clip，被使用，最多 Z 次/天, 最多 A 次/7天
                if mat_timestamps:
                    one_day_used_mat_cnt = 0
                    one_week_used_mat_cnt = 0
                    mat_timestamps = mat_timestamps.decode('utf-8', errors='ignore').split(',')
                    for mat_timestamp in mat_timestamps:
                        mat_timestamp = int(mat_timestamp)
                        time_difference = current_timestamp - mat_timestamp
                        days_difference = time_difference / (24 * 60 * 60)
                        if days_difference <= 1:
                            one_day_used_mat_cnt += 1
                        if days_difference <= 7:
                            one_week_used_mat_cnt += 1
                    if one_day_used_mat_cnt > 50 or one_week_used_mat_cnt > 250:
                        continue
                valid_resp.append(resp_item)

            shot_table.at[index, public_material_column] = valid_resp

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(GetPublicFilterVideoOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_input(name="request_table", type="DataTable", desc="请求表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表")
