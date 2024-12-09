import time

from video_graph.common.utils.redis import RedisManager
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class RecordPublicVideoByAccountOp(Op):
    """
    Function:
        记录公域素材使用频次

    Attributes:
        account_id_column (int): 账户ID列名，默认为 "account_id"。
        redis_name (str): redis集群名，默认 "creativeCenterCache"。
        video_blob_key_column (str): 视频blob地址列名，默认 "video_blob_key"。
        start_time_column (str): 视频开始时间列名，默认 "start_time"。
        end_time_column (str): 视频开始时间列名，默认 "end_time"。

    InputTables:
        shot_table: 镜号表格
        request_table：请求表格

    OutputTables:
        shot_table: 添加了公域素材的表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/extend_op/public_video_op/record_public_video_by_account_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        request_table: DataTable = op_context.input_tables[1]
        account_id_column = self.attrs.get("account_id_column", "account_id")
        redis_name = self.attrs.get("redis_name", "creativeCenterCache")
        video_blob_key_column = self.attrs.get("video_blob_key", "video_blob_key")
        start_time_column = self.attrs.get("start_time_column", "start_time")
        end_time_column = self.attrs.get("end_time_column", "end_time")

        account_id = request_table.at[0, account_id_column]
        redis = RedisManager().get_client(redis_name)

        resource_id_list = []
        for index, row in shot_table.iterrows():
            start_time = shot_table.at[index, start_time_column]
            end_time = shot_table.at[index, end_time_column]
            if end_time > start_time:
                resource_id_list.append(shot_table.at[index, video_blob_key_column])

        for resource_id in resource_id_list:
            current_timestamp = str(int(time.time()))
            mat_timestamps = redis.get(resource_id)
            mat_acc_timestamps = redis.get(f"{resource_id}_{account_id}")

            if mat_timestamps:
                mat_timestamps = mat_timestamps.decode('utf-8', errors='ignore').split(',')
                mat_timestamps.append(current_timestamp)
            else:
                mat_timestamps = [current_timestamp]
            redis.set(f'{resource_id}', ",".join(mat_timestamps), ex=60 * 60 * 24 * 7)

            if mat_acc_timestamps:
                mat_acc_timestamps = mat_acc_timestamps.decode('utf-8', errors='ignore').split(',')
                mat_acc_timestamps.append(current_timestamp)
            else:
                mat_acc_timestamps = [current_timestamp]
            redis.set(f'{resource_id}_{account_id}', ",".join(mat_acc_timestamps),
                      ex=60 * 60 * 24 * 7)

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(RecordPublicVideoByAccountOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_input(name="request_table", type="DataTable", desc="请求表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表")
