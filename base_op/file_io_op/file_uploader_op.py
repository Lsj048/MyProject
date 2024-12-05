import os

from video_graph.common.utils.blobstore import BlobStoreClientManager
from video_graph.common.utils.tools import build_bbs_resource_id
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class FileUploaderOp(Op):
    """
    【local】文件上传算子，用于上传本地文件到 Blob 存储

    Attributes:
        file_blob_key_column (str): 输入表中表示文件 Blob Key 的列名。
        file_path_column (str): 输入表中表示文件路径的列名。
        blob_db (str): Blob 存储的数据库名称，默认为 "ad"。
        blob_table (str): Blob 存储的表名称，默认为 "nieuwland-material"。
        blob_key_prefix (str): Blob Key 的前缀，默认为空字符串。

    InputTables:
        in_table: 输入表格。

    OutputTables:
        in_table: 添加了文件 Blob Key 的输入表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/file_io_op/file_uploader_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.file_io_op.file_uploader_op import *

        # 创建输入表，可以为单列也可以为多列，具体可参考TempFileRemove算子的例子
        input_table = DataTable(
            name="testTable",
            data={
            'file_need_upload_path': ['test_op/test_audio.mp3']
        })

        # 创建 OpContext 并添加输入表
        op_context = OpContext("graph_name","request_tag","request_id")
        op_context.input_tables.append(input_table)

        # 配置 FileUploaderOp 的属性，对于blob_db blob_table blob_key_prefix不添加就上传到默认的库中,若添加则必须要设定为提前创建完成的blob_db和blob_table
        #算子通过self.attrs.get得到输入表中对应处理的列名，所以attrs中的命名方式需要与输入表data中的相统一
        file_uploader_op = FileUploaderOp(
            name = "FileUploaderOp",
            attrs = {
            "file_path_column": "file_need_upload_path",
        #    "blob_db": "test_db",
        #    "blob_table": "test_video",
        #    "blob_key_prefix": "test_prefix_"
        })

        # 运行 FileUploaderOp
        success = file_uploader_op.process(op_context)

        # 检查结果
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子计算失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        file_blob_key_column = self.attrs.get("file_blob_key_column", "file_blob_key")
        file_path_column = self.attrs.get("file_path_column", "file_path")
        blob_db = self.attrs.get("blob_db", "ad")
        blob_table = self.attrs.get("blob_table", "nieuwland-material")
        blob_key_prefix = self.attrs.get("blob_key_prefix", "")

        in_table[file_blob_key_column] = None
        for index, row in in_table.iterrows():
            file_path = row.get(file_path_column)
            if file_path is None or not os.path.exists(file_path):
                continue

            blob_key = f"{blob_key_prefix}{os.path.basename(file_path)}"
            blob_client = BlobStoreClientManager().get_client(f"{blob_db}-{blob_table}")
            blob_client.upload_file_with_retry(file_path, blob_key)

            in_table.loc[index, file_blob_key_column] = build_bbs_resource_id((blob_db, blob_table, blob_key))

        op_context.output_tables.append(in_table)
        return True


op_register.register_op(FileUploaderOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="file_blob_key_column", type="str", desc="blobstore地址列名") \
    .add_attr(name="file_path_column", type="str", desc="文件地址列名") \
    .add_attr(name="blob_db", type="str", desc="blob db") \
    .add_attr(name="blob_table", type="str", desc="blob table") \
    .add_attr(name="blob_key_prefix", type="str", desc="blob key前缀") \
    .set_parallel(True)
