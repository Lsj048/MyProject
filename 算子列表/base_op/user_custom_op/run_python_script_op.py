from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class RunPythonScriptOp(Op):
    """
    Function:
        运行 python 脚本，脚本中可以处理 op_context 中的数据，实现自定义逻辑

    Attributes:
        import_script (str): 导入包脚本。
        script (str): 要运行的脚本。

    InputTables:
        无

    OutputTables:
        无

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/user_custom_op/run_python_script_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.user_custom_op.run_python_script_op import *

# 配置并实例化算子
run_python_script_op = RunPythonScriptOp(
    name="RunPythonScriptOp",
    attrs={
        "import_script": "import math\n",
        "script": "result = math.sqrt(16)\nprint(\"平方根结果为:\",result)"
    }
)

# 执行算子
success = run_python_script_op.compute(op_context)

# 检查输出表格
if success:
    print("执行成功!")
else:
    print("算子执行失败.")
    """

    def compute(self, op_context: OpContext) -> bool:
        import_script = self.attrs.get('import_script')
        script = self.attrs.get("script")
        #import_script = import_script.replace('\n', '').replace('\t', '')
        #script = script.replace('\n', '').replace('\t', '').replace(' ', '')
        exec(import_script + script)
        return True


op_register.register_op(RunPythonScriptOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_input(name="...", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="...", type="DataTable", desc="placeholder") \
    .add_attr(name="import_script", type="str", desc="导入包脚本") \
    .add_attr(name="script", type="str", desc="要运行的脚本")
