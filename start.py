# start.py
import os
import os.path
import sys

# Renderが割り当てるPORT。ローカル実行時は 10000 にフォールバック
port = os.environ.get("PORT", "10000")

# 実行するコマンド: 正しく Streamlit サーバーとして app.py を起動する
cmd = [
    "streamlit",
    "run",
    "app.py",
    "--server.port",
    port,
    "--server.address",
    "0.0.0.0",
]

# 今のプロセスを streamlit に置き換える
os.execvp(cmd[0], cmd)
