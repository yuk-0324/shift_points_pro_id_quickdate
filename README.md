# シフトポイント（社員ID・日付自動化・期間プリセット版）

- 入力は 社員ID固定（表示は「ID - 名前」）。重複防止: 日付×シフト×emp_id
- 日付は 既定で「今日」。ワンクリック「昨日で記録」ボタンあり。
- ダッシュボードは 期間プリセット（今日/今週/今月/先月/カスタム）。

## 実行
```
pip install -r requirements.txt
cp .env.sample .env
streamlit run app.py
```
