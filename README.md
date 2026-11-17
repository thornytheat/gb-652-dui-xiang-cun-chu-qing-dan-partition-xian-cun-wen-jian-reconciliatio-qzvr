# 对象存储清单与分区现存文件对账

        面向一线复核人员的单服务数据工作台。样例数据随仓库提供，结论保留行号、原因码和原值。

        ## 业务模块

        - M1 清单采信：`/api/manifests/import`
- M2 路径契约：`/api/paths/normalize`
- M3 对象核验：`/api/objects/verify`
- M4 分区判定：`/api/partitions`
- M5 差异对账：`/api/reconcile`
- M6 批次导出：`/api/export/differences.csv`
- M7 归档恢复对账：`/api/restore/reconcile`（`GET` 只读回读，`POST` 提交或续接）

        ## 核心规则

        路径规范化后按 key、size 与摘要逐项比对，目录标记、空分区和缺失对象不得混为一类。

        ## 归档恢复对账

        归档对象需恢复后核验。M7 读取实际清单与对象登记，以本地存储服务替身复现受理、查询与回执丢失：

        - 恢复申请按 `(key, version_id)` 幂等，重试复用原 `request_id`，不重复计费。
        - 受理后中断可续接：重跑先查询存储侧已有受理记录，不重复申请。
        - 到期仍无法确认转“待裁决”，既不冒充缺失也不冒充核验成功。
        - 迟到回执按 `request_id` 归回原申请，不按当前最新版本改挂。
        - 旧对象版本的恢复结果不覆盖新版本；恢复完成后再比大小与摘要。
        - 接口回读申请、费用次数与分区核验结果；不改路径规则，不做补数领取。

        样例：`sample_data/restore_manifest.csv` 与 `sample_data/restore_objects.jsonl`。

        ## 启动

        ```bash
        docker compose up --build
        ```

        浏览器访问 `http://localhost:18211`。运行测试：

        ```bash
        docker compose exec -T app pytest -q
        ```

        ## 样例流程

        依次运行六个模块；最后在批次页复核输入、有效、隔离与输出合计。同一输入摘要重复执行时复用原 `run_id`。

        ## 已知不覆盖

        不处理加密文件与需要外部密钥的对象；不连接真实生产存储。
