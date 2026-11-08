# qq-webhook-bridge

QQ 官方 webhook 接收端与常驻 VM 的 Redis/TLS 消费组件。

```text
QQ → Vercel 验签、加密、原子入队 → Upstash Redis
                                      ↓ Redis/TLS
                                常驻 VM → 本地 inbox → 原 worker
```

Vercel 收到事件并入队后立即返回协议 ACK。空闲等待留在 VM，避免全天占用
Fluid 内存。`api/qq-events?action=token` 保留为有缓存、有超时的短时 token 代理。
`api/qq-wait` 与旧 drain/delete 调用返回 HTTP 410。

## 部署

1. 在 Vercel 保留 `QQ_CLIENT_SECRET`、`POLL_SECRET`、
   `UPSTASH_REDIS_REST_URL` 与 `UPSTASH_REDIS_REST_TOKEN`。
2. 给 VM 私有配置目录交付同一数据库的原生 Redis host、port、password。
   使用接收方公钥加密附件，凭据文件权限 600，目录 700。
3. 按 [VM 接线协议](docs/VM_TRANSPORT.md) 安装与原桥接程序兼容的消费组件。
   原 inbox hook、SDK、outbox、语音与表情逻辑继续由宿主维护。
4. 先确认 VM 已通过实际 Redis/TLS 连接并成为唯一消费者，再发布本仓库到
   Vercel production。Git 推送后核实生产部署对应的 commit 与 READY 状态。
5. 对照旧进程和任务清单，结束 HTTP 长轮询，断开废弃 Blob 项目连接。
   Vercel 历史用量会按平台窗口自然退出统计。

原生阻塞命令使用 Redis/TLS。Upstash 的 HTTP REST 接口兼容范围不同，不能
将原生 `BLMOVE` 等同于 REST 调用。

## 消息恢复

接收端仍写原格式 AES-256-GCM 密文。VM 使用 `BLMOVE` 按 FIFO 顺序将一条
消息移动到 `qq:processing`，完成原 SDK 归一化和本地持久化后才确认删除。
连接中断或进程重启先恢复 processing。确定性 inbox 文件名和宿主已有去重
防止同一次交接生成多个工作文件。损坏密文原样移入 `qq:invalid`，供管理员
修复。该表只保存加密原文。

```sh
npm ci --ignore-scripts
npm test
```

测试使用本地模拟队列与临时目录，生产消息按正常入口处理。
