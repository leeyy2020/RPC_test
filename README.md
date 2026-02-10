# Sui Checkpoint Subscriber

基于 gRPC 订阅 Sui checkpoint，解码事件并提取指定池子的 `sqrt_price` 变化。

## 功能

- 实时订阅 checkpoint（`SubscribeCheckpoints`）
- 尝试对事件 BCS 做结构化解码
- 过滤目标 `TARGET_OBJECT_ID` 的 `before_sqrt_price / after_sqrt_price`
- 每个 checkpoint 落盘到 `checkpoints_json/`
- 控制台输出解码统计和 `sqrt` 变化日志

## 环境要求

- Node.js 18+
- npm

## 安装

```bash
npm install
```

## 配置

在项目根目录创建或修改 `.env`：

```env
FULLNODE_URL=http://127.0.0.1:9000
JSONRPC_URL=http://127.0.0.1:9000
TARGET_OBJECT_ID=0x15dbcac854b1fc68fc9467dbd9ab34270447aabd8cc0e04a5864d95ccb86b74a
DECODE_CONCURRENCY=8
DEBUG_RAW=0
```

说明：

- `TARGET_OBJECT_ID` 必填
- `JSONRPC_URL` 不填时默认使用 `FULLNODE_URL`
- `DECODE_CONCURRENCY` 不填默认 `8`
- `DEBUG_RAW=1` 时会打印原始订阅消息

## 启动

```bash
npm start
```

## 输出

- 目录：`/root/rpc_test/checkpoints_json`
- 文件命名：`checkpoint_<sequence>.json`
- 控制台日志：
  - `[decode]`：每个 checkpoint 解码耗时和成功/失败数量
  - `[sqrt]`：命中目标池子的 sqrt 变化

## 主要文件

- `subscribe_checkpoints.ts`：唯一核心脚本
- `package.json`：运行脚本与依赖

