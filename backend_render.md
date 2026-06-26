# Vento Timing Backend — Render 部署技术路线

> 最后更新: 2026-06-26 (v2 — add HTTP jsonStream + frontend deploy)

---

## 目录

1. [架构总览](#1-架构总览)
2. [环境变量清单](#2-环境变量清单)
3. [Render 配置 (render.yaml)](#3-render-配置-renderyaml)
4. [数据库部署](#4-数据库部署)
5. [F1 Token 管理](#5-f1-token-管理)
6. [直播/回放模式切换](#6-直播回放模式切换)
7. [Key 文件与职责](#7-关键文件与职责)
8. [本地开发与生产差异](#8-本地开发与生产差异)
9. [已知限制](#9-已知限制)
10. [故障排查指南](#10-故障排查指南)
11. [HTTP jsonStream 数据采集](#11-http-jsonstream-数据采集)
12. [前端部署 (Vercel)](#12-前端部署-vercel)
13. [生产 URL](#13-生产-url)

---

## 1. 架构总览

```
用户浏览器 ──→ 门户 iframe ──→ Vento 前端 (Vercel)
                                    │
                          REST API / WebSocket
                                    │
                              Vento 后端 (Render)
                                    │
                    ┌───────────────┼───────────────┐
                    │               │               │
               F1 SignalR       HTTP jsonStream   tyre_raw.db
              (实时推送)        (轮询拉取)        (轮胎数据库)
              TimingData        CarData.z          4 sessions
              DriverList        Position.z         28 drivers
              SessionInfo            │
              TrackStatus            │
                    │               │
                    └───────┬───────┘
                            │
                    DataProcessor → WebSocket → 前端------|----------|----------|
| 前端 | Vercel | `frontend/` (独立仓库) |
| 后端 | Render | `backend/` (独立仓库) |
| 数据库 | 嵌入仓库 | `backend/data/tyre_raw.db` |

---

## 2. 环境变量清单

### 2.1 Render Dashboard 必须设置

| 变量名 | 示例值 | 说明 |
|--------|--------|------|
| `F1_TOKEN` | `eyJra...` | F1 直播鉴权 JWT token |
| `F1_EMAIL` | `xxx@gmail.com` | F1 TV 账号邮箱（备用） |
| `F1_PASSWORD` | `******` | F1 TV 账号密码（备用） |
| `TYRE_DB_PATH` | `data/tyre_raw.db` | 轮胎数据库路径 |

### 2.2 render.yaml 声明的变量

```yaml
envVars:
  - key: TYRE_DB_PATH
    value: data/tyre_raw.db
  - key: PYTHON_VERSION
    value: "3.11"
  - key: F1_TOKEN
    sync: false          # 从 Dashboard 手动设置
  - key: F1_EMAIL
    sync: false
  - key: F1_PASSWORD
    sync: false
```

> **注意**: `TYRE_DB_PATH` 的路径 `data/tyre_raw.db` 是相对于仓库根目录（即 `backend/`）的，Render 的工作目录默认为仓库根目录。

---

## 3. Render 配置 (render.yaml)

```yaml
services:
  - type: web
    name: vento-timing-backend
    env: python
    buildCommand: pip install -r requirements.txt
    startCommand: uvicorn app.main:app --host 0.0.0.0 --port $PORT
    envVars:
      - key: TYRE_DB_PATH
        value: data/tyre_raw.db
      - key: PYTHON_VERSION
        value: "3.11"
      - key: F1_TOKEN
        sync: false
      - key: F1_EMAIL
        sync: false
      - key: F1_PASSWORD
        sync: false
```

### 3.1 Build 注意点

- 构建命令仅 `pip install -r requirements.txt`
- Playwright / Chromium **未安装**（无法在 Render 免费计划上运行浏览器自动化）
- `httpx`、`numpy`、`cryptography`、`pywin32` 等依赖在 `requirements.txt` 中

### 3.2 关于磁盘

- Render 免费计划无法挂载持久磁盘
- 所有数据（数据库、回放数据）直接嵌入 Git 仓库

---

## 4. 数据库部署

### 4.1 轮胎数据库 (tyre_raw.db)

| 属性 | 值 |
|------|-----|
| 文件路径 | `backend/data/tyre_raw.db` |
| 大小 | ~592 KB |
| 数据来源 | OpenF1 API / F1 直播录制 |
| 内容 | 4 个 session（Practice 1-3, Qualifying），28 位车手 |
| Git 策略 | 通过 `!data/tyre_raw.db` 排除规则加入仓库 |

### 4.2 数据库读取模块

两个模块都通过 `TYRE_DB_PATH` 环境变量定位数据库：

| 模块 | 用途 | 默认路径（env 未设置时） |
|------|------|------------------------|
| `tyre_raw_db.py` | 写入/读取原始圈速数据 | `../tyre_raw.db`（项目根目录） |
| `tyre_analysis.py` | 退化分析 API | `data/tyre_raw.db`（相对于仓库根） |

```python
# tyre_analysis.py 的路径解析逻辑
_DB_PATH = (
    os.environ.get("TYRE_DB_PATH")                    # ① 环境变量优先
    or os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "tyre_raw.db")
)
                                                      # ② 回退到仓库内路径
```

### 4.3 API 端点

| 端点 | 说明 |
|------|------|
| `GET /api/tyre/drivers` | 车手列表 + 可用 session |
| `GET /api/tyre/degradation?driver=N` | 单车手退化分析 |
| `GET /api/tyre/compare?drivers=1,44` | 多车手对比 |
| `GET /api/tyre/grid-degradation?compound=MEDIUM` | 全部车手聚合退化 |
| `GET /api/tyre/qualy-best?driver=N&compound=MEDIUM` | 排位最快圈 |

---

## 5. F1 Token 管理

### 5.1 Token 获取流程

```
auto_login.py (本地 Playwright 浏览器)
    │ 自动打开浏览器 → 填写邮箱密码
    │ 拦截 by-password API 响应
    ▼
提取 subscriptionToken
    │
    ▼
保存到 backend/.env
    │
    ▼
复制到 Render Dashboard → F1_TOKEN
```

### 5.2 Token 有效期

- 当前 token 类型: `F1 TV Access Annual`
- 典型有效期: ~90 天（部分 token 3 天）
- 过期后后端自动检测（`auth.py`），但无法在 Render 上自动刷新
  - CloudFront WAF 阻止了纯 HTTP 登录
  - Playwright 无法在 Render 上运行
- 过期后需要在本地运行 `auto_login.py` 重新获取

### 5.3 Token 验证模块

`backend/app/auth.py` 提供 `ensure_valid_token()`：

1. 检查 `F1_TOKEN` 环境变量
2. 解码 JWT 验证过期时间
3. 如果过期且有 `F1_EMAIL`/`F1_PASSWORD`，尝试自动登录
4. 自动登录会在 Render 上失败（CloudFront 403），回退到日志警告

---

## 6. 直播/回放模式切换

### 6.1 API 端点

| 端点 | 效果 |
|------|------|
| `GET /api/mode` | 查看当前模式（live/replay） |
| `GET /api/replay/start?speed=20` | 切换到回放模式（20 倍速） |
| `GET /api/replay/stop` | 切换回 F1 直播模式 |

### 6.2 模式工作原理

```
f1_connection_loop()
    │
    ├─ replay_mode = env("REPLAY_MODE") or _replay_requested
    │
    ├─ True  → ReplayClient (读取回放 JSON → 模拟广播)
    │
    └─ False → F1SignalRClient (连接 F1 SignalR → 实时广播)
```

### 6.3 回放数据

- 数据来源: `archive/design_prototypes/data/`（OpenF1 API 加拿大站数据）
- 嵌入路径: `backend/app/replay_data/`（截取前 3000 条记录，~1.19 MB）
- 路径解析: `replay_feed.py` 自动检测路径（环境变量 → 归档路径 → 嵌入路径）
- 合成遥测: 回放数据中的速度/RPM/油门等为 `_gen_speed_profile()` 合成的近似值

---

## 7. 关键文件与职责

### 7.1 后端核心

| 文件 | 职责 |
|------|------|
| `app/main.py` | FastAPI 应用入口、路由、WebSocket、广播循环 |
| `app/auth.py` | Token 验证、自动登录（httpx 备用） |
| `app/data_processor.py` | 数据处理（Facade 安全网） |
| `app/tyre_analysis.py` | 轮胎退化分析 API |
| `app/tyre_raw_db.py` | 轮胎原始数据库读写 |
| `app/transports/signalr_feed.py` | F1 SignalR 直播客户端 |
| `app/transports/replay_feed.py` | 回放数据客户端 |

### 7.2 部署配置

| 文件 | 职责 |
|------|------|
| `render.yaml` | Render 服务定义、环境变量 |
| `requirements.txt` | Python 依赖清单 |
| `.gitignore` | Git 排除规则（含 `!data/tyre_raw.db`） |
| `data/tyre_raw.db` | 嵌入仓库的轮胎数据库 |
| `app/replay_data/` | 嵌入仓库的回放数据 |

---

## 8. 本地开发与生产差异

| 项目 | 本地开发 | 生产 (Render) |
|------|----------|---------------|
| 启动方式 | `python run.py` 或 `uvicorn app.main:app` | `uvicorn app.main:app`（render.yaml） |
| API 端口 | 8000 | `$PORT`（Render 自动注入） |
| 数据库路径 | `D:/Vento_Timing/tyre_raw.db`（项目根） | `data/tyre_raw.db`（仓库相对路径） |
| F1 Token | `backend/.env` 文件 | Render Dashboard 环境变量 |
| WebSocket | `ws://localhost:8000/ws` | `wss://vento-timing-backend.onrender.com/ws` |
| 前端 API | `localhost:5173` → Vite proxy → 8000 | Vercel → Render（跨域） |
| 回放数据 | `archive/design_prototypes/data/` | `app/replay_data/`（嵌入路径） |

---

## 9. 已知限制

### 9.1 功能限制

| 限制 | 原因 | 影响 |
|------|------|------|
| 无法自动刷新 F1 Token | CloudFront WAF 阻止纯 HTTP 请求；Playwright 无法在 Render 运行 | token 过期后需手动更新 |
| 无法从 Chrome 提取 cookie | Chrome 127+ App-Bound 加密 + 单实例锁 | 无法自动获取已登录的 token |
| 轮胎数据库无正赛数据 | 数据来源为练习赛/排位赛 | 退化分析不包含 Race stint |
| 回放遥测为合成值 | OpenF1 API 只有速度点数据 | 速度/RPM/油门为推算值 |

### 9.2 部署限制

| 限制 | 说明 |
|------|------|
| Render 免费计划无持久磁盘 | 所有数据嵌入 Git 仓库 |
| 大文件 Git 管理 | `tyre_raw.db` ~592 KB 可接受；回放数据已截取到 ~1.19 MB |
| 构建时间 | `pip install` 依赖较多，首次构建约 2-3 分钟 |

---

## 10. 故障排查指南

### 10.1 后端无法启动

```bash
# 检查 Render 部署日志
# 常见原因：
#   1. requirements.txt 缺少依赖
#   2. Python 语法错误（如多余的括号）
#   3. 路径错误（Windows 风格路径在 Linux 上不存在）
```

### 10.2 F1 连接失败

```
/api/status → connected: false
    │
    ├─ Token 过期 → 在本地运行 auto_login.py 获取新 token
    │                 → 更新 Render Dashboard 的 F1_TOKEN
    │
    └─ Token 有效 → 检查 F1 服务本身是否可用
```

### 10.3 轮胎 API 报错

```
/api/tyre/drivers → {"error":"unable to open database file"}
    │
    ├─ TYRE_DB_PATH 环境变量未设置或指向错误路径
    │   → 在 Render Dashboard 设置 TYRE_DB_PATH=data/tyre_raw.db
    │
    └─ data/tyre_raw.db 文件不存在
        → 确认文件在 Git 仓库中且已部署
```

### 10.4 前端看不到数据

```
前端加载但无数据
    │
    ├─ 直播模式 + 无比赛 → 正常，等待 F1 session
    │   → 可切换回放模式演示
    │
    ├─ 回放模式无数据 → 检查 /api/mode 确认模式
    │   → 调用 /api/replay/start?speed=20
    │
    └─ 轮胎策略无数据 → 检查 /api/tyre/drivers
        → 确认后端环境变量和数据库路径正确
```

### 10.5 模式切换不生效

```
/api/replay/stop → 返回 404
    │
    └─ 部署的代码不包含 API 端点
        → 检查 Render 部署日志，确认最新提交已成功部署
```

---



## 11. HTTP jsonStream 数据采集

### 11.1 背景

F1 SignalR 直播推送中，`CarData.z` 和 `Position.z` 在某些 session（如 Practice 1）不会被推送。
F1 的 live timing 静态服务器提供 HTTP `.jsonStream` 文件，包含这两个 topic 的数据。

### 11.2 JsonStreamFeed 模块

`app/transports/json_stream_feed.py` 实现了基于 HTTP byte-range 的流式拉取：

| 步骤 | 说明 |
|------|------|
| 1. 获取 session info | `GET /v2/event-tracking/meeting` → 提取 session_key |
| 2. 构建 jsonStream URL | `/static/.../2026-06-26_Practice_1/CarData.z.jsonStream` |
| 3. HTTP GET + Range | `Range: bytes={offset}-` 断点续传 |
| 4. 逐行解析 | 每行 JSON → 提取 `Lines` 字段（base64+zlib 压缩） |
| 5. 解码 → `on_message` | 解压 → JSON → `handle_f1_message` → DataProcessor |

### 11.3 启动流程

```python
# main.py lifespan 中
_json_feed = JsonStreamFeed(token=token, on_message=handle_f1_message)
json_feed_task = asyncio.create_task(_json_feed.start())

# 停止时
json_feed_task.cancel()
await _json_feed.stop()
```

### 11.4 数据格式

`CarData.z.jsonStream` 每行：
```json
{"Utc": "2026-06-26T13:57:00.000Z", "Lines": "<base64+zlib>"}
```
```python
# Lines 字段解码后
{"Entries": [{"Cars": {"1": {"Speed": 300, "RPM": 8000, ...}}}]}
```

### 11.5 常见问题

| 错误 | 原因 | 处理 |
|------|------|------|
| `416 Requested Range Not Satisfiable` | jsonStream 文件无新数据（赛车未在跑/数据未更新） | 正常，持续轮询即可 |
| `'str' object has no attribute 'get'` | 解码后的数据是字符串而非 dict | `_handle_CarData`/`_handle_Position` 内置 `json.loads()` 自动转换 |


## 12. 前端部署 (Vercel)

### 12.1 仓库

| 项目 | URL |
|------|-----|
| GitHub | `https://github.com/Vincent200211/vento-timing-frontend` |
| Vercel | `https://vento-timing-frontend.vercel.app` |

### 12.2 部署步骤

1. 推送到 GitHub 主分支 → Vercel 自动检测并部署
2. Framework Preset 自动识别为 Vite
3. Build Command: `npm run build`
4. Output Directory: `dist`

### 12.3 前端环境变量

在 Vercel Dashboard → Settings → Environment Variables：

| 变量 | 值 | 说明 |
|------|-----|------|
| `VITE_API_BASE` | 可选 | 后端 API 地址，默认自动检测 |

### 12.4 API 地址自动检测

`frontend/src/services/api.js` 和 `frontend/src/StrategyView.jsx` 会自动判断当前环境：

```javascript
const IS_PRODUCTION = typeof location !== "undefined"
    && location.hostname !== "localhost"
    && location.hostname !== "127.0.0.1";
const API_BASE = IS_PRODUCTION
    ? "https://vento-timing-backend.onrender.com/api"
    : "/api";
```

### 12.5 前端配置

| 文件 | 说明 |
|------|------|
| `vercel.json` | SPA 路由配置（纯 ASCII，无 BOM） |
| `.gitignore` | 排除 node_modules / dist / .env.local |
| `src/services/api.js` | REST / WebSocket 地址配置 |


## 13. 生产 URL

| 组件 | URL |
|------|-----|
| 后端状态 | `https://vento-timing-backend.onrender.com/api/status` |
| 后端模式 | `https://vento-timing-backend.onrender.com/api/mode` |
| 启动回放 | `https://vento-timing-backend.onrender.com/api/replay/start?speed=20` |
| 切回直播 | `https://vento-timing-backend.onrender.com/api/replay/stop` |
| 前端 | `https://vento-timing-frontend.vercel.app` |
| 轮胎 API | `https://vento-timing-backend.onrender.com/api/tyre/drivers` |

## 附录: 数据库字段说明

### tyre_raw.db 表结构

```sql
-- 会话表
CREATE TABLE sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_key INTEGER UNIQUE NOT NULL,
    meeting_name TEXT NOT NULL,
    session_type TEXT NOT NULL,     -- Practice / Qualifying
    session_name TEXT
);

-- 车手表  
CREATE TABLE drivers (
    driver_number INTEGER PRIMARY KEY,
    driver_name TEXT,
    team_name TEXT,
    team_colour TEXT
);

-- Stint 表
CREATE TABLE stints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    driver_number INTEGER NOT NULL,
    stint_number INTEGER,
    compound TEXT,                  -- SOFT / MEDIUM / HARD / INTERMEDIATE / WET
    lap_start INTEGER,
    lap_end INTEGER,
    tyre_age_at_start INTEGER,
    FOREIGN KEY (session_id) REFERENCES sessions(id)
);

-- 圈速表
CREATE TABLE laps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    driver_number INTEGER NOT NULL,
    stint_id INTEGER,
    lap_number INTEGER,
    position INTEGER,
    lap_time REAL,
    sector_1_time REAL,
    sector_2_time REAL,
    sector_3_time REAL,
    tyre_age INTEGER,
    track_status TEXT,
    is_outlap INTEGER DEFAULT 0,
    is_inlap INTEGER DEFAULT 0,
    FOREIGN KEY (session_id) REFERENCES sessions(id),
    FOREIGN KEY (stint_id) REFERENCES stints(id)
);
```

---

*文档维护: 请随代码变更同步更新此文档*
