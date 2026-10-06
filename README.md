# meshy2api-gateway

一个**单账号**的 Meshy.ai 网关：把 Meshy 的对话 / 生图 / 3D / 骨骼动画能力封装成
OpenAI 风格的 HTTP API，并自带一个网页控制台。

> 这是 meshy2api 的**单账号精简版**：账号池调度、批量注册、积分任务、代理池、
> IP 风控都已剥离。认证使用一个 Meshy 账号（access/refresh token + 浏览器的
> `meshy_device_id`），存在独立的凭据库里，可用控制台一键导入或**浏览器登录**捕获。

---

## 视频教程

**白嫖 AI 生成 3D 动画！图片一键变会动的角色**
小红书：https://xhslink.cn/o/4PfsXStoBFV
（复制链接后打开【小红书】App 即可查看笔记）

---

## 自动注册 & 无限续杯

**本仓库的精简版不含自动注册和无限续杯**（这是有意的：注册链路依赖 hCaptcha
铸造 + 临时邮箱域名 + 代理池，属于另一套系统）。

如果你需要：

- **自动注册**：批量注册 Meshy 账号（hCaptcha → 临时邮箱 → OTP → 建号入库）
- **无限续杯**：账号积分耗尽后自动补号 / 自动切换 / 循环使用，长期稳定不掉线
- **账号池调度**：多账号按积分/并发择优、失败自动降级
- 其他定制（代理池风控、私有部署、按量分发等）

请联系：

> **tmpyunex@yunex.ccwu.cc**

---

## 快速开始

```bash
cp config.example.json config-8090.json
# 编辑 config-8090.json：填 dataDir / proxy，账号可留空，启动后用控制台导入
node server.mjs --config config-8090.json      # → http://127.0.0.1:8090
open http://127.0.0.1:8090/console
```

要求：**Node 18+**（用到全局 `fetch`；3D 任务持久化用 `node:sqlite`，需 Node 22.5+）。
无任何 npm 依赖。`proxy` 很重要——国内直连 `www.meshy.ai` / `auth.meshy.ai`
通常不通，必须配代理。

**空库也能启动**：没有账号时进程不退出，只提供 `/health`、`/console` 和
`/v1/gateway/*` 账号管理接口；生成类接口返回 `503 no_account`，导入并激活账号后即可用。

---

## 账号从哪来

凭据存在独立 SQLite 文件（`credentialsDb`，默认 `<dataDir>/credentials.db`），
与业务库（`jobs3d` / `image_log`）分开，方便整库分发/快照。启动时的解析顺序：

```
env  >  credentials.db 当前账号  >  config.json → gateway  >  data/session.json
```

### 导入方式一览

共有 **4 种**把账号放进凭据库的方式（全部走同一张 `accounts` 表）：

| # | 方式 | 入口 | 需要什么 |
|---|---|---|---|
| 1 | **浏览器登录**（推荐） | 控制台「账号」页按钮 / `/v1/gateway/login/browser/*` | 机器上有 Chrome/Edge + 有头环境；它负责**登录并捕获**，无需手动找 token |
| 2 | **手动粘贴** | 控制台表单 / `POST /v1/gateway/accounts` | 已知 `email` + `refresh_token` + `device_id`（`access_token` 可选） |
| 3 | **上传数据库文件** | 控制台上传 / `POST /v1/gateway/accounts/upload` + `/import-db` | 一份含 `accounts` 表的 `.db`（原项目 `meshy2api.db` 或本网关 `credentials.db`） |
| 4 | **环境变量 / config** | `MESHY_REFRESH_TOKEN` 等 / `config.gateway` | 单账号凭据（不写库，仅作启动用，优先级最高） |

> **批量自动注册**（`register.py`）**不在本网关版本里** —— 那是原项目/付费版的能力。
> 本网关只能上面 4 种方式把已有账号导入。需要自动注册/无限续杯请见上方联系方式。

### 方式一：控制台「账号」页

打开 `http://127.0.0.1:8091/console` → **账号** 页：

1. **浏览器登录**（推荐）：点「打开浏览器并等待登录」→ 网关拉起一个有头浏览器 →
   你在窗口里登录/注册 Meshy → 网关每 3 秒自动检测并捕获会话（`access_token` /
   `refresh_token` / `device_id` / `email`）→ 入库。**网关不接触你的密码**，只读取
   登录成功后 Meshy 自己写下的 cookie 与 `localStorage`。每次开始前会**先清 cookie**，
   可连续登录不同账号。
2. **手动导入**：粘贴 `email` + `refresh_token` + `device_id`（`access_token` 可选）。
3. **从数据库文件导入**：上传一份 `.db`（原项目 `meshy2api.db` 或本网关
   `credentials.db`）→ 列出账号 → 勾选导入。

> 导入时会**验证**：先用 `access_token` 探测（省一次轮换），失败才用
> `refresh_token` 换新，并补齐积分 / 月度刷新时间。导入**不自动切换**当前账号，
> 需在列表里手动点「切换」（热切换，无需重启）。

### 方式二：API

| Method | Path | Description |
|---|---|---|
| GET    | `/v1/gateway/accounts` | 列出账号（token 已脱敏，含 `dead` 标记） |
| POST   | `/v1/gateway/accounts` | 手动导入 `{email, refresh_token, access_token?, device_id, activate?}` |
| POST   | `/v1/gateway/accounts/upload` | 上传 `.db`（base64）→ 解析出账号列表（不入库） |
| POST   | `/v1/gateway/accounts/import-db` | 从上传的 `.db` 导入选中的 `emails[]` |
| POST   | `/v1/gateway/accounts/switch` | 热切换当前账号 `{email}` |
| POST   | `/v1/gateway/accounts/refresh` | 重新验证 + 刷新积分 `{email?}`（默认当前） |
| DELETE | `/v1/gateway/accounts?email=` | 删除账号（当前账号不可删） |
| GET    | `/v1/gateway/active` | 当前生效账号 |
| GET    | `/v1/gateway/login/browser/config` | 浏览器登录配置（端口/路径/启动页） |
| GET    | `/v1/gateway/login/browser/status` | 登录流程状态 |
| POST   | `/v1/gateway/login/browser/start` | 拉起浏览器并开始等待 `{port?, path?, startUrl?}` |
| POST   | `/v1/gateway/login/browser/capture` | 立即尝试捕获 |
| POST   | `/v1/gateway/login/browser/cancel` | 取消并关闭浏览器 |
| GET    | `/v1/gateway/keepalive` | 保号状态（开关/间隔/下次运行/上次结果） |
| POST   | `/v1/gateway/keepalive/run` | 立即跑一次保号 |
| POST   | `/v1/gateway/keepalive/config` | 改保号配置 `{enabled?, intervalMinutes?, deadRetryMinutes?}` |

- 环境变量覆盖：`MESHY_ACCESS_TOKEN` / `MESHY_REFRESH_TOKEN` / `MESHY_EMAIL` /
  `MESHY_DEVICE_ID` / `MESHY_PROXY`（env 优先级最高）。
- 浏览器登录默认端口 `9222`，只监听 `127.0.0.1`；浏览器路径自动探测
  Chrome / Edge / Chromium（Windows / macOS / Linux），也可在控制台手动指定。
- ⚠️ 多个网关实例同时用浏览器登录时，**错开 `browser.port`**（如 9222 / 9223）。

> **快照提醒**：若从"另一个正在运行的桥持续轮换 token"的库快照导入，部分
> `refresh_token` 会是 `Invalid Refresh Token: Already Used`。`access_token` 仍
> 有效的号能正常导入；其余建议在停止那个桥之后再取快照。旧库里若某账号没有
> `device_id`（老版本注册号），导入会被拒，需用浏览器登录重新捕获。

## 自动保号 & 故障转移

两个后台能力，让"号不容易掉"：

**1. 自动保号（keep-alive，定时轮换 RT）**
- 后台定时器每隔 `keepAlive.intervalMinutes`（默认 30，可配）遍历 `credentials.db`
  **全部账号**，用 refresh_token 强制换新 token（串行、间隔 300ms），保持 token 新鲜、
  RT 链路活跃。
- 失败（`Already Used` / `Not Found` / 401）→ 标记死号，`deadRetryMinutes`（默认 60）
  内跳过；结果写入 `/v1/gateway/keepalive` 的 `lastResult`。
- 配置持久化在 `credentials.db` 的 `meta` 表；控制台/接口可随时开关调整。

```jsonc
"keepAlive": { "enabled": true, "intervalMinutes": 30, "deadRetryMinutes": 60 }
```

**2. 自动故障转移（auto pool failover）**
- 当前生效账号的 token 失效时，网关**自动从 `credentials.db` 挑一个没死的号**
  （排除死号、优先积分高），热切换到它并重试；日志 `[auto-pool] active account dead → switched A → B`。
- 死号缓存 10 分钟；`/v1/stats` 的 `account.pool` 与账号列表的 `dead` 字段可见。

> ⚠️ **保号 = 主动轮换 refresh_token。同一个号同一时刻只能有一个持有者！**
> 若一个号同时在别处（朋友的实例 / 浏览器 / 另一个网关）被使用，两边都会轮换 RT →
> **互相作废 → 双双 401**（`Already Used`）。保号只对"你独占、无他处使用"的号安全。
> 反过来说，保号能显著减少「access_token 过期」「RT 长时间闲置失效」这两类 401，
> 但对「被其他持有者轮换」无能为力。

### `deviceId` 必须与浏览器一致

Meshy 每个用户/平台只允许一个活跃设备。device_id 不一致会触发
`403 DeviceKicked / rule1_same_platform`，并让网页端登出。务必沿用浏览器里的
那个 `meshy_device_id`。

---

## 代理

Node 的全局 `fetch` 忽略 `HTTP(S)_PROXY`，所以网关用 `node:http`/`node:tls`
自己手写 CONNECT 隧道（零依赖）：

```jsonc
"proxy": "http://127.0.0.1:7890"     // Clash 默认端口
```

`MESHY_PROXY` / `HTTPS_PROXY` / `HTTP_PROXY` / `ALL_PROXY` 可覆盖。隧道实现会
**手动跟随重定向**（签名 CDN 会 302，GLB/图片下载依赖它）并返回
**`text/event-stream` 的 ReadableStream**（agent turn 依赖它）。

---

## 格式转换（交给第三方）

本网关**只输出 GLB**：Meshy 生成的产物是加密容器 `model.meshy`，网关下载后用本地
WASM 解密成标准 **GLB**（免费、永久有效，不依赖会过期的签名 URL）。

**为什么不做 FBX / OBJ / STL / USDZ / BLEND / 3MF / DXF？**
Meshy 网页端的「导出 / 格式转换」是**会员功能**：免费账号点导出会被前端直接拦下弹出
"开通会员"，请求根本不会发出。而且纯本地的 GLB→其他格式转换并不都能无损完成
（STL/OBJ/PLY 容易；FBX/BLEND/DXF 需要专业导出器）。

所以，**先把 GLB 下载下来，再拿去下面任意一个第三方平台免费转换**即可：

| 平台 | 地址 | 说明 |
|---|---|---|
| **CloudConvert** | https://cloudconvert.com/glb-converter | 支持 GLB→FBX/OBJ/STL/USDZ/BLEND 等 200+ 格式，有 REST API（注册送免费额度） |
| **Aspose.3D** | https://products.aspose.app/3d/conversion/glb | 网页免费转换，支持 glTF/OBJ/STL/FBX/3DS 等；也有 Cloud API |
| **AnyConv** | https://anyconv.com/glb-converter/ | 纯网页，无需注册，小文件够用 |
| **Convertio** | https://convertio.co/glb-stl/ | 网页 + API，免费额度较小 |
| **Vectary** | https://www.vectary.com/3d-model-viewer/ | 在线查看并导出，网页操作 |
| **3DConvert** | https://3d-convert.com/en/ | 在线 3D 转换 + 查看 |
| **Blender**（本地） | https://www.blender.org/ | 想完全离线/批量：导入 GLB → 导出 FBX/STL/OBJ/BLEND 等，最可靠 |

> **格式提示**：从 GLB 转出时，STL 会**丢失颜色与贴图**（3D 打印格式只存网格）；
> OBJ 会**丢失骨骼动画**（动画只在 GLB/FBX/USDZ 中保留）；BLEND 只能用 Blender 生成。

---

## API

| Method | Path | Description |
|---|---|---|
| GET  | `/health` | 存活 + token 到期时间 |
| GET  | `/v1/models` | 对话 + 生图 + 3D 模型列表（3D 档位保留 20/30 成本） |
| POST | `/v1/chat/completions` | OpenAI 兼容对话（agent turn + SSE） |
| POST | `/v1/image/generation` | 生图，6 个模型（`stream:true` 走 SSE） |
| POST | `/v1/3d/generations` | 3D 生成（REST `/v1/files/images` → `/v2/tasks`） |
| GET  | `/v1/3d/generations` | 任务列表 |
| GET  | `/v1/3d/generations/{id}` | 任务详情 |
| GET  | `/v1/3d/generations/{id}/model.glb` | 解密后的 GLB 下载（其他格式见「格式转换」一节） |
| POST | `/v1/3d/animate` | 绑定骨骼 + 应用动画动作 |
| GET  | `/v1/3d/animations` | 内置动作库（632 个 biped 动作） |
| GET  | `/v1/3d/animate/{id}` | 动画任务详情 |
| POST | `/v1/pipeline` | 一条龙：生图 → 3D → 动画 |
| GET  | `/v1/stats` | 桥 + 账号 + 任务统计 |
| GET  | `/v1/image/quota` | 上游生图配额 |

在配置里设 `apiKey`（或环境变量 `MESHY2API_KEY`）即可要求客户端带
`Authorization: Bearer <key>`；留空 = 不校验。

### 3D 模型档位（`MODEL3D_MAP`）

```
meshy-7       → blueberry        Meshy 7.1 - Flagship   20 无贴图 / 30 有贴图
meshy-6       → avocado          Meshy 6                20 / 30
meshy-6-lite  → meshy-6-lite     Meshy 6 Lite           20 / 30
meshy-5.1     → meshy-5.1        Meshy 5                20 / 30
meshy-4       → meshy-4          Meshy 4                20 / 30
meshy-t2      → smart_topology   Meshy T2               5
```

---

## 关键实现说明

- **3D 用的是 `/v1/files/images`，不是 `/v4/agent/artifacts/upload`。** 后者返回的
  `img_upload_*` id 无法被 `imageIds` 引用（`Image not found`）；`/v1/files/images`
  返回的才是 `imageIds` 需要的 UUID。3D 只发**一次**
  `POST /v2/tasks phase:"draft"`（服务端自动串成 generate），不是两次请求。
- **免费账号只允许 1 个 pending 任务。** agent turn 用模块级 `turnChain` 串行化；
  3D REST 任务额外用 `with3dSlot` FIFO 排队（队列深度见 `/v1/stats` 的
  `bridge.pending3d`），避免并发调 `/v1/3d/generations` 直接吃 `TooManyPendingTasks`。
- **GLB 解密**：下载签名的加密容器，用逆向出来的 WASM worker 本地解密
  （`recon/decrypt/meshy_decrypt.mjs` + `mesh_loader.wasm`）。可用
  `"decrypt3dGlb": false` 关闭。
- **浏览器登录用 CDP**：网关用 `--remote-debugging-port` 拉起独立 profile 的有头
  浏览器，等用户自己登录后读取该域的 `sb-auth-auth-token` cookie 与
  `meshy_device_id`。调试端口只绑 `127.0.0.1`，捕获后自动关闭浏览器实例。
- 持久化用 SQLite（`node:sqlite`）：业务表 `jobs3d` / `image_log`，凭据表
  `accounts`（在独立的 `credentials.db`）。

---

## 与原版（多账号）的差异

本版本**没有**：

- `recon/register.py`（批量注册）
- `mail` 配置 + `GET /v1/config/mail`
- 账号池：`AccountPool` / `AccountClient` / `resolveClient` / `pickDistinct`
- `/v1/accounts*`（注册 / 刷新积分 / 签到 / 关注创作者 / 导出 / 删除 / rig 配额）、
  积分任务、每日签到
- 代理池 `/v1/proxies*` + `ipUsageDb`（出网 `config.proxy` 保留）
- 并行扇出 `/v1/image/generation/parallel`、`/v1/3d/generations/parallel`
- 风控 `MESHY_IP_GUARD` / ip_usage
- 调试端点 `/v1/models/debug`、`MESHY_DEBUG_3D`、`MESHY_DEBUG_ARTIFACT`

**需要自动注册 / 无限续杯 / 账号池版本，请联系：tmpyunex@yunex.ccwu.cc**
