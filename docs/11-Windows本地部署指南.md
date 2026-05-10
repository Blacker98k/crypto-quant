# Windows 本地部署指南

最后更新：2026-05-10

本文面向接手本项目的 AI 助手，用于在 Windows 机器上从零部署、启动、验证 crypto-quant 本地看板与模拟交易系统。执行时应主动完成环境安装、依赖安装、服务启动和验证，不要把缺失依赖直接丢给用户处理。

## 1. 安全边界

默认只部署和运行模拟交易。

- 不读取、不提交、不记录任何 API key、secret、账户资金、真实余额或真实盈亏。
- 不切换 live trading，不执行真实下单。
- 不把本地看板暴露到公网；局域网访问也只用于受信任网络。
- 不使用 `git reset --hard`、`git checkout --` 等破坏性命令回退用户改动。
- 发现工作区有未提交改动时，先理解改动来源和范围，再继续部署。

## 2. 进入仓库并检查状态

```powershell
Set-Location "C:\Users\小伟\Documents\Codex\2026-05-08\github\crypto-quant"
git status -sb
git fetch origin
git rev-list --left-right --count HEAD...origin/main
```

判断规则：

- 输出 `0 0` 表示本地 HEAD 与 `origin/main` 的提交历史同步。
- 如果 `git status -sb` 显示 `M`、`??`，说明工作区还有本地未提交改动；此时远端不包含这些当前改动。
- 部署不要求工作区干净，但发布、合并或交付前必须说明哪些改动尚未提交。

## 3. 缺失依赖时由 AI 自行安装

先检查版本：

```powershell
git --version
python --version
uv --version
node --version
```

如果缺少工具，AI 应自行安装并重新打开或刷新当前 PowerShell 环境。

```powershell
winget install --id Git.Git -e
winget install --id Python.Python.3.11 -e
winget install --id astral-sh.uv -e
winget install --id OpenJS.NodeJS.LTS -e
```

如果 `winget` 无法安装 `uv`，使用官方安装脚本：

```powershell
irm https://astral.sh/uv/install.ps1 | iex
```

如果 Python 包出现本地编译失败，再安装 Visual Studio Build Tools：

```powershell
winget install --id Microsoft.VisualStudio.2022.BuildTools -e
```

安装后重新检查版本。不要要求用户手动安装，除非机器权限完全阻止 AI 执行安装。

## 4. 安装项目依赖

```powershell
uv sync
```

如果网络环境需要代理，先设置代理；本机代理端口按实际环境调整：

```powershell
$env:HTTP_PROXY = "http://127.0.0.1:57777"
$env:HTTPS_PROXY = "http://127.0.0.1:57777"
$env:NO_PROXY = "localhost,127.0.0.1,0.0.0.0,::1"
$env:no_proxy = $env:NO_PROXY
```

注意：代理只用于访问外网。访问本地看板时必须设置 `NO_PROXY`，否则浏览器或脚本可能把 `127.0.0.1` 请求也发给代理，导致看板假性不可用。

## 5. 启动看板与模拟交易

本地访问：

```powershell
$env:CQ_DASHBOARD_HOST = "127.0.0.1"
$env:CQ_DASHBOARD_PORT = "8089"
$env:NO_PROXY = "localhost,127.0.0.1,0.0.0.0,::1"
$env:no_proxy = $env:NO_PROXY
uv run python -m dashboard.server
```

如果 Binance 公共行情需要走本机代理：

```powershell
$env:CQ_BINANCE_PROXY = "http://127.0.0.1:57777"
uv run python -m dashboard.server
```

如果需要局域网手机访问，把 host 改为 `0.0.0.0`：

```powershell
$env:CQ_DASHBOARD_HOST = "0.0.0.0"
$env:CQ_DASHBOARD_PORT = "8089"
uv run python -m dashboard.server
```

后台运行可以使用：

```powershell
Start-Process -FilePath "uv" `
  -ArgumentList @("run", "python", "-m", "dashboard.server") `
  -WorkingDirectory "C:\Users\小伟\Documents\Codex\2026-05-08\github\crypto-quant" `
  -WindowStyle Hidden
```

停止占用 8089 的旧服务：

```powershell
$procIds = Get-NetTCPConnection -LocalPort 8089 -State Listen -ErrorAction SilentlyContinue |
  Select-Object -ExpandProperty OwningProcess -Unique
foreach ($procId in $procIds) {
  Stop-Process -Id $procId -Force
}
```

## 6. 验证后端接口

服务启动后先检查状态接口：

```powershell
Invoke-RestMethod http://127.0.0.1:8089/api/status | ConvertTo-Json -Depth 8
Invoke-RestMethod http://127.0.0.1:8089/api/data_health | ConvertTo-Json -Depth 8
Invoke-RestMethod http://127.0.0.1:8089/api/strategy_matrix | ConvertTo-Json -Depth 8
Invoke-RestMethod http://127.0.0.1:8089/api/fills?limit=10 | ConvertTo-Json -Depth 8
Invoke-RestMethod http://127.0.0.1:8089/api/positions | ConvertTo-Json -Depth 8
```

关键观察点：

- `simulation_running` 应为 true。
- `market_data_stale` 应为 false。
- `price_count` 或行情列表应非空。
- 策略矩阵应看到当前启用策略的评估时间持续更新。
- 成交可能不是每秒都有；如果长时间没有新成交，要结合策略矩阵、风控事件和行情新鲜度判断原因。

## 7. 验证 WebSocket

前端右上角的 WS 状态来自本地看板 WebSocket，不是 Binance 平台的账户连接状态。

用 Node 做一次本地 WS smoke test：

```powershell
@'
const ws = new WebSocket('ws://127.0.0.1:8089/ws');
const timeout = setTimeout(() => {
  console.log(JSON.stringify({ ok: false, error: 'timeout' }));
  ws.close();
}, 5000);
ws.onmessage = (event) => {
  clearTimeout(timeout);
  const msg = JSON.parse(event.data);
  console.log(JSON.stringify({
    ok: true,
    ws_connected: msg.ws_connected,
    price_count: Object.keys(msg.prices || {}).length,
    position_count: (msg.positions || []).length,
    stale: msg.market_data_stale
  }));
  ws.close();
};
ws.onerror = (error) => {
  clearTimeout(timeout);
  console.log(JSON.stringify({ ok: false, error: String(error) }));
};
'@ | node -
```

如果这个测试通过，但页面显示未连接，优先检查前端是否连错地址、浏览器缓存、代理绕过设置，以及控制台报错。

## 8. 局域网手机访问

启动时使用：

```powershell
$env:CQ_DASHBOARD_HOST = "0.0.0.0"
$env:CQ_DASHBOARD_PORT = "8089"
uv run python -m dashboard.server
```

查找电脑局域网 IP：

```powershell
Get-NetIPAddress -AddressFamily IPv4 |
  Where-Object { $_.IPAddress -ne "127.0.0.1" -and $_.IPAddress -notlike "169.254*" } |
  Select-Object IPAddress, InterfaceAlias
```

如 Windows 防火墙阻止访问，只允许专用网络入站：

```powershell
New-NetFirewallRule `
  -DisplayName "crypto-quant dashboard 8089" `
  -Direction Inbound `
  -Action Allow `
  -Protocol TCP `
  -LocalPort 8089 `
  -Profile Private
```

手机和电脑连接同一 Wi-Fi 后访问：

```text
http://<电脑局域网IP>:8089/
```

不要把该端口映射到公网，因为本地看板默认没有登录鉴权。

## 9. 质量门

部署后若改过代码，至少运行：

```powershell
uv run pytest -q
uv run ruff check .
uv run mypy core
git diff --check
```

如果只改前端 HTML，不要把 HTML 文件传给 ruff。ruff 只检查 Python；前端页面应通过浏览器、接口和已有静态测试验证。

## 10. 常见踩坑

### 10.1 本地请求被代理污染

症状：浏览器或脚本访问 `127.0.0.1:8089` 失败，但服务实际在运行。

处理：

```powershell
$env:NO_PROXY = "localhost,127.0.0.1,0.0.0.0,::1"
$env:no_proxy = $env:NO_PROXY
```

### 10.2 Binance WebSocket 偶发连接失败

症状：行情 REST 正常，但 WS 显示未连接或连接后很快断开。

处理顺序：

1. 确认 `CQ_BINANCE_PROXY` 是否需要设置，端口是否可用。
2. 调用 `/api/data_health` 判断行情是否仍然新鲜。
3. 使用 Node smoke test 验证本地看板 WS 是否正常。
4. 确保代码包含 Windows/aiohttp DNS 兼容处理；旧版本可能因 DNS resolver 在 Windows 上不稳定。

### 10.3 启动后长时间显示未预热

策略需要 K 线窗口。正确行为是先预热 1m 数据并连接 WS，再在后台补齐更长周期 K 线。若启动阶段阻塞在完整历史回补，页面会显得像没连上。

### 10.4 看错数据库

实际运行数据在：

```text
data/dashboard.sqlite
```

不要被仓库根目录下的旧 sqlite 文件或临时数据库误导。查询成交、持仓、风控事件时以当前服务配置的数据文件为准。

### 10.5 历史成交里出现已停用策略

删除或停用策略不会自动删除历史成交。历史记录是审计数据，保留是正常的。判断当前是否还在运行某策略，应看策略矩阵、最新 run log 和新成交的 strategy 字段。

### 10.6 端口混淆

当前看板默认端口是 8089。旧脚本或旧文档里可能出现 8088，应以当前 `dashboard.server` 和环境变量为准。

### 10.7 PowerShell 变量名

不要把循环变量命名为 `$PID`，它是 PowerShell 内置只读变量。停止进程时使用 `$procId`。

### 10.8 中文路径和乱码

仓库路径里有中文用户名。现代 PowerShell 通常没问题；如果注释或输出乱码，切到 UTF-8 终端：

```powershell
chcp 65001
```

### 10.9 Git 代理

如果 `git fetch` 或 `git push` 失败，而普通 HTTP 请求能走代理，可能需要临时给 Git 指定代理：

```powershell
git -c http.proxy=http://127.0.0.1:57777 `
    -c https.proxy=http://127.0.0.1:57777 `
    fetch origin
```

## 11. 部署完成判定

满足以下条件才算部署可用：

- 页面可打开：`http://127.0.0.1:8089/`
- `/api/status` 返回模拟交易运行中。
- `/api/data_health` 显示行情新鲜。
- 本地 `/ws` 能收到推送。
- 实时行情和当前持仓随真实 Binance 公共行情变化更新。
- 策略矩阵的评估时间持续推进。
- 风控事件、成交、持仓、策略表现接口能够正常返回结构化数据。
- 未读取 secrets，未启用 live trading，未发生真实下单。

## 12. 接手 AI 的工作准则

- 缺依赖就安装，安装后验证版本。
- 服务没跑就启动，端口被占就定位并处理旧进程。
- 数据不刷新就从接口、WS、日志、代理、数据库五个方向排查。
- 没有成交时不要伪造成交；应检查策略信号、行情预热、风控拒单、最小名义金额、冷却时间和仓位限制。
- 文档、提交信息和公开评论不要写入用户具体资金、余额、收益或亏损数字。
- 修改部署、风控、策略、数据口径时，同步更新相关文档和测试。
