# Projectying（萤）

一个在 Termux/终端中运行的AI聊天程序（测试中） Rust TUI（ratatui）项目，支持 MAIN/DOG 双通道与工具调用链。（半成品状态）


## Termux 一键运行（自动自检并安装依赖）

```bash
./run.sh
```

如有错误运行自检：

```bash
./selfcheck.sh
```

直接用 `cargo run` 也会在启动前尝试执行 `scripts/bootstrap.sh` 自动补齐依赖；如需跳过：`cargo run -- --no-bootstrap`。

## 依赖自检/自动安装

- 通用入口：`./scripts/bootstrap.sh`
- Termux 专用：`./scripts/bootstrap-termux.sh`（会用 `pkg install -y` 自动补齐依赖）

自检脚本（会先 bootstrap，再跑测试与内置 `--selfcheck`）：

```bash
./scripts/selfcheck.sh
```

## 配置（默认含测试 key）

本项目运行时默认读取：

- `config/dog_api.json`
- `config/main_api.json`
- `config/system.json`

仓库默认提供一套用于联调/演示的测试 key（已做限额）。如需使用你自己的 key，可用示例覆盖后再填写：

```bash
cp -n config/dog_api.example.json  config/dog_api.json
cp -n config/main_api.example.json config/main_api.json
cp -n config/system.example.json   config/system.json
```

注意：Termux 的 `termux-api` 包只能提供命令；若命令调用失败，还需要在手机上安装 **Termux:API APP** 并授予权限。
