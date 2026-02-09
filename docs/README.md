# openhare 官网（静态站点）

本目录用于 GitHub Pages 发布的静态官网。

## 目录结构

- `docs/`：官网文档目录
- `docs/site/`：Astro 项目源码（开发、构建用）

## 语言

- English（默认）：`/`
- 中文：`/zh/`

## 本地开发

```bash
cd docs/site
npm install
npm run dev
```

## 构建（输出到 docs/site/dist）

```bash
cd docs/site
npm run build
```

构建完成后应能看到：

- `docs/site/dist/index.html`
- `docs/site/dist/assets/...`

> 注意：构建产物不提交到主分支，由 GitHub Actions 负责构建并发布到 GitHub Pages。

## GitHub Pages 发布

在 GitHub 仓库中打开 **Settings → Pages**：

- **Source**：GitHub Actions

然后推送代码即可自动构建并发布。

## 配置 base 路径（重要）

如果你的 Pages 地址是 `https://<user>.github.io/<repo>/`，则需要设置 base 为 `/<repo>/`。

本项目在 CI 中会根据仓库名自动注入 `BASE_PATH`（例如 `/openhare/`），本地默认按仓库名 `openhare` 配置。需要调整时，编辑：

- `docs/site/astro.config.mjs`

对应的 Pages 工作流文件：

- `.github/workflows/pages.yml`

