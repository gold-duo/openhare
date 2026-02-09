import { defineConfig } from 'astro/config';

/**
 * GitHub Pages 通常发布在子路径：
 *   https://<user>.github.io/<repo>/
 * 因此需要设置 base 为 `/<repo>/`。
 */
const repoName = process.env.REPO_NAME ?? 'openhare';
const base = process.env.BASE_PATH ?? `/${repoName}/`;

export default defineConfig({
  // 构建产物输出到本项目 dist，由 GitHub Actions 发布到 Pages
  outDir: './dist',
  base,
  build: {
    // 将默认的 `_astro/` 改为更通用的目录名
    assets: 'assets',
  },

  // dist 可安全清空
  vite: {
    build: {
      emptyOutDir: true,
    },
  },
});

