# Tool Guidance - 多工具使用说明

本文档提供所有可用工具的详细使用指导。

## 文档访问工具

<when_tool list_files>
### 1. list_files - 列出目录文件

**功能**：列出指定目录中的文件，支持模式匹配和递归搜索。

**参数**：
- `directory` (必需)：目录路径
- `pattern` (可选)：文件名模式（如 `*.md`）
- `recursive` (可选)：是否递归搜索子目录
- `limit` (可选)：最大返回数量

**示例**：
```json
{
  "directory": "workspace/docs",
  "pattern": "*.md",
  "recursive": true,
  "limit": 20
}
```

**返回值**：
- `files`: 文件列表（包含名称、路径、大小、修改时间）
- `total`: 文件总数
- `filtered`: 返回的文件数

**最佳实践**：
- 先不使用 `pattern`，了解目录全貌
- 使用 `recursive=false` 避免过度搜索
- 合理设置 `limit` 避免结果过多
</when_tool>

<when_tool get_file_sections>
### 2. get_file_sections - 提取文件章节

**功能**：提取文件的章节结构，支持 Markdown 标题识别。

**参数**：
- `file_path` (必需)：文件路径
- `limit` (可选)：最大返回 section 数

**支持格式**：
- Markdown 文件（识别 # 标题）
- 其他文本文件（返回单个 section）

**返回值**：
- `sections`: 章节列表（title, line_range, line_count, preview）
- `total_sections`: 章节总数
- `total_lines`: 文件总行数

**使用场景**：
1. 分析文档结构
2. 定位特定章节
3. 快速了解文档内容

**组合使用**：
```
list_files → get_file_sections → read_file
   ↓              ↓                  ↓
  发现文档      了解结构          读取内容
```
</when_tool>

<when_tool search_files>
### 3. search_files - 搜索文件内容

**功能**：在目录中搜索包含关键词的文件（大小写不敏感）。

**参数**：
- `directory` (必需)：搜索目录
- `query` (必需)：搜索关键词
- `include_types` (可选)：文件类型过滤（如 `["md", "txt"]`）
- `limit` (可选)：最大返回匹配数

**搜索策略**：
- 使用精确关键词（避免过于泛泛）
- 先搜索特定文件类型
- 关注匹配行号和上下文

**返回值**：
- `matches`: 匹配列表（file, line_number, matched_line_content）
- `total_matches`: 匹配总数

<when_tool read_file>
**后续操作**：找到匹配后，使用 `read_file` 读取完整上下文。
</when_tool>
</when_tool>

<when_tool read_file>
### 4. read_file - 读取文件内容

**功能**：读取文件内容，支持指定行范围。

**参数**：
- `file_path` (必需)：文件路径
- `start_line` (可选)：起始行号（1-based）
- `end_line` (可选)：结束行号（inclusive）

**编码支持**：
- UTF-8（优先）
- GBK（中文）
- Latin1 / CP1252

**性能建议**：
- 大文件使用行范围限制
- 避免一次读取整个超大文件
- 配合 `get_file_sections` 定位行范围

**返回值**：
- `content`: 文件内容
- `total_lines`: 总行数
- `line_range`: 实际读取范围（如指定）
- `truncated`: 是否被截断（系统自动限制）
</when_tool>

## 工具组合模式

<when_tag doc>
### 文档分析工作流

**标准流程**：
1. **发现阶段**：`list_files` 列出目录文件
2. **结构阶段**：`get_file_sections` 了解文档结构
3. **搜索阶段**：`search_files` 定位关键内容
4. **读取阶段**：`read_file` 获取详细信息

**快速模式**（已知文件路径）：
- 跳过 `list_files`
- 直接 `get_file_sections` 或 `read_file`

**搜索模式**（关键词导向）：
- `search_files` 找到匹配
- `read_file` 读取上下文
</when_tag>

## 通用工具

<when_tool get_current_time>
### get_current_time - 获取当前时间

**功能**：获取当前时间信息。

**参数**：
- `timezone` (可选)：时区（默认 Asia/Shanghai）

**返回值**：
- `time`: 格式化时间（YYYY年MM月DD日 HH:MM:SS）
- `weekday`: 星期几
- `iso`: ISO 8601 格式

**使用场景**：
- 记录操作时间
- 时间戳标记
- 时间相关推理
</when_tool>

## 限制与约束

### 数量限制
- `list_files`: 最多 {{list_files_max}} 个文件
- `get_file_sections`: 最多 {{get_sections_max}} 个 section
- `search_files`: 最多 {{search_max_results}} 个匹配
- `read_file`: 最多 {{read_file_max_chars}} 字符

### 安全限制
- 只能访问已注册的路径
- 自动验证路径安全性
- 防止路径遍历攻击

---

**重要提示**：
1. 优先使用工具获取信息，不要臆造
2. 合理设置 `limit` 参数避免数据过载
3. 工具调用失败时，检查路径权限和参数格式
