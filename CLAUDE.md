# CLAUDE.md - AI Assistant Guide

**Last Updated:** November 17, 2025
**Project:** Crawl4AI RAG MCP Server
**Purpose:** Guide for AI assistants (Claude, GPT, etc.) working with this codebase

---

## Project Overview

This is a **Model Context Protocol (MCP) server** that provides AI agents with web crawling and RAG (Retrieval-Augmented Generation) capabilities. It integrates:
- **Crawl4AI** for intelligent web crawling
- **Supabase + pgvector** for vector storage
- **OpenAI embeddings** for semantic search
- **FastMCP** for the MCP server framework

**Core Purpose:** Enable AI agents to crawl websites, store content in a vector database, and perform semantic search for RAG.

**Future Vision:** Integrate into the [Archon project](https://github.com/coleam00/Archon) as a knowledge engine for AI coding assistants.

---

## Codebase Structure

### Directory Layout

```
mcp-crawl4ai-rag/
├── src/                          # All source code
│   ├── crawl4ai_mcp.py          # Main MCP server (646 lines)
│   ├── utils.py                  # Utilities (294 lines)
│   └── file_scraper.py          # File scraper (456 lines)
├── crawled_pages.sql             # Database schema
├── pyproject.toml                # Dependencies & metadata
├── uv.lock                       # Locked dependencies
├── Dockerfile                    # Container config
├── .env.example                  # Environment template
├── README.md                     # User documentation
├── UPDATES.md                    # Status report (this session)
├── CLAUDE.md                     # This file
└── LICENSE                       # MIT License
```

### File Descriptions

#### `src/crawl4ai_mcp.py` (Main Server)

**Role:** MCP server entry point with tool definitions

**Key Components:**
- `Crawl4AIContext` - Dataclass holding crawler and Supabase client
- `crawl4ai_lifespan()` - Async context manager for resource lifecycle
- `mcp` - FastMCP server instance
- Five `@mcp.tool()` decorated functions (the MCP tools)

**MCP Tools Exposed:**
1. `crawl_single_page(url: str)` - Crawl and store a single page
2. `smart_crawl_url(url: str, max_depth: int = 3, max_concurrent: int = 5, chunk_size: int = 1000)` - Intelligent multi-page crawling
3. `get_available_sources()` - List all crawled domains
4. `perform_rag_query(query: str, source: Optional[str] = None, match_count: int = 5)` - Semantic search
5. `execute_site_scraping_to_files(target_urls: List[str], output_base_dir: str, max_depth: int = 3, save_raw_html: bool = False)` - Save to JSON files

**Important Functions:**
- `is_sitemap(url)` - Detect sitemap URLs
- `is_txt(url)` - Detect text file URLs
- `get_all_page_urls(url, crawler)` - Extract URLs from sitemap/txt
- `chunk_markdown(markdown, chunk_size)` - Smart chunking
- `crawl_recursive(crawler, url, domain, max_depth, visited, supabase_client, chunk_size)` - Recursive crawler

**Entry Point:**
```python
if __name__ == "__main__":
    import uvicorn
    # ... config from env vars ...
    uvicorn.run(...)
```

#### `src/utils.py` (Utilities)

**Role:** Helper functions for embeddings and database operations

**Key Functions:**
- `get_supabase_client()` - Initialize Supabase with env vars
- `create_embeddings_batch(texts: List[str])` - Batch OpenAI embeddings
- `create_embedding(text: str)` - Single embedding (calls batch internally)
- `generate_contextual_embedding(full_document: str, chunk: str)` - **Advanced feature** for contextual retrieval
- `add_documents_to_supabase(client, documents, batch_size=20)` - Insert with de-duplication
- `search_documents(client, query_text, source_filter=None, match_count=5)` - Vector search

**Important Details:**
- Uses `text-embedding-3-small` model (hard-coded, flagged for improvement)
- Contextual embeddings use `MODEL_CHOICE` env var (optional LLM)
- Parallel contextual generation with ThreadPoolExecutor (10 workers)
- Fallback to zero-vectors on embedding failure (questionable, flagged in UPDATES.md)
- Batch size default: 20 (hard-coded, noted for future config)

#### `src/file_scraper.py` (File Scraper)

**Role:** Save scraped content to local JSON files

**Key Functions:**
- `scrape_page_to_data(page_url, page_html, page_markdown, page_links, source_domain, crawl_depth)` - Extract structured data
- `scrape_site_to_files(target_urls, output_base_dir, crawler, max_depth=3, save_raw_html=False)` - Recursive scraping with file output
- `_sanitize_filename(url)` - Convert URL to safe filename

**Output Format:** JSON files with:
```json
{
  "url": "...",
  "title": "...",
  "metadata": [...],
  "markdown_content": "...",
  "text_content": "...",
  "internal_links": [...],
  "external_links": [...],
  "images": [...],
  "source_domain": "...",
  "crawl_depth": 0,
  "timestamp": "...",
  "processing_errors": [...]
}
```

**Status:** Added via PR #1, noted as "not fully tested due to Playwright environment issues"

#### `crawled_pages.sql` (Database Schema)

**Database:** PostgreSQL with pgvector extension (via Supabase)

**Table: `crawled_pages`**
```sql
CREATE TABLE crawled_pages (
    id bigserial PRIMARY KEY,
    url varchar UNIQUE,
    chunk_number integer,
    content text,
    metadata jsonb,
    embedding vector(1536),
    created_at timestamp DEFAULT now(),
    UNIQUE(url, chunk_number)
);
```

**Indexes:**
- `crawled_pages_embedding_idx` - IVFFlat for vector search (lists=100)
- `crawled_pages_metadata_idx` - GIN for metadata filtering
- `crawled_pages_source_idx` - B-tree on `metadata->>'source'`

**Function: `match_crawled_pages`**
```sql
match_crawled_pages(
    query_embedding vector(1536),
    match_count int,
    filter jsonb DEFAULT '{}'
)
```
Returns: Table with `id, url, chunk_number, content, metadata, similarity`

**Security:** RLS enabled with policy for public read access

---

## Development Workflows

### Initial Setup

**Option 1: Docker (Recommended)**
```bash
# Clone repo
git clone https://github.com/coleam00/mcp-crawl4ai-rag.git
cd mcp-crawl4ai-rag

# Create .env from template
cp .env.example .env
# Edit .env with your credentials

# Build
docker build -t mcp/crawl4ai-rag --build-arg PORT=8051 .

# Run
docker run --env-file .env -p 8051:8051 mcp/crawl4ai-rag
```

**Option 2: Direct Python**
```bash
# Install uv
pip install uv

# Create virtual environment
uv venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
uv pip install -e .

# Set up Playwright browsers (required!)
crawl4ai-setup

# Run
uv run src/crawl4ai_mcp.py
```

**Database Setup:**
1. Create Supabase project at https://supabase.com
2. Go to SQL Editor in dashboard
3. Run contents of `crawled_pages.sql`
4. Copy project URL and service key to `.env`

### Environment Variables

**Required:**
- `OPENAI_API_KEY` - OpenAI API key for embeddings
- `SUPABASE_URL` - Supabase project URL
- `SUPABASE_SERVICE_KEY` - Supabase service role key

**Optional:**
- `TRANSPORT` - "sse" or "stdio" (default: "sse")
- `HOST` - Server host (default: "0.0.0.0")
- `PORT` - Server port (default: "8051")
- `MODEL_CHOICE` - LLM for contextual embeddings (e.g., "gpt-4")

### Running the Server

**SSE Mode (for most MCP clients):**
```bash
# .env should have TRANSPORT=sse
uv run src/crawl4ai_mcp.py
# Server runs on http://localhost:8051/sse
```

**Stdio Mode (for Claude Desktop):**
```bash
# .env should have TRANSPORT=stdio
uv run src/crawl4ai_mcp.py
# Server communicates via stdin/stdout
```

### Git Workflow

**Current State (November 2025):**
- Working on feature branch: `claude/document-repo-status-017FPt7W3sWk2zNCKT5trf3e`
- No main/master branch established yet (flagged in UPDATES.md)

**Recommended (Future):**
```bash
# Create feature branch
git checkout -b feature/your-feature-name

# Make changes
git add .
git commit -m "feat: description"

# Push
git push -u origin feature/your-feature-name

# Create PR to main (once main branch established)
```

**Commit Message Convention:**
Use conventional commits:
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `test:` - Adding/updating tests
- `refactor:` - Code refactoring
- `chore:` - Maintenance tasks

### Testing (When Implemented)

**Current State:** No automated tests exist (critical gap, see UPDATES.md)

**Planned (Future):**
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/test_utils.py

# Run specific test
pytest tests/test_utils.py::test_create_embeddings_batch
```

---

## Key Conventions

### Code Style

**Python Version:** 3.12+ (modern features expected)

**Type Hints:** Use throughout
```python
def crawl_single_page(url: str) -> dict[str, Any]:
    ...
```

**Async/Await:** Prefer async functions for I/O operations
```python
async def crawl_recursive(...) -> None:
    result = await crawler.arun(...)
```

**Dataclasses:** Use for structured data
```python
@dataclass
class Crawl4AIContext:
    crawler: AsyncWebCrawler
    supabase_client: Client
```

**Docstrings:** Google-style docstrings
```python
def chunk_markdown(markdown: str, chunk_size: int = 1000) -> List[str]:
    """
    Split markdown into chunks while preserving structure.

    Args:
        markdown: The markdown content to chunk
        chunk_size: Target size for each chunk

    Returns:
        List of markdown chunks
    """
```

### Error Handling

**Current Pattern (will be improved):**
```python
try:
    result = openai.embeddings.create(...)
except Exception as e:
    print(f"Error: {e}")  # Will be replaced with logging
    return default_value
```

**Future Pattern:**
```python
import logging

logger = logging.getLogger(__name__)

try:
    result = openai.embeddings.create(...)
except openai.RateLimitError as e:
    logger.warning(f"Rate limited, retrying: {e}")
    # Retry logic
except openai.APIError as e:
    logger.error(f"API error: {e}")
    raise
```

### Configuration

**Current:** Mix of env vars and hard-coded values
```python
# Hard-coded (flagged for improvement)
model="text-embedding-3-small"
batch_size=20
```

**Future:** Centralized configuration
```python
@dataclass
class Config:
    embedding_model: str = "text-embedding-3-small"
    batch_size: int = 20
    max_workers: int = 10
    # ... etc
```

### Naming Conventions

**Functions:** `snake_case`
```python
def get_all_page_urls(...):
```

**Classes:** `PascalCase`
```python
class Crawl4AIContext:
```

**Constants:** `UPPER_SNAKE_CASE`
```python
DEFAULT_CHUNK_SIZE = 1000
```

**Private Functions:** Prefix with `_`
```python
def _sanitize_filename(url: str) -> str:
```

---

## Architecture Patterns

### MCP Tool Pattern

**Structure:**
```python
@mcp.tool()
async def tool_name(
    ctx: Context[Crawl4AIContext],
    param1: str,
    param2: int = default_value
) -> dict[str, Any]:
    """
    Tool description for AI agents.

    Args:
        ctx: MCP context with crawler and supabase_client
        param1: Description
        param2: Description

    Returns:
        Dictionary with success status and results
    """
    # Access context
    crawler = ctx.deps.crawler
    supabase = ctx.deps.supabase_client

    # Do work
    result = await some_async_operation()

    # Return structured response
    return {
        "success": True,
        "data": result,
        "message": "Operation completed"
    }
```

### Lifespan Management

**Pattern:**
```python
@asynccontextmanager
async def crawl4ai_lifespan(server: FastMCP) -> AsyncIterator[Crawl4AIContext]:
    # Setup
    crawler = AsyncWebCrawler(...)
    await crawler.__aenter__()
    client = get_supabase_client()

    try:
        yield Crawl4AIContext(crawler=crawler, supabase_client=client)
    finally:
        # Cleanup
        await crawler.__aexit__(None, None, None)
```

### Crawling Pattern

**Recursive Crawling:**
```python
visited = set()

async def crawl_recursive(crawler, url, domain, max_depth, visited, ...):
    if max_depth == 0 or url in visited:
        return

    visited.add(url)
    result = await crawler.arun(url)

    # Process content
    await process_and_store(result)

    # Follow links
    for link in extract_internal_links(result):
        await crawl_recursive(crawler, link, domain, max_depth - 1, visited, ...)
```

### Chunking Strategy

**Smart Chunking (respects markdown structure):**
1. Split by headers (##, ###)
2. If chunk too large, split by paragraphs
3. If still too large, split by sentences
4. If still too large, split by words
5. Preserve code blocks (don't split)

**Implementation:** See `chunk_markdown()` in `src/crawl4ai_mcp.py:292-368`

### Database Operations

**Batch Insert Pattern:**
```python
# Delete existing records for URL
client.table("crawled_pages").delete().eq("url", url).execute()

# Prepare batch
batch = []
for chunk in chunks:
    embedding = create_embedding(chunk)
    batch.append({
        "url": url,
        "chunk_number": i,
        "content": chunk,
        "embedding": embedding,
        "metadata": {...}
    })

    if len(batch) >= batch_size:
        client.table("crawled_pages").insert(batch).execute()
        batch = []

# Insert remaining
if batch:
    client.table("crawled_pages").insert(batch).execute()
```

**Vector Search Pattern:**
```python
# Create query embedding
query_embedding = create_embedding(query_text)

# Search with optional filtering
results = client.rpc(
    "match_crawled_pages",
    {
        "query_embedding": query_embedding,
        "match_count": 5,
        "filter": {"source": domain} if domain else {}
    }
).execute()
```

---

## Common Tasks for AI Assistants

### Adding a New MCP Tool

1. **Define the tool function:**
```python
@mcp.tool()
async def new_tool_name(
    ctx: Context[Crawl4AIContext],
    param: str
) -> dict[str, Any]:
    """
    Clear description for AI agents.

    Args:
        ctx: Context
        param: Parameter description

    Returns:
        Result dictionary
    """
    # Implementation
    return {"success": True, "result": "..."}
```

2. **Test manually** (no automated tests yet)

3. **Update README** with tool documentation

4. **Update this file** (CLAUDE.md) with any new patterns

### Adding a New Utility Function

1. **Add to `src/utils.py`:**
```python
def new_utility_function(param: str) -> ReturnType:
    """
    Clear docstring.

    Args:
        param: Description

    Returns:
        Description
    """
    # Implementation
```

2. **Import in `crawl4ai_mcp.py`:**
```python
from utils import get_supabase_client, new_utility_function
```

3. **Use in tools** as needed

### Modifying Database Schema

1. **Update `crawled_pages.sql`** with new schema

2. **Add migration notes** in comments

3. **Test in Supabase dashboard** before committing

4. **Update affected functions** in `utils.py`

5. **Document changes** in commit message

### Adding Configuration Options

**Current (until refactoring):**
1. Add to `.env.example`
2. Use `os.getenv()` with default value
3. Document in README

**Future (after config refactoring):**
1. Add to Config dataclass
2. Use config object throughout
3. Add validation

### Debugging Issues

**Current Tools:**
- `print()` statements (will be replaced with logging)
- Docker logs: `docker logs <container_id>`
- Manual testing with MCP client

**Future Tools:**
- Structured logging with levels
- Log aggregation
- Error tracking (Sentry or similar)

---

## Known Issues & Workarounds

### Issue: No Automated Tests

**Problem:** Changes may break existing functionality without detection

**Workaround:**
- Manual testing before commits
- Test with actual MCP client (Claude Desktop, Windsurf, etc.)
- Check both SSE and stdio transports

**Future:** See UPDATES.md for testing roadmap

### Issue: Hard-coded Embedding Model

**Problem:** Can't switch to different models or use local embeddings

**Current Workaround:** Edit `utils.py:45` to change model

**Future:** Configuration-based model selection, Ollama support

### Issue: Zero-vector Fallback on Embedding Failure

**Problem:** Silently returns [0.0] * 1536 on API errors, causing poor search results

**Current Workaround:** Monitor OpenAI API status, ensure API key is valid

**Future:** Proper error handling with retries, user-facing errors

### Issue: File Scraper Not Fully Tested

**Problem:** May have bugs in edge cases

**Current Workaround:** Test thoroughly before using in production

**Future:** Add comprehensive tests, fix Playwright environment issues

### Issue: No Rate Limiting Support

**Problem:** May hit API rate limits on large crawls

**Current Workaround:**
- Reduce `max_concurrent` parameter
- Add manual delays if needed
- Monitor API usage

**Future:** Built-in rate limiting with exponential backoff

---

## Testing Guidelines (Future)

### Test Structure (Planned)

```
tests/
├── unit/
│   ├── test_utils.py
│   ├── test_chunking.py
│   └── test_file_scraper.py
├── integration/
│   ├── test_mcp_tools.py
│   ├── test_database.py
│   └── test_crawling.py
├── e2e/
│   └── test_full_workflow.py
└── fixtures/
    ├── sample_html.html
    ├── sample_sitemap.xml
    └── mock_responses.json
```

### Test Coverage Goals

- Unit tests: 80%+ coverage
- Integration tests: All MCP tools
- E2E tests: Common workflows

### Mocking Strategy

**Mock External Services:**
- Supabase client (use mock/fixture data)
- OpenAI API (use cached embeddings)
- Crawl4AI crawler (use sample HTML)

**Example:**
```python
@pytest.fixture
def mock_supabase_client():
    client = MagicMock()
    client.table().insert().execute.return_value = ...
    return client
```

---

## Security Considerations

### Environment Variables

- **NEVER commit `.env` file** (in `.gitignore`)
- Use `.env.example` as template
- Rotate keys regularly
- Use service role key for Supabase (not anon key)

### Database Security

- RLS policies in place (see `crawled_pages.sql`)
- Service key required for writes
- Read-only public access for searches

### Crawling Considerations

- Respect `robots.txt` (future feature)
- Rate limiting to avoid overwhelming sites
- User-agent identification (future feature)

### API Keys

- OpenAI API key has usage limits
- Monitor costs
- Set up usage alerts in OpenAI dashboard

---

## Performance Considerations

### Current Bottlenecks

1. **Embedding Generation** - OpenAI API calls (rate-limited)
2. **Crawling Speed** - Limited by `max_concurrent` and site responsiveness
3. **Database Inserts** - Batch size of 20 (adjustable)

### Optimization Tips

**For Large Crawls:**
- Increase `max_concurrent` (but watch memory)
- Increase batch size in `add_documents_to_supabase()`
- Use contextual embeddings selectively (slower but better results)

**For Better Search:**
- Use source filtering when possible
- Increase chunk size for longer context
- Adjust `match_count` based on needs

**Memory Management:**
- Uses MemoryAdaptiveDispatcher (Crawl4AI feature)
- Clear visited URLs set periodically on very large crawls
- Monitor container memory usage

---

## Integration Examples

### SSE Transport (Recommended)

**MCP Client Config:**
```json
{
  "mcpServers": {
    "crawl4ai-rag": {
      "transport": "sse",
      "url": "http://localhost:8051/sse"
    }
  }
}
```

**For Windsurf:**
```json
{
  "mcpServers": {
    "crawl4ai-rag": {
      "transport": "sse",
      "serverUrl": "http://localhost:8051/sse"
    }
  }
}
```

### Stdio Transport (Claude Desktop)

**Config:**
```json
{
  "mcpServers": {
    "crawl4ai-rag": {
      "command": "uv",
      "args": ["run", "src/crawl4ai_mcp.py"],
      "env": {
        "TRANSPORT": "stdio",
        "OPENAI_API_KEY": "sk-...",
        "SUPABASE_URL": "https://....supabase.co",
        "SUPABASE_SERVICE_KEY": "eyJ..."
      }
    }
  }
}
```

### Docker with Stdio

**Config:**
```json
{
  "mcpServers": {
    "crawl4ai-rag": {
      "command": "docker",
      "args": [
        "run", "--rm", "-i",
        "-e", "TRANSPORT",
        "-e", "OPENAI_API_KEY",
        "-e", "SUPABASE_URL",
        "-e", "SUPABASE_SERVICE_KEY",
        "mcp/crawl4ai-rag"
      ],
      "env": {
        "TRANSPORT": "stdio",
        "OPENAI_API_KEY": "sk-...",
        "SUPABASE_URL": "https://....supabase.co",
        "SUPABASE_SERVICE_KEY": "eyJ..."
      }
    }
  }
}
```

---

## Troubleshooting

### Server Won't Start

**Check:**
1. Environment variables set correctly
2. Supabase credentials valid
3. OpenAI API key valid
4. Port 8051 not in use
5. Docker running (if using Docker)

**Commands:**
```bash
# Check environment
cat .env

# Test Supabase connection
python -c "from utils import get_supabase_client; client = get_supabase_client(); print('Connected!')"

# Check port
lsof -i :8051  # Mac/Linux
netstat -ano | findstr :8051  # Windows
```

### Crawling Fails

**Check:**
1. URL is accessible
2. Site doesn't block bots
3. Playwright browsers installed (`crawl4ai-setup`)
4. Network connectivity

**Debug:**
```python
# Test crawler directly
from crawl4ai import AsyncWebCrawler
async with AsyncWebCrawler() as crawler:
    result = await crawler.arun("https://example.com")
    print(result.success)
```

### Embeddings Fail

**Check:**
1. OpenAI API key valid
2. API quota not exceeded
3. Network connectivity to OpenAI

**Workaround:**
- Current code falls back to zero-vectors (silent failure)
- Check logs for "Error creating embedding" messages

### Database Errors

**Check:**
1. SQL schema applied correctly
2. Supabase project active
3. Service key (not anon key) used
4. pgvector extension enabled

**Test:**
```bash
# Run a simple query
psql $DATABASE_URL -c "SELECT version();"
```

---

## Resources

### Documentation

- **MCP Spec:** https://modelcontextprotocol.io
- **FastMCP:** https://github.com/jlowin/fastmcp
- **Crawl4AI:** https://docs.crawl4ai.com
- **Supabase:** https://supabase.com/docs
- **pgvector:** https://github.com/pgvector/pgvector

### Related Projects

- **Archon:** https://github.com/coleam00/Archon
- **Mem0 MCP Template:** https://github.com/coleam00/mcp-mem0/

### Author

- **Cole Medin** - YouTube channel with MCP tutorials
- GitHub: https://github.com/coleam00

---

## Quick Reference

### Common Commands

```bash
# Install dependencies
uv pip install -e .
crawl4ai-setup

# Run server
uv run src/crawl4ai_mcp.py

# Build Docker
docker build -t mcp/crawl4ai-rag .

# Run Docker
docker run --env-file .env -p 8051:8051 mcp/crawl4ai-rag

# Future: Run tests
pytest
pytest --cov=src
```

### Environment Variables Cheatsheet

```bash
# Required
OPENAI_API_KEY=sk-...
SUPABASE_URL=https://....supabase.co
SUPABASE_SERVICE_KEY=eyJ...

# Optional
TRANSPORT=sse  # or stdio
HOST=0.0.0.0
PORT=8051
MODEL_CHOICE=gpt-4  # for contextual embeddings
```

### File Locations

- **Source code:** `src/`
- **Main server:** `src/crawl4ai_mcp.py`
- **Utilities:** `src/utils.py`
- **File scraper:** `src/file_scraper.py`
- **Database schema:** `crawled_pages.sql`
- **Dependencies:** `pyproject.toml`
- **Environment template:** `.env.example`
- **Your environment:** `.env` (create this, not in repo)

---

## Contribution Guidelines (Future)

**Current State:** No formal guidelines yet

**When Contributing:**
1. Follow existing code style
2. Add docstrings to new functions
3. Update CLAUDE.md if adding new patterns
4. Update README.md if changing user-facing features
5. Test manually before committing
6. Use conventional commit messages

**Future:**
- Add CONTRIBUTING.md
- Set up pre-commit hooks
- Require tests for new features
- Code review process

---

## Changelog

### November 17, 2025
- Created CLAUDE.md (this file)
- Created UPDATES.md (status report)
- Documented current state and conventions

### May 25, 2025
- Added file scraper feature (PR #1)
- `execute_site_scraping_to_files` tool added

### May 7, 2025
- Added contextual embeddings support
- Parallel contextual embedding generation

### Earlier
- Initial implementation with MCP tools
- Smart crawling, chunking, and RAG

---

## For AI Assistants: Best Practices

When working on this codebase:

1. **Read UPDATES.md first** - Understand current state and issues
2. **Check this file (CLAUDE.md)** - Follow established patterns
3. **Maintain async/await** - All I/O should be non-blocking
4. **Add type hints** - Python 3.12+ features expected
5. **Document changes** - Update this file if adding new patterns
6. **Manual testing** - No automated tests yet, test thoroughly
7. **Configuration** - Note hard-coded values for future refactoring
8. **Error handling** - Will be improved, but follow current patterns
9. **Commit messages** - Use conventional commits
10. **Ask before major changes** - Especially architecture changes

### When Asked to Implement Features:

1. Check UPDATES.md "What's Left to Do" section
2. Understand the priority level
3. Follow existing patterns in codebase
4. Consider backward compatibility
5. Update documentation (README, CLAUDE.md)
6. Test manually with MCP client
7. Note any new hard-coded values for future config

### When Debugging:

1. Check "Troubleshooting" section above
2. Look for `print()` statements in code
3. Test components individually
4. Check environment variables
5. Verify external services (Supabase, OpenAI)
6. Review recent changes in git history

---

**Last Updated:** November 17, 2025

**Status:** Living document - update as codebase evolves

**Maintained by:** AI assistants working on this codebase

**Questions?** Check README.md or UPDATES.md for additional context.
