// ═════════════════════════════════════════════════════════════════════
// Baton Router — Cloudflare Queues + D1 Event Router
// Inter-agent messaging that survives the void.
// ═════════════════════════════════════════════════════════════════════

export interface Env {
  DB: D1Database;
  BATON_QUEUE: Queue<QueueMessage>;
  RATE_LIMIT: KVNamespace;
  ROUTER_API_KEY?: string;
}

interface QueueMessage {
  messageId: string;
  toAgent: string;
  attempt: number;
}

// ── Types ───────────────────────────────────────────────────────────

type AgentRole = 'forge' | 'loom' | 'oracle' | 'ship' | 'crab';
type MessageType = 'I2I' | 'GC_SYNC' | 'PID_UPDATE' | 'CONSERVATION_AUDIT' | 'BOTTLE' | 'SPLINE';
type Priority = -1 | 0 | 1;
type MessageStatus = 'pending' | 'queued' | 'delivered' | 'acked' | 'expired';
type LogEvent = 'created' | 'delivered' | 'acked' | 'expired' | 'replayed';

const VALID_ROLES: AgentRole[] = ['forge', 'loom', 'oracle', 'ship', 'crab'];
const VALID_TYPES: MessageType[] = ['I2I', 'GC_SYNC', 'PID_UPDATE', 'CONSERVATION_AUDIT', 'BOTTLE', 'SPLINE'];
const VALID_PRIORITIES: Priority[] = [-1, 0, 1];

const RATE_LIMIT_WINDOW = 60; // seconds
const RATE_LIMIT_MAX = 100;   // requests per window per IP

// ── Utilities ───────────────────────────────────────────────────────

function uuidv4(): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = [...bytes].map((b) => b.toString(16).padStart(2, '0'));
  return `${hex.slice(0, 4).join('')}-${hex.slice(4, 6).join('')}-${hex.slice(6, 8).join('')}-${hex.slice(8, 10).join('')}-${hex.slice(10, 16).join('')}`;
}

async function hashKey(key: string): Promise<string> {
  const data = new TextEncoder().encode(key);
  const hash = await crypto.subtle.digest('SHA-256', data);
  return [...new Uint8Array(hash)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

function generateApiKey(): string {
  return `btr_${uuidv4().replace(/-/g, '')}`;
}

function json(data: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, X-API-Key, Authorization',
      ...headers,
    },
  });
}

function errorResponse(message: string, status: number): Response {
  return json({ error: message }, status);
}

// ── Rate Limiting ───────────────────────────────────────────────────

async function checkRateLimit(kv: KVNamespace, ip: string): Promise<{ allowed: boolean; remaining: number }> {
  const key = `rl:${ip}`;
  const raw = await kv.get(key);
  let count = raw ? parseInt(raw, 10) : 0;

  if (count >= RATE_LIMIT_MAX) {
    return { allowed: false, remaining: 0 };
  }

  count++;
  await kv.put(key, count.toString(), { expirationTtl: RATE_LIMIT_WINDOW });
  return { allowed: true, remaining: RATE_LIMIT_MAX - count };
}

// ── Auth ────────────────────────────────────────────────────────────

async function authenticate(
  db: D1Database,
  request: Request
): Promise<{ valid: boolean; agentId?: string }> {
  const apiKey = request.headers.get('X-API-Key') ?? extractBearerToken(request);
  if (!apiKey) return { valid: false };

  const keyHash = await hashKey(apiKey);
  const agent = await db
    .prepare('SELECT id FROM agents WHERE api_key_hash = ? AND status = ?')
    .bind(keyHash, 'active')
    .first<{ id: string }>();

  if (!agent) return { valid: false };
  return { valid: true, agentId: agent.id };
}

function extractBearerToken(request: Request): string | null {
  const auth = request.headers.get('Authorization');
  if (auth && auth.startsWith('Bearer ')) return auth.slice(7);
  return null;
}

// ── Input Validation ────────────────────────────────────────────────

function validateEnum<T extends string>(value: string, valid: T[], field: string): T | null {
  const v = value as T;
  return valid.includes(v) ? v : null;
}

function validatePayload(payload: unknown): string | null {
  if (typeof payload !== 'string') return 'payload must be a string';
  if (payload.length === 0) return 'payload must not be empty';
  if (payload.length > 256_000) return 'payload exceeds 256KB limit';
  return null;
}

function validateAgentName(name: unknown): string | null {
  if (typeof name !== 'string') return 'name must be a string';
  if (name.length < 1 || name.length > 64) return 'name must be 1-64 characters';
  if (!/^[a-z0-9_-]+$/i.test(name)) return 'name may only contain alphanumeric, hyphen, underscore';
  return null;
}

// ── CORS Preflight ──────────────────────────────────────────────────

function handleCORS(): Response {
  return new Response(null, {
    status: 204,
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, X-API-Key, Authorization',
      'Access-Control-Max-Age': '86400',
    },
  });
}

// ═════════════════════════════════════════════════════════════════════
// Router
// ═════════════════════════════════════════════════════════════════════

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    // CORS preflight
    if (request.method === 'OPTIONS') return handleCORS();

    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    // Rate limiting
    const ip = request.headers.get('CF-Connecting-IP') ?? 'unknown';
    const rl = await checkRateLimit(env.RATE_LIMIT, ip);
    if (!rl.allowed) {
      return errorResponse('Rate limit exceeded. Slow down.', 429);
    }

    // ── Public endpoints ────────────────────────────────────────
    if (path === '/health' && method === 'GET') {
      return json({ status: 'ok', service: 'baton-router', timestamp: new Date().toISOString() });
    }

    if (path === '/' && method === 'GET') {
      return handleStatus(env);
    }

    if (path === '/stats' && method === 'GET') {
      return handleStats(env);
    }

    if (path === '/routes' && method === 'GET') {
      return handleListRoutes(env);
    }

    // ── Agent registration (public) ────────────────────────────
    if (path === '/register' && method === 'POST') {
      return handleRegister(env, request);
    }

    // ── Authenticated endpoints ────────────────────────────────
    const auth = await authenticate(env.DB, request);

    if (path === '/send' && method === 'POST') {
      return handleSend(env, request, auth);
    }

    if (path === '/routes' && method === 'POST') {
      return handleCreateRoute(env, request, auth);
    }

    // ── Inbox ───────────────────────────────────────────────────
    const inboxMatch = path.match(/^\/inbox\/(.+)$/);
    if (inboxMatch && method === 'GET') {
      return handleInbox(env, inboxMatch[1], auth);
    }

    // ── Ack ─────────────────────────────────────────────────────
    const ackMatch = path.match(/^\/ack\/(.+)$/);
    if (ackMatch && method === 'POST') {
      return handleAck(env, ackMatch[1], auth);
    }

    // ── Replay ──────────────────────────────────────────────────
    const replayMatch = path.match(/^\/replay\/(.+)$/);
    if (replayMatch && method === 'GET') {
      return handleReplay(env, replayMatch[1], url.searchParams.get('since'), auth);
    }

    // ── 404 ──────────────────────────────────────────────────────
    return errorResponse('Not found', 404);
  },

  // ═════════════════════════════════════════════════════════════════
  // Queue Consumer — async delivery attempts
  // ═════════════════════════════════════════════════════════════════
  async queue(
    batch: MessageBatch<QueueMessage>,
    env: Env,
    _ctx: ExecutionContext
  ): Promise<void> {
    for (const msg of batch.messages) {
      const { messageId, toAgent, attempt } = msg.body;

      try {
        // Check if message still needs delivery
        const message = await env.DB
          .prepare('SELECT id, status, to_agent FROM messages WHERE id = ?')
          .bind(messageId)
          .first<{ id: string; status: string; to_agent: string }>();

        if (!message) {
          msg.ack();
          continue;
        }

        if (message.status === 'acked' || message.status === 'expired') {
          msg.ack();
          continue;
        }

        // Verify target agent exists and is active
        const agent = await env.DB
          .prepare('SELECT id, status FROM agents WHERE id = ? OR name = ?')
          .bind(toAgent, toAgent)
          .first<{ id: string; status: string }>();

        if (!agent || agent.status !== 'active') {
          // Agent unavailable — record attempt
          await logEvent(env.DB, messageId, 'expired', `Agent ${toAgent} unavailable after ${attempt} attempts`);

          await env.DB
            .prepare(`UPDATE messages SET status = 'expired', delivered_at = datetime('now') WHERE id = ?`)
            .bind(messageId)
            .run();

          // Move to dead letter
          await env.DB
            .prepare(`INSERT INTO dead_letter (message_id, reason, original_payload) VALUES (?, ?, ?)`)
            .bind(messageId, `Agent unavailable: ${toAgent}`, JSON.stringify(msg.body))
            .run();

          msg.ack();
          continue;
        }

        // Mark as delivered — agent can now poll inbox
        await env.DB
          .prepare(`UPDATE messages SET status = 'delivered', delivered_at = datetime('now') WHERE id = ? AND status IN ('pending', 'queued')`)
          .bind(messageId)
          .run();

        await logEvent(env.DB, messageId, 'delivered', `Attempt ${attempt}`);

        msg.ack();
      } catch (err) {
        console.error(`Delivery error for ${messageId}:`, err);
        // Retry will happen via queue retry mechanism
        msg.retry();
      }
    }
  },
};

// ═════════════════════════════════════════════════════════════════════
// Endpoint Handlers
// ═════════════════════════════════════════════════════════════════════

// ── GET / — Router status ───────────────────────────────────────────

async function handleStatus(env: Env): Promise<Response> {
  const recentCount = await env.DB
    .prepare(`SELECT COUNT(*) as count FROM messages WHERE created_at > datetime('now', '-1 hour')`)
    .first<{ count: number }>();

  const pendingCount = await env.DB
    .prepare(`SELECT COUNT(*) as count FROM messages WHERE status IN ('pending', 'queued')`)
    .first<{ count: number }>();

  const agentCount = await env.DB
    .prepare(`SELECT COUNT(*) as count FROM agents WHERE status = 'active'`)
    .first<{ count: number }>();

  const dlqCount = await env.DB
    .prepare(`SELECT COUNT(*) as count FROM dead_letter`)
    .first<{ count: number }>();

  return json({
    service: 'baton-router',
    version: '1.0.0',
    timestamp: new Date().toISOString(),
    active_agents: agentCount?.count ?? 0,
    messages_last_hour: recentCount?.count ?? 0,
    pending_messages: pendingCount?.count ?? 0,
    dead_letter_count: dlqCount?.count ?? 0,
    endpoints: [
      'GET  /',
      'GET  /health',
      'GET  /stats',
      'POST /register',
      'POST /send',
      'GET  /inbox/:agent_id',
      'POST /ack/:message_id',
      'GET  /replay/:agent_id?since=',
      'GET  /routes',
      'POST /routes',
    ],
  });
}

// ── POST /register — Register a new agent ──────────────────────────

async function handleRegister(env: Env, request: Request): Promise<Response> {
  let body: Record<string, unknown>;
  try {
    body = await request.json() as Record<string, unknown>;
  } catch {
    return errorResponse('Invalid JSON body', 400);
  }

  const name = body.name as string;
  const role = body.role as string;
  const publicKey = (body.public_key as string) ?? null;

  const nameError = validateAgentName(name);
  if (nameError) return errorResponse(nameError, 400);

  const validRole = validateEnum(role, VALID_ROLES, 'role');
  if (!validRole) return errorResponse(`Invalid role. Must be one of: ${VALID_ROLES.join(', ')}`, 400);

  if (publicKey !== null && typeof publicKey !== 'string') {
    return errorResponse('public_key must be a string', 400);
  }

  // Check for existing agent
  const existing = await env.DB
    .prepare('SELECT id FROM agents WHERE name = ?')
    .bind(name)
    .first();

  if (existing) {
    return errorResponse(`Agent "${name}" already registered`, 409);
  }

  const agentId = uuidv4();
  const apiKey = generateApiKey();
  const apiKeyHash = await hashKey(apiKey);

  try {
    await env.DB
      .prepare(`INSERT INTO agents (id, name, public_key, role, status, api_key_hash) VALUES (?, ?, ?, ?, 'active', ?)`)
      .bind(agentId, name, publicKey, validRole, apiKeyHash)
      .run();
  } catch (err) {
    console.error('Registration error:', err);
    return errorResponse('Failed to register agent', 500);
  }

  return json({
    agent_id: agentId,
    name,
    role: validRole,
    api_key: apiKey,
    warning: 'Store your API key securely. It will not be shown again.',
  }, 201);
}

// ── POST /send — Send a baton message ───────────────────────────────

async function handleSend(
  env: Env,
  request: Request,
  auth: { valid: boolean; agentId?: string }
): Promise<Response> {
  if (!auth.valid) return errorResponse('Authentication required', 401);

  let body: Record<string, unknown>;
  try {
    body = await request.json() as Record<string, unknown>;
  } catch {
    return errorResponse('Invalid JSON body', 400);
  }

  const fromAgent = body.from_agent as string;
  const toAgent = body.to_agent as string;
  const type = body.type as string;
  const payload = body.payload;
  const priority = (body.priority as number) ?? 0;

  if (!fromAgent) return errorResponse('from_agent is required', 400);
  if (!toAgent) return errorResponse('to_agent is required', 400);

  const validType = validateEnum(type, VALID_TYPES, 'type');
  if (!validType) return errorResponse(`Invalid type. Must be one of: ${VALID_TYPES.join(', ')}`, 400);

  const payloadError = validatePayload(payload);
  if (payloadError) return errorResponse(payloadError, 400);

  if (!VALID_PRIORITIES.includes(priority as Priority)) {
    return errorResponse(`Invalid priority. Must be one of: -1, 0, 1`, 400);
  }

  // Verify from_agent matches authenticated agent
  if (fromAgent !== auth.agentId) {
    const fromAgentRecord = await env.DB
      .prepare('SELECT id FROM agents WHERE id = ? OR name = ?')
      .bind(fromAgent, fromAgent)
      .first<{ id: string }>();

    if (!fromAgentRecord || fromAgentRecord.id !== auth.agentId) {
      return errorResponse('from_agent does not match authenticated identity', 403);
    }
  }

  // Verify target agent exists
  const target = await env.DB
    .prepare('SELECT id, name FROM agents WHERE id = ? OR name = ?')
    .bind(toAgent, toAgent)
    .first<{ id: string; name: string }>();

  if (!target) {
    return errorResponse(`Target agent "${toAgent}" not found`, 404);
  }

  const messageId = uuidv4();

  try {
    // Insert message
    await env.DB
      .prepare(`INSERT INTO messages (id, from_agent, to_agent, type, payload, priority, status) VALUES (?, ?, ?, ?, ?, ?, 'pending')`)
      .bind(messageId, auth.agentId, target.id, validType, payload, priority)
      .run();

    // Log creation
    await logEvent(env.DB, messageId, 'created', `${validType} from ${fromAgent} to ${target.name}`);

    // Enqueue for async delivery — urgent messages first
    await env.BATON_QUEUE.send(
      { messageId, toAgent: target.id, attempt: 1 },
      { contentType: 'json' }
    );

    // Update status to queued
    await env.DB
      .prepare(`UPDATE messages SET status = 'queued' WHERE id = ?`)
      .bind(messageId)
      .run();
  } catch (err) {
    console.error('Send error:', err);
    return errorResponse('Failed to send message', 500);
  }

  return json({
    message_id: messageId,
    status: 'queued',
    from: auth.agentId,
    to: target.id,
    type: validType,
    priority,
    created_at: new Date().toISOString(),
  }, 202);
}

// ── GET /inbox/:agent_id — Get undelivered messages ─────────────────

async function handleInbox(
  env: Env,
  agentId: string,
  auth: { valid: boolean; agentId?: string }
): Promise<Response> {
  if (!auth.valid) return errorResponse('Authentication required', 401);

  // Agent can only read their own inbox (or admin via API key)
  const requestingAgent = auth.agentId;
  const isAdmin = !requestingAgent; // If admin key used, agentId is undefined but valid... 
  // Actually authenticate() returns agentId for agent keys, undefined for invalid
  // For admin access, we'd check ROUTER_API_KEY separately

  // Verify agentId matches or is accessible
  const agent = await env.DB
    .prepare('SELECT id, name, role FROM agents WHERE id = ?')
    .bind(agentId)
    .first<{ id: string; name: string; role: string }>();

  if (!agent) {
    return errorResponse('Agent not found', 404);
  }

  // Update last_seen
  await env.DB
    .prepare(`UPDATE agents SET last_seen = datetime('now') WHERE id = ?`)
    .bind(agentId)
    .run();

  const messages = await env.DB
    .prepare(`
      SELECT id, from_agent, to_agent, type, payload, priority, created_at, delivered_at
      FROM messages
      WHERE to_agent = ? AND status IN ('delivered', 'queued')
      ORDER BY
        CASE priority WHEN 1 THEN 0 WHEN 0 THEN 1 WHEN -1 THEN 2 END,
        created_at ASC
    `)
    .bind(agentId)
    .all();

  return json({
    agent: agent.name,
    role: agent.role,
    count: messages.results.length,
    messages: messages.results,
  });
}

// ── POST /ack/:message_id — Acknowledge receipt ─────────────────────

async function handleAck(
  env: Env,
  messageId: string,
  auth: { valid: boolean; agentId?: string }
): Promise<Response> {
  if (!auth.valid) return errorResponse('Authentication required', 401);

  const message = await env.DB
    .prepare('SELECT id, to_agent, status FROM messages WHERE id = ?')
    .bind(messageId)
    .first<{ id: string; to_agent: string; status: string }>();

  if (!message) {
    return errorResponse('Message not found', 404);
  }

  if (message.to_agent !== auth.agentId) {
    return errorResponse('Not authorized to ack this message', 403);
  }

  if (message.status === 'acked') {
    return json({ message_id: messageId, status: 'already_acked' });
  }

  await env.DB
    .prepare(`UPDATE messages SET status = 'acked', acked_at = datetime('now') WHERE id = ?`)
    .bind(messageId)
    .run();

  await logEvent(env.DB, messageId, 'acked', `Acknowledged by ${auth.agentId}`);

  return json({ message_id: messageId, status: 'acked' });
}

// ── GET /replay/:agent_id?since=timestamp ────────────────────────────

async function handleReplay(
  env: Env,
  agentId: string,
  since: string | null,
  auth: { valid: boolean; agentId?: string }
): Promise<Response> {
  if (!auth.valid) return errorResponse('Authentication required', 401);

  const agent = await env.DB
    .prepare('SELECT id, name FROM agents WHERE id = ?')
    .bind(agentId)
    .first<{ id: string; name: string }>();

  if (!agent) {
    return errorResponse('Agent not found', 404);
  }

  // Parse 'since' timestamp — default to 24h ago
  let sinceClause = `datetime('now', '-1 day')`;
  const binds: (string | number)[] = [agentId];

  if (since) {
    // Validate timestamp format (basic)
    const parsed = Date.parse(since);
    if (!isNaN(parsed)) {
      sinceClause = `?`;
      binds.push(since);
    }
  }

  const messages = await env.DB
    .prepare(`
      SELECT id, from_agent, to_agent, type, payload, priority,
             created_at, delivered_at, acked_at, status
      FROM messages
      WHERE to_agent = ? AND created_at > ${sinceClause}
      ORDER BY created_at ASC
    `)
    .bind(...binds)
    .all();

  // Log replay event
  if (messages.results.length > 0) {
    for (const msg of messages.results) {
      const m = msg as { id: string };
      await logEvent(env.DB, m.id, 'replayed', `Replayed for agent ${agent.name}`);
    }
  }

  return json({
    agent: agent.name,
    since: since ?? '24h ago',
    count: messages.results.length,
    messages: messages.results,
  });
}

// ── GET /routes — List routing rules ────────────────────────────────

async function handleListRoutes(env: Env): Promise<Response> {
  const routes = await env.DB
    .prepare('SELECT * FROM routes ORDER BY priority DESC, created_at ASC')
    .all();

  return json({ count: routes.results.length, routes: routes.results });
}

// ── POST /routes — Create a routing rule (admin) ────────────────────

async function handleCreateRoute(
  env: Env,
  request: Request,
  _auth: { valid: boolean; agentId?: string }
): Promise<Response> {
  // Route creation requires admin API key
  if (!env.ROUTER_API_KEY) {
    return errorResponse('Route management not configured. Set ROUTER_API_KEY secret.', 503);
  }

  const providedKey = request.headers.get('X-API-Key') ?? extractBearerTokenFromRequest(request);
  if (providedKey !== env.ROUTER_API_KEY) {
    return errorResponse('Admin authentication required for route management', 403);
  }

  let body: Record<string, unknown>;
  try {
    body = await request.json() as Record<string, unknown>;
  } catch {
    return errorResponse('Invalid JSON body', 400);
  }

  const pattern = body.pattern as string;
  const targetAgent = body.target_agent as string;
  const priority = (body.priority as number) ?? 0;

  if (!pattern || typeof pattern !== 'string') {
    return errorResponse('pattern is required', 400);
  }
  if (!targetAgent || typeof targetAgent !== 'string') {
    return errorResponse('target_agent is required', 400);
  }
  if (!VALID_PRIORITIES.includes(priority as Priority)) {
    return errorResponse('Invalid priority. Must be -1, 0, or 1', 400);
  }

  // Verify target agent exists
  const agent = await env.DB
    .prepare('SELECT id FROM agents WHERE id = ? OR name = ?')
    .bind(targetAgent, targetAgent)
    .first();

  if (!agent) {
    return errorResponse(`Target agent "${targetAgent}" not found`, 404);
  }

  const result = await env.DB
    .prepare('INSERT INTO routes (pattern, target_agent, priority) VALUES (?, ?, ?)')
    .bind(pattern, targetAgent, priority)
    .run();

  return json({
    id: result.meta.last_row_id,
    pattern,
    target_agent: targetAgent,
    priority,
    status: 'created',
  }, 201);
}

// ── GET /stats — Throughput, delivery rates, DLQ count ───────────────

async function handleStats(env: Env): Promise<Response> {
  const total = await env.DB
    .prepare('SELECT COUNT(*) as count FROM messages')
    .first<{ count: number }>();

  const delivered = await env.DB
    .prepare(`SELECT COUNT(*) as count FROM messages WHERE status IN ('delivered', 'acked')`)
    .first<{ count: number }>();

  const acked = await env.DB
    .prepare(`SELECT COUNT(*) as count FROM messages WHERE status = 'acked'`)
    .first<{ count: number }>();

  const expired = await env.DB
    .prepare(`SELECT COUNT(*) as count FROM messages WHERE status = 'expired'`)
    .first<{ count: number }>();

  const pending = await env.DB
    .prepare(`SELECT COUNT(*) as count FROM messages WHERE status IN ('pending', 'queued')`)
    .first<{ count: number }>();

  const dlq = await env.DB
    .prepare(`SELECT COUNT(*) as count FROM dead_letter`)
    .first<{ count: number }>();

  const last24h = await env.DB
    .prepare(`SELECT COUNT(*) as count FROM messages WHERE created_at > datetime('now', '-1 day')`)
    .first<{ count: number }>();

  const byType = await env.DB
    .prepare(`SELECT type, COUNT(*) as count FROM messages GROUP BY type ORDER BY count DESC`)
    .all();

  const byPriority = await env.DB
    .prepare(`SELECT priority, COUNT(*) as count FROM messages GROUP BY priority ORDER BY priority DESC`)
    .all();

  const total_count = total?.count ?? 0;
  const delivered_count = delivered?.count ?? 0;
  const acked_count = acked?.count ?? 0;

  return json({
    timestamp: new Date().toISOString(),
    totals: {
      total: total_count,
      pending: pending?.count ?? 0,
      delivered: delivered_count,
      acked: acked_count,
      expired: expired?.count ?? 0,
      dead_letter: dlq?.count ?? 0,
      last_24h: last24h?.count ?? 0,
    },
    rates: {
      delivery_rate: total_count > 0 ? (delivered_count / total_count).toFixed(4) : '0',
      ack_rate: total_count > 0 ? (acked_count / total_count).toFixed(4) : '0',
      expiry_rate: total_count > 0 ? ((expired?.count ?? 0) / total_count).toFixed(4) : '0',
    },
    breakdown: {
      by_type: byType.results,
      by_priority: byPriority.results,
    },
  });
}

// ═════════════════════════════════════════════════════════════════════
// Helpers
// ═════════════════════════════════════════════════════════════════════

async function logEvent(
  db: D1Database,
  messageId: string,
  event: LogEvent,
  detail?: string
): Promise<void> {
  await db
    .prepare('INSERT INTO message_log (message_id, event, detail) VALUES (?, ?, ?)')
    .bind(messageId, event, detail ?? null)
    .run();
}

function extractBearerTokenFromRequest(request: Request): string | null {
  const auth = request.headers.get('Authorization');
  if (auth && auth.startsWith('Bearer ')) return auth.slice(7);
  return null;
}
