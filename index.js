import { extension_settings } from '../../../extensions.js';
import { saveSettingsDebounced } from '../../../../script.js';

// FlowBridge / LongFlow: local streaming-reliability patch.
const EXT = 'LongFlow';
const MARK = Symbol.for('longflow.fetch-hook');
const DEF = { enabled: true, smoothDisplay: true, smoothChunkChars: 32, smoothDelayMs: 18 };
let originalFetch = null;
let wrappedFetch = null;
let lastOk = false;
let lastDetail = '还没有完成测试';

function cfg() {
    extension_settings[EXT] ??= {};
    for (const [k, v] of Object.entries(DEF)) {
        if (extension_settings[EXT][k] === undefined) extension_settings[EXT][k] = v;
    }
    return extension_settings[EXT];
}
function toast(k, m) { window.toastr?.[k]?.(m, 'LongFlow') ?? console.log('[LongFlow]', m); }
function name() {
    const a = new Uint8Array(12);
    crypto.getRandomValues(a);
    return 'lf_emit_' + [...a].map(x => x.toString(16).padStart(2, '0')).join('');
}
function clone(x) { return JSON.parse(JSON.stringify(x)); }
function inject(body) {
    const b = clone(body);
    if (!Array.isArray(b.messages)) return null;
    const n = name();
    const tool = {
        type: 'function',
        function: {
            name: n,
            description: 'Use this transport tool exactly once for the final user-visible answer. Put the complete final reply in content.',
            parameters: {
                type: 'object',
                properties: { content: { type: 'string' } },
                required: ['content'],
                additionalProperties: false,
            },
        },
    };
    b.tools = [...(Array.isArray(b.tools) ? b.tools : []), tool];
    if (b.tool_choice === undefined || b.tool_choice === 'none') b.tool_choice = 'auto';
    const last = [...b.messages].reverse().find(m => !['system', 'developer'].includes(m?.role))?.role;
    b.messages.push({
        role: (last === 'assistant' || last === 'model' || !last) ? 'user' : 'system',
        content: `For the final user-visible reply, call the tool \`${n}\` exactly once. Put the entire final reply in its content argument and do not output user-visible text outside that tool call.`,
    });
    return { body: b, tool: n, stream: b.stream === true };
}

/** Return complete JSON content, or recover characters from an interrupted JSON string. */
function parseArgs(source) {
    if (source && typeof source === 'object') {
        return { text: typeof source.content === 'string' ? source.content : '', complete: typeof source.content === 'string' };
    }
    const s = String(source ?? '');
    try {
        const obj = JSON.parse(s);
        if (typeof obj?.content === 'string') return { text: obj.content, complete: true };
    } catch { /* May be incomplete; recover only the content string. */ }

    const match = /"content"\s*:\s*"/.exec(s);
    if (!match) return { text: '', complete: false };
    let result = '';
    let escaped = false;
    const start = match.index + match[0].length;
    for (let i = start; i < s.length; i++) {
        const ch = s[i];
        if (escaped) {
            if (ch === 'u') {
                const hex = s.slice(i + 1, i + 5);
                if (/^[\da-fA-F]{4}$/.test(hex)) {
                    result += String.fromCharCode(parseInt(hex, 16));
                    i += 4;
                } else {
                    break; // Interrupted Unicode escape; leave its incomplete bytes out.
                }
            } else {
                result += ({ n: '\n', r: '\r', t: '\t', b: '\b', f: '\f', '"': '"', '\\': '\\', '/': '/' }[ch] ?? ch);
            }
            escaped = false;
        } else if (ch === '\\') {
            escaped = true;
        } else if (ch === '"') {
            break;
        } else {
            result += ch;
        }
    }
    // JSON.parse failed, so mark this as a recovery even if the quoted value ended.
    return { text: result, complete: false };
}
function outChunk(meta, text, finish = null) {
    return {
        id: meta.id || 'chatcmpl-longflow',
        object: 'chat.completion.chunk',
        created: meta.created || Math.floor(Date.now() / 1000),
        model: meta.model || '',
        choices: [{ index: 0, delta: text ? { content: text } : {}, finish_reason: finish }],
    };
}
function splitText(s, c) {
    if (!s) return [];
    if (!c.smoothDisplay) return [s];
    const size = Math.max(8, Math.min(256, Number(c.smoothChunkChars) || 32));
    const chars = Array.from(s); // Avoid splitting surrogate pairs / emoji.
    const chunks = [];
    for (let i = 0; i < chars.length; i += size) chunks.push(chars.slice(i, i + size).join(''));
    return chunks;
}
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

/** Incremental SSE parser: handles CR, LF, CRLF even across network chunks. */
function makeSseParser(onEvent) {
    let line = '';
    let dataLines = [];
    let skipLF = false;
    function flushLine() {
        if (line === '') {
            if (dataLines.length) onEvent(dataLines.join('\n'));
            dataLines = [];
        } else if (line.startsWith('data:')) {
            let value = line.slice(5);
            if (value.startsWith(' ')) value = value.slice(1);
            dataLines.push(value);
        }
        line = '';
    }
    return {
        push(text) {
            for (const ch of text) {
                if (skipLF) {
                    skipLF = false;
                    if (ch === '\n') continue;
                }
                if (ch === '\r' || ch === '\n') {
                    flushLine();
                    if (ch === '\r') skipLF = true;
                } else {
                    line += ch;
                }
            }
        },
        end() {
            if (line) flushLine();
            if (dataLines.length) onEvent(dataLines.join('\n'));
            dataLines = [];
        },
    };
}

async function streamResp(resp, tool, c) {
    if (!resp.body) return resp;
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    const encoder = new TextEncoder();
    const calls = new Map();
    let meta = {};
    let plain = '';
    let finishReason = null;
    let upstreamDone = false;
    let upstreamError = '';
    let sawTool = false;
    let readFailure = '';
    const queue = [];
    let prepared = null;

    function processEvent(data) {
        if (data.trim() === '[DONE]') {
            upstreamDone = true;
            return;
        }
        let j;
        try { j = JSON.parse(data); } catch { return; }
        if (j.error) {
            upstreamError = String(j.error.message || j.error.type || 'API returned an error').slice(0, 240);
            return;
        }
        if (j.id) meta.id = j.id;
        if (j.created) meta.created = j.created;
        if (j.model) meta.model = j.model;
        for (const choice of j.choices || []) {
            if (choice.finish_reason != null) finishReason = choice.finish_reason;
            const delta = choice.delta || {};
            if (typeof delta.content === 'string') plain += delta.content;
            for (const tc of delta.tool_calls || []) {
                sawTool = true;
                const key = `${choice.index ?? 0}:${tc.index ?? 0}`;
                const state = calls.get(key) || { name: '', args: '' };
                const fragment = tc.function?.name;
                if (typeof fragment === 'string' && fragment) {
                    // Some gateways stream fragments; others stream cumulative values.
                    if (!state.name) state.name = fragment;
                    else if (tool.startsWith(state.name + fragment)) state.name += fragment;
                    else if (fragment.startsWith(state.name)) state.name = fragment;
                    else state.name += fragment;
                }
                if (typeof tc.function?.arguments === 'string') state.args += tc.function.arguments;
                calls.set(key, state);
            }
        }
    }
    const parser = makeSseParser(processEvent);
    async function prepareOutput() {
        try {
            while (!upstreamDone) {
                const next = await reader.read();
                if (next.done) break;
                parser.push(decoder.decode(next.value, { stream: true }));
            }
            parser.push(decoder.decode());
            parser.end();
        } catch (e) {
            readFailure = String(e?.message || e);
            console.warn('[LongFlow] upstream stream interrupted:', e);
        } finally {
            if (upstreamDone) {
                try { await reader.cancel(); } catch { /* Already closed. */ }
            }
            try { reader.releaseLock(); } catch { /* Already released. */ }
        }
        const matching = [...calls.values()].find(x => x.name === tool);
        let text = '';
        let issue = '';
        if (matching) {
            const result = parseArgs(matching.args);
            text = result.text;
            if (!result.complete) issue = '工具参数未完整返回，已尽量恢复收到的文字';
            if (!text) issue = '目标工具调用没有可解析的 content 正文';
        } else if (plain) {
            text = plain;
            if (sawTool) issue = '目标工具未调用，已保留普通正文';
        } else {
            issue = sawTool ? '收到工具调用，但没有匹配 LongFlow 的目标工具' : '没有收到正文或工具调用';
        }
        if (upstreamError) issue = `上游接口错误：${upstreamError}`;
        else if (readFailure) issue = `流式连接读取中断：${readFailure.slice(0, 140)}`;
        else if (!upstreamDone && !finishReason) issue = issue ? `${issue}；上游缺少结束标记` : '上游缺少结束标记';

        // Preserve partial text; never silently return an empty success response.
        if (issue) {
            console.warn('[LongFlow]', issue, {
                upstreamFinish: finishReason, upstreamDone, sawTool,
                receivedToolCalls: calls.size, recoveredCharacters: text.length,
            });
            if (!text) text = `[LongFlow] 本次生成没有可显示的正文：${issue}。请检查浏览器控制台及 API 中转的流式工具调用支持。`;
            else if (matching && !parseArgs(matching.args).complete) text += '\n\n[LongFlow 提示：检测到工具参数未完整返回，以上为已恢复的部分正文。]';
        }
        lastOk = !!matching && !!text && !issue;
        lastDetail = issue || `工具调用成功，提取 ${text.length} 个字符`;
        let finish = finishReason === 'length' ? 'length' : finishReason === 'content_filter' ? 'content_filter' : 'stop';
        if (!upstreamDone && !finishReason && !issue) finish = 'length';
        const parts = splitText(text, c);
        for (let i = 0; i < parts.length; i++) {
            queue.push({ obj: outChunk(meta, parts[i]), delay: i && c.smoothDisplay ? Math.max(0, Number(c.smoothDelayMs) || 18) : 0 });
        }
        queue.push({ obj: outChunk(meta, '', finish), delay: 0 });
    }
    const stream = new ReadableStream({
        async pull(controller) {
            try {
                prepared ??= prepareOutput();
                await prepared;
                if (queue.length) {
                    const item = queue.shift();
                    if (item.delay) await sleep(item.delay);
                    controller.enqueue(encoder.encode(`data: ${JSON.stringify(item.obj)}\n\n`));
                } else {
                    controller.enqueue(encoder.encode('data: [DONE]\n\n'));
                    controller.close();
                }
            } catch (e) {
                console.error('[LongFlow] response transform failed:', e);
                controller.error(e);
            }
        },
        async cancel(reason) {
            try { await reader.cancel(reason); } catch { /* Ignore cancellation errors. */ }
        },
    });
    const headers = new Headers(resp.headers);
    headers.delete('content-length');
    headers.delete('content-encoding');
    headers.set('content-type', 'text/event-stream; charset=utf-8');
    return new Response(stream, { status: resp.status, statusText: resp.statusText, headers });
}

async function nonStream(resp, tool) {
    let j;
    try { j = await resp.clone().json(); } catch { return resp; }
    for (const ch of j.choices || []) {
        const calls = ch?.message?.tool_calls || [];
        const hit = calls.find(x => x?.function?.name === tool);
        if (hit) {
            const result = parseArgs(hit.function.arguments);
            ch.message.content = result.text || '[LongFlow] 目标工具调用未提供可解析的 content 正文。';
            ch.message.tool_calls = calls.filter(x => x !== hit);
            ch.finish_reason = ch.message.tool_calls.length ? 'tool_calls' : 'stop';
            lastOk = !!result.text && result.complete;
            lastDetail = lastOk ? `工具调用成功，提取 ${result.text.length} 个字符` : '工具参数缺失或未完整返回';
        }
    }
    const headers = new Headers(resp.headers);
    headers.delete('content-length');
    headers.delete('content-encoding');
    headers.set('content-type', 'application/json; charset=utf-8');
    return new Response(JSON.stringify(j), { status: resp.status, statusText: resp.statusText, headers });
}
function isTarget(input, init) {
    try {
        const url = new URL(typeof input === 'string' ? input : input.url, location.href);
        return url.pathname === '/api/backends/chat-completions/generate'
            && String(init?.method || 'GET').toUpperCase() === 'POST';
    } catch { return false; }
}
async function hooked(input, init) {
    const c = cfg();
    if (!c.enabled || !isTarget(input, init) || typeof init?.body !== 'string') {
        return originalFetch.call(window, input, init);
    }
    let body;
    try { body = JSON.parse(init.body); } catch { return originalFetch.call(window, input, init); }
    const injected = inject(body);
    if (!injected) return originalFetch.call(window, input, init);
    const resp = await originalFetch.call(window, input, { ...init, body: JSON.stringify(injected.body) });
    if (!resp.ok) return resp;
    return injected.stream ? streamResp(resp, injected.tool, c) : nonStream(resp, injected.tool);
}
function install() {
    if (window.fetch === wrappedFetch) return;
    if (window.fetch?.[MARK]) return;
    originalFetch = window.fetch;
    wrappedFetch = (input, init) => hooked(input, init);
    Object.defineProperty(wrappedFetch, MARK, { value: true });
    window.fetch = wrappedFetch;
}
function render() {
    if (document.getElementById('longflow_panel')) return;
    const host = document.querySelector('#extensions_settings2') || document.querySelector('#extensions_settings');
    if (!host) return;
    const c = cfg();
    const d = document.createElement('div');
    d.id = 'longflow_panel';
    d.innerHTML = '<div class="inline-drawer"><div class="inline-drawer-toggle inline-drawer-header"><b>LongFlow</b></div><div class="inline-drawer-content longflow-content"><label class="checkbox_label"><input id="longflow_enabled" type="checkbox"><span id="longflow_enabled_text"></span></label><label class="checkbox_label"><input id="longflow_smooth" type="checkbox"><span>平滑显示</span></label><button id="longflow_test" class="menu_button">测试</button></div></div>';
    host.appendChild(d);
    const enabled = d.querySelector('#longflow_enabled');
    const smooth = d.querySelector('#longflow_smooth');
    const label = d.querySelector('#longflow_enabled_text');
    enabled.checked = !!c.enabled;
    smooth.checked = !!c.smoothDisplay;
    const updateLabel = () => { label.textContent = enabled.checked ? '已启动' : '已停用'; };
    updateLabel();
    enabled.oninput = () => { c.enabled = enabled.checked; updateLabel(); saveSettingsDebounced(); };
    smooth.oninput = () => { c.smoothDisplay = smooth.checked; saveSettingsDebounced(); };
    d.querySelector('#longflow_test').onclick = () => toast(lastOk ? 'success' : 'info', lastDetail);
}
export async function init() { cfg(); install(); render(); console.log('[LongFlow] streaming reliability patch loaded'); }
export async function onEnable() { cfg().enabled = true; install(); }
export async function onDisable() {
    cfg().enabled = false;
    if (wrappedFetch && window.fetch === wrappedFetch && originalFetch) {
        window.fetch = originalFetch;
        wrappedFetch = null;
        originalFetch = null;
    }
}
if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init, { once: true });
else queueMicrotask(init);
