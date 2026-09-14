const API_BASE = "";

const form = document.querySelector("#chat-form");
const input = document.querySelector("#message-input");
const messages = document.querySelector("#messages");
const state = document.querySelector("#state");
const sendButton = document.querySelector("#send-button");
const sessionList = document.querySelector("#session-list");
const newSessionButton = document.querySelector("#new-session-button");
const homeButton = document.querySelector("#home-button");
const sessionTitle = document.querySelector("#session-title");

const STORAGE_KEY = "doridangBotSessions";
const MAX_SESSIONS = 40;
const SUGGESTED_PROMPTS = [
  {label: "팀 전체", prompt: "팀원 별 프로젝트 진행상황"},
  {label: "리스크", prompt: "이번에 리더가 확인해야 할 위험 업무만 알려줘"},
  {label: "개인 담당", prompt: "차보령 대리는?"},
];

let pendingRequest = null;
let sessions = loadSessions();
const initialSessionId = new URLSearchParams(window.location.search).get("session");
let activeSessionId = sessions.some((session) => session.id === initialSessionId)
  ? initialSessionId
  : sessions[0]?.id || createSession().id;

function appendMessage(role, text = "") {
  const item = document.createElement("article");
  item.className = `message ${role}`;
  const bubble = document.createElement("div");
  bubble.className = "bubble";
  setBubbleContent(bubble, role, text);
  item.appendChild(bubble);
  messages.appendChild(item);
  messages.scrollTop = messages.scrollHeight;
  return bubble;
}

function setBubbleContent(bubble, role, text) {
  bubble.dataset.raw = text;
  if (role === "assistant") {
    bubble.classList.add("markdown");
    bubble.innerHTML = renderMarkdown(text);
  } else {
    bubble.textContent = text;
  }
}

function appendAssistantText(bubble, text) {
  const next = `${bubble.dataset.raw || ""}${text}`;
  setBubbleContent(bubble, "assistant", next);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function inlineMarkdown(value) {
  return escapeHtml(value)
    .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
    .replace(/(^|\s)_([^_\n]+)_(?=\s|$)/g, "$1<em>$2</em>")
    .replace(/\[([^\]]+)\]\((https?:\/\/[^)\s]+)\)/g, '<a href="$2" target="_blank" rel="noopener noreferrer">$1</a>')
    .replace(/`(.+?)`/g, "<code>$1</code>");
}

function renderMarkdown(text) {
  const lines = String(text || "").split(/\r?\n/);
  const html = [];
  let index = 0;

  while (index < lines.length) {
    const line = lines[index];
    const trimmed = line.trim();

    if (!trimmed) {
      index += 1;
      continue;
    }

    if (/^\|.+\|$/.test(trimmed) && index + 1 < lines.length && /^\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|?$/.test(lines[index + 1].trim())) {
      const headers = splitTableRow(trimmed);
      index += 2;
      const rows = [];
      while (index < lines.length && /^\|.+\|$/.test(lines[index].trim())) {
        rows.push(splitTableRow(lines[index].trim()));
        index += 1;
      }
      html.push(renderTable(headers, rows));
      continue;
    }

    if (/^#{1,3}\s+/.test(trimmed)) {
      const level = Math.min(trimmed.match(/^#+/)[0].length, 3);
      html.push(`<h${level}>${inlineMarkdown(trimmed.replace(/^#{1,3}\s+/, ""))}</h${level}>`);
      index += 1;
      continue;
    }

    if (/^[-*]\s+/.test(trimmed)) {
      const items = [];
      while (index < lines.length && /^[-*]\s+/.test(lines[index].trim())) {
        items.push(lines[index].trim().replace(/^[-*]\s+/, ""));
        index += 1;
      }
      html.push(`<ul>${items.map((item) => `<li>${inlineMarkdown(item)}</li>`).join("")}</ul>`);
      continue;
    }

    if (/^\d+\.\s+/.test(trimmed)) {
      const items = [];
      while (index < lines.length && /^\d+\.\s+/.test(lines[index].trim())) {
        items.push(lines[index].trim().replace(/^\d+\.\s+/, ""));
        index += 1;
      }
      html.push(`<ol>${items.map((item) => `<li>${inlineMarkdown(item)}</li>`).join("")}</ol>`);
      continue;
    }

    const paragraph = [];
    while (
      index < lines.length
      && lines[index].trim()
      && !/^[-*]\s+/.test(lines[index].trim())
      && !/^\d+\.\s+/.test(lines[index].trim())
      && !/^#{1,3}\s+/.test(lines[index].trim())
      && !/^\|.+\|$/.test(lines[index].trim())
    ) {
      paragraph.push(lines[index].trim());
      index += 1;
    }
    if (!paragraph.length) {
      paragraph.push(lines[index]);
      index += 1;
    }
    html.push(`<p>${paragraph.map(inlineMarkdown).join("<br>")}</p>`);
  }

  return html.join("");
}

function splitTableRow(row) {
  return row.replace(/^\|/, "").replace(/\|$/, "").split("|").map((cell) => cell.trim());
}

function renderTable(headers, rows) {
  const head = headers.map((cell) => `<th>${inlineMarkdown(cell)}</th>`).join("");
  const body = rows
    .map((row) => `<tr>${row.map((cell) => `<td>${inlineMarkdown(cell)}</td>`).join("")}</tr>`)
    .join("");
  return `<div class="table-wrap"><table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}

function loadSessions() {
  try {
    const parsed = JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]");
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function saveSessions() {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(sessions.slice(0, MAX_SESSIONS)));
}

function createSession() {
  const session = {
    id: String(Date.now()),
    title: "새 대화",
    updatedAt: Date.now(),
    messages: [],
  };
  sessions.unshift(session);
  saveSessions();
  return session;
}

function activeSession() {
  let session = sessions.find((item) => item.id === activeSessionId);
  if (!session) {
    session = createSession();
    activeSessionId = session.id;
  }
  return session;
}

function sessionUrl(sessionId) {
  const url = new URL(window.location.href);
  url.searchParams.set("session", sessionId);
  return `${url.pathname}${url.search}${url.hash}`;
}

function setActiveSession(sessionId, {push = true, focus = false} = {}) {
  if (!sessions.some((session) => session.id === sessionId)) return;
  activeSessionId = sessionId;
  renderActiveSession();
  renderSessions();
  const stateValue = {sessionId: activeSessionId};
  const nextUrl = sessionUrl(activeSessionId);
  if (push) {
    if (history.state?.sessionId === activeSessionId) {
      history.replaceState(stateValue, "", nextUrl);
    } else {
      history.pushState(stateValue, "", nextUrl);
    }
  } else {
    history.replaceState(stateValue, "", nextUrl);
  }
  if (focus) input.focus();
}

function titleFrom(text) {
  const compact = text.replace(/\s+/g, " ").trim();
  return compact.length > 24 ? `${compact.slice(0, 24)}...` : compact || "새 대화";
}

function renderSessions() {
  sessionList.textContent = "";
  sessions.forEach((session) => {
    const row = document.createElement("div");
    row.className = `session-row${session.id === activeSessionId ? " active" : ""}`;

    const button = document.createElement("button");
    button.className = "session-button";
    button.type = "button";
    button.textContent = session.title || "새 대화";
    button.addEventListener("click", () => {
      setActiveSession(session.id);
    });

    const rename = document.createElement("button");
    rename.className = "session-rename";
    rename.type = "button";
    rename.textContent = "✎";
    rename.title = "이름 변경";
    rename.addEventListener("click", (event) => {
      event.stopPropagation();
      const nextTitle = prompt("세션 이름", session.title || "새 대화");
      if (!nextTitle) return;
      session.title = titleFrom(nextTitle);
      session.updatedAt = Date.now();
      saveSessions();
      renderActiveSession();
      renderSessions();
    });

    const remove = document.createElement("button");
    remove.className = "session-delete";
    remove.type = "button";
    remove.textContent = "×";
    remove.title = "대화 삭제";
    remove.addEventListener("click", (event) => {
      event.stopPropagation();
      sessions = sessions.filter((item) => item.id !== session.id);
      if (!sessions.length) {
        const next = createSession();
        activeSessionId = next.id;
      }
      if (activeSessionId === session.id) activeSessionId = sessions[0].id;
      saveSessions();
      renderActiveSession();
      renderSessions();
      history.replaceState({sessionId: activeSessionId}, "", sessionUrl(activeSessionId));
    });

    row.append(button, rename, remove);
    sessionList.appendChild(row);
  });
}

function renderActiveSession() {
  const session = activeSession();
  messages.textContent = "";
  session.messages.forEach((message) => {
    const bubble = appendMessage(message.role, message.text);
    bubble.dataset.messageId = message.id || "";
    if (message.status === "failed" || message.status === "cancelled") {
      const note = document.createElement("p");
      note.textContent = message.status === "cancelled" ? "답변을 중단했습니다." : "답변이 완료되지 않았습니다.";
      const retry = document.createElement("button");
      retry.type = "button";
      retry.className = "retry-button";
      retry.textContent = "다시 시도";
      retry.addEventListener("click", () => sendMessage(message.question, {retryId: message.id}));
      bubble.parentElement.append(note, retry);
    }
  });
  if (!session.messages.length) renderEmptyState();
  sessionTitle.textContent = session.title === "새 대화" ? "Flow 프로젝트 현황" : session.title;
}

function renderEmptyState() {
  const panel = document.createElement("section");
  panel.className = "empty-state";

  const copy = document.createElement("div");
  copy.className = "empty-copy";
  copy.innerHTML = `
    <span>추천 질문</span>
    <h2>무엇을 확인할까요?</h2>
    <p>담당자의 상황부터 막힌 이유, 지금 결정할 일까지 편하게 이어서 물어보세요.</p>
  `;

  const grid = document.createElement("div");
  grid.className = "prompt-toggle-group";
  SUGGESTED_PROMPTS.forEach((item) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "prompt-toggle";
    button.textContent = item.label;
    button.setAttribute("aria-pressed", "false");
    button.title = item.prompt;
    button.addEventListener("click", () => {
      grid.querySelectorAll(".prompt-toggle").forEach((node) => {
        node.classList.remove("active");
        node.setAttribute("aria-pressed", "false");
      });
      button.classList.add("active");
      button.setAttribute("aria-pressed", "true");
      messages.textContent = "";
      input.value = item.prompt;
      sendMessage(item.prompt).catch((error) => {
        const errorText = `오류: ${error.message}`;
        appendMessage("assistant", errorText);
        addSessionMessage("assistant", errorText);
        setBusy(false);
      });
    });
    grid.appendChild(button);
  });

  panel.append(copy, grid);
  messages.appendChild(panel);
}

function addSessionMessage(role, text) {
  const session = activeSession();
  session.messages.push({role, text, at: Date.now()});
  if (role === "user" && session.title === "새 대화") {
    session.title = titleFrom(text);
  }
  session.updatedAt = Date.now();
  sessions = [session, ...sessions.filter((item) => item.id !== session.id)];
  activeSessionId = session.id;
  saveSessions();
  renderSessions();
  sessionTitle.textContent = session.title === "새 대화" ? "Flow 프로젝트 현황" : session.title;
}

function setBusy(isBusy, label = "대기") {
  state.textContent = label;
  sendButton.disabled = false;
  sendButton.textContent = isBusy ? "■" : "↑";
  sendButton.setAttribute("aria-label", isBusy ? "응답 중단" : "전송");
  input.disabled = false;
}

function parseSse(buffer, onEvent) {
  const parts = buffer.split("\n\n");
  const rest = parts.pop() || "";
  for (const part of parts) {
    const line = part.split("\n").find((value) => value.startsWith("data: "));
    if (line) onEvent(line.slice(6));
  }
  return rest;
}

async function sendMessage(text, {retryId = null} = {}) {
  if (pendingRequest || !text) return;
  const session = activeSession();
  const sessionId = session.id;
  let historyEnd = session.messages.length;
  let reply;
  if (retryId) {
    const index = session.messages.findIndex((m) => m.id === retryId);
    if (index < 0) return;
    historyEnd = Math.max(0, index - 1);
    // 같은 가지에서 재시도한다. 이후 대화를 암묵적으로 삭제하지 않는다.
    if (index !== session.messages.length - 1) {
      input.value = text;
      input.focus();
      return;
    }
    reply = session.messages[index];
    reply.text = "";
    reply.status = "pending";
    delete reply.context;
  } else {
    session.messages.push({id: crypto.randomUUID?.() || String(Date.now()), role: "user", text, at: Date.now()});
    reply = {id: `${Date.now()}-${Math.random().toString(16).slice(2)}`, role: "assistant", text: "", question: text, status: "pending", at: Date.now()};
    session.messages.push(reply);
  }
  const previous = session.messages.slice(0, historyEnd);
  const history = previous.filter((m) => !m.status || m.status === "complete")
    .filter((m) => m.text && m.text !== "(생각중)..")
    .slice(-10).map((m) => ({role: m.role, text: m.text}));
  const priorReply = [...previous].reverse().find((m) => m.role === "assistant" && m.status === "complete" && m.context);
  const controller = new AbortController();
  pendingRequest = {controller, sessionId, messageId: reply.id};
  if (session.title === "새 대화") session.title = titleFrom(text);
  session.updatedAt = Date.now();
  saveSessions();
  renderActiveSession();
  renderSessions();
  setBusy(true, "업무를 확인하고 있어요.");
  let completed = false;
  let gotAnswer = false;
  let waitingText = "업무를 확인하고 있어요.";
  let error = null;
  let reader;
  const paint = () => {
    if (activeSessionId !== sessionId) return;
    const bubble = [...messages.querySelectorAll(".bubble")].find((node) => node.dataset.messageId === reply.id);
    if (!bubble) return;
    const follow = messages.scrollHeight - messages.scrollTop - messages.clientHeight < 90;
    // 출처는 완료 이벤트에서 서버가 검증한 링크만 활성화한다.
    const shown = gotAnswer ? reply.text : reply.text.replace(/\[([^\]]+)\]\(https?:\/\/[^)]+\)/g, "$1");
    setBubbleContent(bubble, "assistant", shown || waitingText);
    if (follow) messages.scrollTop = messages.scrollHeight;
  };
  try {
    const response = await fetch(`${API_BASE}/api/chat`, {
      method: "POST", signal: controller.signal,
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({message: text, user: "웹", session_id: sessionId, message_id: reply.id,
        history, context: priorReply?.context || {}}),
    });
    if (!response.ok || !response.body) throw new Error(`요청 실패: ${response.status}`);
    reader = response.body.getReader();
    const decoder = new TextDecoder("utf-8");
    let buffer = "";
    const handleEvent = (payload) => {
      if (payload === "[DONE]") { completed = (gotAnswer || Boolean(reply.text.trim())) && !error; return; }
      const event = JSON.parse(payload);
      if (event.type === "delta") reply.text += event.text || "";
      else if (event.type === "answer") {
        reply.text = event.text || "";
        reply.context = event.context;
        reply.sources = event.sources;
        reply.validation = event.validation;
        gotAnswer = true;
      } else if (event.type === "error") {
        error = new Error(event.message || "답변 생성 실패");
      } else if (event.type === "status") {
        waitingText = event.text || "확인 중";
        state.textContent = waitingText;
      }
      paint();
    };
    while (true) {
      const {value, done} = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, {stream: true});
      buffer = parseSse(buffer.replace(/\r\n/g, "\n"), handleEvent);
    }
    buffer += decoder.decode();
    if (buffer.trim()) parseSse(buffer + "\n\n", handleEvent);
    if (error) throw error;
    if (!completed) throw new Error("답변 연결이 종료되었습니다.");
    reply.status = "complete";
  } catch (failure) {
    reply.status = failure.name === "AbortError" ? "cancelled" : "failed";
    if (!reply.text) reply.text = failure.name === "AbortError" ? "" : "답변을 완료하지 못했습니다.";
    delete reply.context;
  } finally {
    if (reader) { try { await reader.cancel(); } catch {} }
    pendingRequest = null;
    reply.at = Date.now();
    saveSessions();
    if (activeSessionId === sessionId) {
      const follow = messages.scrollHeight - messages.scrollTop - messages.clientHeight < 90;
      const top = messages.scrollTop;
      renderActiveSession();
      if (!follow) messages.scrollTop = top;
    }
    renderSessions();
    setBusy(false);
  }
}

form.addEventListener("submit", (event) => {
  event.preventDefault();
  if (pendingRequest) { pendingRequest.controller.abort(); return; }
  const text = input.value.trim();
  if (!text) return;
  input.value = "";
  input.style.height = "auto";
  sendMessage(text);
});

input.addEventListener("input", () => {
  input.style.height = "auto";
  input.style.height = `${Math.min(input.scrollHeight, 180)}px`;
});
input.addEventListener("keydown", (event) => {
  if (event.isComposing || event.keyCode === 229) return;
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    if (!pendingRequest) form.requestSubmit();
  }
});

newSessionButton.addEventListener("click", () => {
  setActiveSession(createSession().id, {focus: true});
});

homeButton.addEventListener("click", () => {
  const current = activeSession();
  if (current.messages.length) {
    setActiveSession(createSession().id, {focus: true});
    return;
  }
  setActiveSession(activeSessionId, {focus: true});
});

window.addEventListener("popstate", (event) => {
  const requestedSessionId = event.state?.sessionId || new URLSearchParams(window.location.search).get("session");
  if (sessions.some((session) => session.id === requestedSessionId)) {
    activeSessionId = requestedSessionId;
  } else if (!sessions.some((session) => session.id === activeSessionId)) {
    activeSessionId = sessions[0]?.id || createSession().id;
  }
  renderActiveSession();
  renderSessions();
});

sessions.forEach((session) => session.messages.forEach((message) => {
  if (message.status === "pending") message.status = "failed";
}));
sessions.sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));
saveSessions();
setActiveSession(activeSessionId, {push: false});
