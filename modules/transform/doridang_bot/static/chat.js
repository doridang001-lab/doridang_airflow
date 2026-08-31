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
  "팀원 별 프로젝트 진행상황",
  "상태 피드백건 확인용",
];

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
  session.messages.forEach((message) => appendMessage(message.role, message.text));
  if (!session.messages.length) renderEmptyState();
  sessionTitle.textContent = session.title === "새 대화" ? "Flow 프로젝트 현황" : session.title;
}

function renderEmptyState() {
  const panel = document.createElement("section");
  panel.className = "empty-state";

  const copy = document.createElement("div");
  copy.className = "empty-copy";
  copy.innerHTML = `
    <span>리더용 빠른 질문</span>
    <h2>무엇을 확인할까요?</h2>
    <p>팀원별 진행상황, 마케팅 실적, 기한 경과 업무를 Flow 데이터 기준으로 바로 정리합니다.</p>
  `;

  const grid = document.createElement("div");
  grid.className = "prompt-grid";
  SUGGESTED_PROMPTS.forEach((prompt) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "prompt-card";
    button.textContent = prompt;
    button.addEventListener("click", () => {
      messages.textContent = "";
      input.value = "";
      sendMessage(prompt).catch((error) => {
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

function updateLastAssistantMessage(text) {
  const session = activeSession();
  for (let index = session.messages.length - 1; index >= 0; index -= 1) {
    if (session.messages[index].role === "assistant") {
      session.messages[index].text = text;
      session.messages[index].at = Date.now();
      session.updatedAt = Date.now();
      saveSessions();
      renderSessions();
      return;
    }
  }
}

function requestHistory() {
  const session = activeSession();
  return session.messages
    .slice(-12)
    .map((message) => ({role: message.role, text: message.text}))
    .filter((message) => message.text && message.text !== "(생각중)..");
}

function setBusy(isBusy, label = "대기") {
  state.textContent = label;
  sendButton.disabled = isBusy;
  input.disabled = isBusy;
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

async function sendMessage(text) {
  appendMessage("user", text);
  addSessionMessage("user", text);
  const thinkingText = "(생각중)..";
  const assistant = appendMessage("assistant", thinkingText);
  addSessionMessage("assistant", thinkingText);
  let isThinking = true;
  setBusy(true, thinkingText);

  const response = await fetch(`${API_BASE}/api/chat`, {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({message: text, user: "웹", history: requestHistory()}),
  });

  if (!response.ok || !response.body) {
    setBubbleContent(assistant, "assistant", `요청 실패: ${response.status}`);
    updateLastAssistantMessage(assistant.dataset.raw || "");
    setBusy(false);
    return;
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8");
  let buffer = "";
  let toolNote = "";

  const handleEvent = (payload) => {
    if (payload === "[DONE]") {
      setBusy(false);
      return;
    }
    const event = JSON.parse(payload);
    if (event.type === "delta") {
      if (isThinking) {
        setBubbleContent(assistant, "assistant", "");
        isThinking = false;
      }
      appendAssistantText(assistant, event.text || "");
      updateLastAssistantMessage(assistant.dataset.raw || "");
      messages.scrollTop = messages.scrollHeight;
    } else if (event.type === "tool") {
      toolNote = `${event.name} 조회 중...`;
      state.textContent = toolNote;
    } else if (event.type === "status") {
      state.textContent = event.text || "대기 중";
    } else if (event.type === "error") {
      appendAssistantText(assistant, `\n오류: ${event.message}`);
      updateLastAssistantMessage(assistant.dataset.raw || "");
    }
  };

  try {
    while (true) {
      const {value, done} = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, {stream: true});
      buffer = parseSse(buffer, handleEvent);
    }
  } finally {
    if (isThinking && assistant.dataset.raw === thinkingText) {
      setBubbleContent(assistant, "assistant", "응답이 종료됐지만 표시할 답변이 없습니다.");
      updateLastAssistantMessage(assistant.dataset.raw || "");
    }
    setBusy(false);
  }
}

form.addEventListener("submit", (event) => {
  event.preventDefault();
  const text = input.value.trim();
  if (!text) return;
  input.value = "";
  sendMessage(text).catch((error) => {
    const errorText = `오류: ${error.message}`;
    appendMessage("assistant", errorText);
    addSessionMessage("assistant", errorText);
    setBusy(false);
  });
});

input.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    form.requestSubmit();
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

sessions.sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));
saveSessions();
setActiveSession(activeSessionId, {push: false});
