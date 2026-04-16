import { useState } from "react";
import { ChatKitPanel } from "./components/ChatKitPanel";
import { GrantCampaignTool } from "./components/GrantCampaignTool";

export default function App() {
  const [tool, setTool] = useState<"chat" | "grant">("chat");
  const clearGrantSession = () => {
    try {
      window.localStorage.removeItem("grant_campaign_tool_form_v1");
      window.localStorage.removeItem("grant_campaign_tool_result_v1");
      window.localStorage.removeItem("grant_campaign_tool_modal_open_v1");
    } catch {
      // no-op
    }
  };
  const switchTool = (next: "chat" | "grant") => {
    if (tool === "grant" && next !== "grant") {
      clearGrantSession();
    }
    setTool(next);
  };

  return (
    <main className="app-shell">
      <div className="app-layout">
        <aside className="app-sidebar">
          <div className="sidebar-brand">
            <p className="kicker">CRI Advantage</p>
            <h1>AI Sales Coach</h1>
          </div>
          <div className="sidebar-group">
            <p className="sidebar-group-label">Research Workspaces</p>
            <button
              className={`sidebar-link ${tool === "chat" ? "active" : ""}`}
              onClick={() => switchTool("chat")}
              type="button"
            >
              Sales Coach Chat
            </button>
            <button
              className={`sidebar-link ${tool === "grant" ? "active" : ""}`}
              onClick={() => switchTool("grant")}
              type="button"
            >
              SLED Campaign Tool
            </button>
          </div>
        </aside>

        <section className="app-main">
          <header className="topbar">
            <p className="kicker topbar-title-centered">
              {tool === "chat" ? "Sales Coach" : "SLED Campaign Tool"}
            </p>
          </header>

          <div className="workspace-wrap">
            {tool === "chat" ? (
              <ChatKitPanel />
            ) : (
              <div className="chat-shell">
                <GrantCampaignTool />
              </div>
            )}
          </div>
        </section>
      </div>
      <div className="mobile-nav">
        <div className="actions-row">
          <button
            className={`sidebar-link ${tool === "chat" ? "active" : ""}`}
            onClick={() => switchTool("chat")}
            type="button"
          >
            Sales Coach Chat
          </button>
          <button
            className={`sidebar-link ${tool === "grant" ? "active" : ""}`}
            onClick={() => switchTool("grant")}
            type="button"
          >
            SLED Campaign Tool
          </button>
        </div>
      </div>
    </main>
  );
}
