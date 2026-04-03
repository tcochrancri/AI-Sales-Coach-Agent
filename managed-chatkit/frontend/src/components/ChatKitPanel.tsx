import { useMemo } from "react";
import { ChatKit, useChatKit } from "@openai/chatkit-react";
import { createClientSecretFetcher, workflowId } from "../lib/chatkitSession";

export function ChatKitPanel() {
  const getClientSecret = useMemo(
    () => createClientSecretFetcher(workflowId),
    []
  );

  const chatkit = useChatKit({
    api: { getClientSecret },
  });

  return (
    <div className="chat-shell chat-shell-chatkit flex h-[88vh] min-h-[560px] w-full overflow-hidden rounded-lg border">
      <div className="chatkit-scale h-full w-full">
        <ChatKit control={chatkit.control} className="h-full w-full" />
      </div>
    </div>
  );
}
