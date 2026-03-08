import fs from "node:fs";
import path from "node:path";
import { InboxMessage } from "./models";
import { withLock } from "./lock";
import { inboxPath } from "./paths";
import { readConfig } from "./teams";

export function nowIso(): string {
  return new Date().toISOString();
}

export async function appendMessage(teamName: string, agentName: string, message: InboxMessage) {
  const p = inboxPath(teamName, agentName);
  const dir = path.dirname(p);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  await withLock(p, async () => {
    let msgs: InboxMessage[] = [];
    if (fs.existsSync(p)) {
      msgs = JSON.parse(fs.readFileSync(p, "utf-8"));
    }
    msgs.push(message);
    fs.writeFileSync(p, JSON.stringify(msgs, null, 2));
  });
}

/**
 * Internal function to read inbox from file path.
 * Does not handle waiting - just reads the file.
 */
async function readInboxFromFile(
  filePath: string,
  unreadOnly: boolean,
  markAsRead: boolean
): Promise<InboxMessage[]> {
  if (!fs.existsSync(filePath)) return [];

  return await withLock(filePath, async () => {
    const allMsgs: InboxMessage[] = JSON.parse(fs.readFileSync(filePath, "utf-8"));

    let result = allMsgs;
    if (unreadOnly) {
      result = allMsgs.filter(m => !m.read);
    }

    // Return COPY of messages BEFORE marking as read
    // This way caller sees read: false for new messages
    const toReturn = result.map(m => ({ ...m }));

    // Mark as read AFTER copying
    if (markAsRead && result.length > 0) {
      for (const m of allMsgs) {
        if (result.includes(m)) {
          m.read = true;
        }
      }
      fs.writeFileSync(filePath, JSON.stringify(allMsgs, null, 2));
    }

    return toReturn;
  });
}

/**
 * Wait for a new message to arrive in the inbox.
 * Uses fs.watch for efficient waiting without polling.
 * @param teamName The team name
 * @param agentName The agent name whose inbox to watch
 * @param timeoutMs Timeout in milliseconds (default 120000 = 2 minutes)
 * @param signal AbortSignal for cancellation (ESC key)
 * @returns true if a new message arrived, false if timeout/aborted
 */
export async function waitForMessage(
  teamName: string,
  agentName: string,
  timeoutMs = 120000,
  signal?: AbortSignal
): Promise<boolean> {
  const p = inboxPath(teamName, agentName);
  const dir = path.dirname(p);

  // Ensure directory exists for fs.watch
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }

  return new Promise((resolve) => {
    let resolved = false;
    let watcher: fs.FSWatcher | null = null;
    let timeoutId: NodeJS.Timeout | null = null;

    const cleanup = () => {
      if (resolved) return;
      resolved = true;
      if (watcher) watcher.close();
      if (timeoutId) clearTimeout(timeoutId);
    };

    // Set up timeout
    timeoutId = setTimeout(() => {
      cleanup();
      resolve(false);
    }, timeoutMs);

    // Handle abort signal (ESC key)
    if (signal) {
      if (signal.aborted) {
        cleanup();
        resolve(false);
        return;
      }
      signal.addEventListener('abort', () => {
        cleanup();
        resolve(false);
      }, { once: true });
    }

    // Check for unread messages
    const checkForNewMessage = async () => {
      try {
        const msgs = await readInboxFromFile(p, true, false);
        if (msgs.length > 0) {
          cleanup();
          resolve(true);
        }
      } catch {
        // File might be locked or corrupted, ignore
      }
    };

    // Check immediately first
    checkForNewMessage();

    // If file doesn't exist yet, watch the directory
    if (!fs.existsSync(p)) {
      watcher = fs.watch(dir, (eventType, filename) => {
        if (filename === path.basename(p)) {
          checkForNewMessage();
        }
      });
    } else {
      watcher = fs.watch(p, () => {
        checkForNewMessage();
      });
    }

    watcher.on('error', () => {
      cleanup();
      resolve(false);
    });
  });
}

/**
 * Read messages from an agent's inbox.
 * @param teamName The team name
 * @param agentName The agent name whose inbox to read
 * @param unreadOnly Only return unread messages (default true)
 * @param markAsRead Mark returned messages as read (default true)
 * @param waitForNew Wait for new messages if none exist (default false)
 * @param timeoutMs Timeout for waiting in milliseconds (default 120000 = 2 minutes)
 * @param signal AbortSignal for cancellation (ESC key)
 */
export async function readInbox(
  teamName: string,
  agentName: string,
  unreadOnly = true,
  markAsRead = true,
  waitForNew = false,
  timeoutMs = 120000,
  signal?: AbortSignal
): Promise<InboxMessage[]> {
  const p = inboxPath(teamName, agentName);

  // If waitForNew is true and no messages exist, wait for them
  if (waitForNew) {
    const existingMsgs = await readInboxFromFile(p, true, false);
    if (existingMsgs.length === 0) {
      // Wait for new message
      const arrived = await waitForMessage(teamName, agentName, timeoutMs, signal);
      if (!arrived) {
        // Timeout or aborted - return empty array
        return [];
      }
    }
  }

  return await readInboxFromFile(p, unreadOnly, markAsRead);
}

export async function sendPlainMessage(
  teamName: string,
  fromName: string,
  toName: string,
  text: string,
  summary: string,
  color?: string
) {
  const msg: InboxMessage = {
    from: fromName,
    text,
    timestamp: nowIso(),
    read: false,
    summary,
    color,
  };
  await appendMessage(teamName, toName, msg);
}

/**
 * Broadcasts a message to all team members except the sender.
 * @param teamName The name of the team
 * @param fromName The name of the sender
 * @param text The message text
 * @param summary A short summary of the message
 * @param color An optional color for the message
 */
export async function broadcastMessage(
  teamName: string,
  fromName: string,
  text: string,
  summary: string,
  color?: string
) {
  const config = await readConfig(teamName);

  // Create an array of delivery promises for all members except the sender
  const deliveryPromises = config.members
    .filter((member) => member.name !== fromName)
    .map((member) => sendPlainMessage(teamName, fromName, member.name, text, summary, color));

  // Execute deliveries in parallel and wait for all to settle
  const results = await Promise.allSettled(deliveryPromises);

  // Log failures for diagnostics
  const failures = results.filter((r): r is PromiseRejectedResult => r.status === "rejected");
  if (failures.length > 0) {
    console.error(`Broadcast partially failed: ${failures.length} messages could not be delivered.`);
    // Optionally log individual errors
    failures.forEach((f) => console.error(`- Delivery error:`, f.reason));
  }
}
