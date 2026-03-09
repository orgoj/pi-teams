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
 * Uses fs.watch for efficient waiting with fallback polling for reliability.
 *
 * FIXES applied:
 * 1. Await initial check before setting up watcher (race condition fix)
 * 2. Re-check after watcher is set up (close race window)
 * 3. Retry on lock contention instead of silent ignore
 * 4. Always watch directory (handles file creation/deletion)
 * 5. Final check on timeout (don't miss messages that arrived during wait)
 *
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

  // Helper to check for messages with retries on lock contention
  const checkForMessages = async (retries = 3): Promise<boolean> => {
    for (let attempt = 0; attempt < retries; attempt++) {
      try {
        const msgs = await readInboxFromFile(p, true, false);
        if (msgs.length > 0) {
          return true;
        }
        return false;
      } catch (e) {
        // File might be locked, wait and retry
        if (attempt < retries - 1) {
          await new Promise(r => setTimeout(r, 50 * (attempt + 1)));
        }
      }
    }
    return false;
  };

  return new Promise((resolve) => {
    let resolved = false;
    let watcher: fs.FSWatcher | null = null;
    let timeoutId: NodeJS.Timeout | null = null;
    let pollIntervalId: NodeJS.Timeout | null = null;
    let lastCheckTime = Date.now();

    const cleanup = () => {
      if (resolved) return;
      resolved = true;
      if (watcher) watcher.close();
      if (timeoutId) clearTimeout(timeoutId);
      if (pollIntervalId) clearInterval(pollIntervalId);
    };

    // Final check before resolving with false (timeout)
    const finalCheckAndResolve = async (reason: string) => {
      cleanup();
      // Always do a final check - message might have arrived during wait
      const hasMessages = await checkForMessages(5);
      if (hasMessages) {
        console.log(`[waitForMessage] Found messages on ${reason} after final check`);
        resolve(true);
      } else {
        console.log(`[waitForMessage] ${reason}, no messages found`);
        resolve(false);
      }
    };

    // Set up timeout
    timeoutId = setTimeout(() => {
      finalCheckAndResolve('timeout');
    }, timeoutMs);

    // Handle abort signal (ESC key)
    if (signal) {
      if (signal.aborted) {
        finalCheckAndResolve('abort');
        return;
      }
      signal.addEventListener('abort', () => {
        finalCheckAndResolve('abort');
      }, { once: true });
    }

    // Debounced check to avoid lock contention from rapid events
    let checkPending = false;
    const scheduleCheck = () => {
      if (checkPending) return;
      checkPending = true;

      // Small delay to coalesce rapid events
      setTimeout(async () => {
        checkPending = false;
        if (resolved) return;

        const hasMessages = await checkForMessages(3);
        if (hasMessages) {
          cleanup();
          resolve(true);
        }
        lastCheckTime = Date.now();
      }, 20);
    };

    // FALLBACK POLLING: Check every 5 seconds as backup
    // This catches messages that fs.watch might miss
    pollIntervalId = setInterval(async () => {
      if (resolved) return;
      // Only poll if no recent check (avoid unnecessary work)
      if (Date.now() - lastCheckTime > 4000) {
        scheduleCheck();
      }
    }, 5000);

    // Main async flow
    (async () => {
      // FIX 1: Await initial check before setting up watcher
      const initialCheck = await checkForMessages(3);
      if (initialCheck) {
        cleanup();
        resolve(true);
        return;
      }

      // FIX 4: Always watch the directory (not the file)
      // This handles file creation, deletion, and atomic writes
      watcher = fs.watch(dir, (eventType, filename) => {
        if (filename === path.basename(p)) {
          scheduleCheck();
        }
      });

      watcher.on('error', (err) => {
        console.error('[waitForMessage] Watcher error:', err);
        // Don't resolve - let fallback polling handle it
      });

      // FIX 2: Re-check after watcher is set up (close race window)
      // This catches messages that arrived between initial check and watcher setup
      await new Promise(r => setTimeout(r, 10)); // Tiny delay for watcher to be ready
      const postSetupCheck = await checkForMessages(3);
      if (postSetupCheck) {
        cleanup();
        resolve(true);
        return;
      }
    })();

    // FIX 5: Final check is handled in finalCheckAndResolve() on timeout/abort
  });
}

/**
 * Read messages from an agent's inbox.
 *
 * FIX: Removed race condition by always doing final read, even after waitForMessage
 * returns false. The waitForMessage now does a final check itself, but we also
 * do one here to be absolutely sure.
 *
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
      await waitForMessage(teamName, agentName, timeoutMs, signal);
      // FIX: Don't return early on false - always do final read
      // waitForMessage now does final check internally, but we do one more here
    }
  }

  // ALWAYS do a final read - this catches any messages that arrived
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
