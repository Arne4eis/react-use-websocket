import { WebSocketLike } from './types';
import { resetWebSockets } from './globals';
import { resetSubscribers } from './manage-subscribers';
import { isEventSourceSupported, WebSocketClosingCode } from './constants';

export function assertIsWebSocket (
    webSocketInstance: WebSocketLike,
    skip?: boolean,
): asserts webSocketInstance is WebSocket {
    if (!skip && webSocketInstance instanceof WebSocket === false) throw new Error('');
};


export function resetGlobalState (url?: string): void {
    resetSubscribers(url);
    resetWebSockets(url);
};

/**
 * Closes a connection the hook does not need anymore.
 *
 * Without a code the browser sends a close frame with no status, and the
 * server sees 1005 instead of a normal closure. Browsers also accept only
 * 1000 and 3000-4999 in `close()`: any other code throws an
 * InvalidAccessError, and the socket would stay open with nobody left to
 * close it. Both cases fall back to 1000.
 */
export function closeWebSocket (
    webSocketInstance: WebSocketLike,
    code?: number,
): void {
    if (isEventSourceSupported && webSocketInstance instanceof EventSource) {
        webSocketInstance.close();
        return;
    }

    const isAllowed =
        code === WebSocketClosingCode.CLOSE_NORMAL_CLOSURE ||
        (code !== undefined && code >= 3000 && code <= 4999);

    (webSocketInstance as WebSocket).close(
        isAllowed ? code : WebSocketClosingCode.CLOSE_NORMAL_CLOSURE,
    );
};
