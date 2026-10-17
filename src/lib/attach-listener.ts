import { MutableRefObject } from "react";
import { setUpSocketIOPing } from "./socket-io";
import { heartbeat } from "./heartbeat";
import {
  DEFAULT_RECONNECT_LIMIT,
  DEFAULT_RECONNECT_INTERVAL_MS,
  ReadyState,
  isEventSourceSupported,
} from "./constants";
import { Options, SendMessage, WebSocketLike } from "./types";
import { assertIsWebSocket, closeWebSocket } from "./util";

export interface Setters {
  setLastMessage: (message: WebSocketEventMap["message"]) => void;
  setReadyState: (readyState: ReadyState) => void;
}

const bindMessageHandler = (
  webSocketInstance: WebSocketLike,
  optionsRef: MutableRefObject<Options>,
  setLastMessage: Setters["setLastMessage"],
  lastMessageTime: MutableRefObject<number>
) => {
  webSocketInstance.onmessage = (message: WebSocketEventMap["message"]) => {
    optionsRef.current.onMessage && optionsRef.current.onMessage(message);

    if (typeof lastMessageTime?.current === "number") {
      lastMessageTime.current = Date.now();
    }

    if (
      typeof optionsRef.current.filter === "function" &&
      optionsRef.current.filter(message) !== true
    ) {
      return;
    }
    if (
      optionsRef.current.heartbeat &&
      typeof optionsRef.current.heartbeat !== "boolean" &&
      optionsRef.current.heartbeat?.returnMessage === message.data
    ) {
      return;
    }

    setLastMessage(message);
  };
};

const bindOpenHandler = (
  webSocketInstance: WebSocketLike,
  optionsRef: MutableRefObject<Options>,
  setReadyState: Setters["setReadyState"],
  reconnectCount: MutableRefObject<number>,
  lastMessageTime: MutableRefObject<number>
) => {
  webSocketInstance.onopen = (event: WebSocketEventMap["open"]) => {
    optionsRef.current.onOpen && optionsRef.current.onOpen(event);
    reconnectCount.current = 0;
    setReadyState(ReadyState.OPEN);
    //start heart beat here
    if (
      optionsRef.current.heartbeat &&
      webSocketInstance instanceof WebSocket
    ) {
      const heartbeatOptions =
        typeof optionsRef.current.heartbeat === "boolean"
          ? undefined
          : optionsRef.current.heartbeat;
      lastMessageTime.current = Date.now();
      heartbeat(webSocketInstance, lastMessageTime, heartbeatOptions);
    }
  };
};

/**
 * The next reconnect attempt for one socket.
 *
 * A failed WebSocket reports itself twice, with an error and then with a
 * close. With both `retryOnError` and `shouldReconnect` set, each event used
 * to schedule an attempt of its own: two sockets were opened at once, and the
 * listeners of the first one were lost, so nothing ever closed it. A socket
 * now gets at most one attempt, whichever event asks first.
 */
interface ReconnectScheduler {
  schedule: () => void;
  cancel: () => void;
}

const createReconnectScheduler = (
  optionsRef: MutableRefObject<Options>,
  reconnect: () => void,
  reconnectCount: MutableRefObject<number>
): ReconnectScheduler => {
  let reconnectTimeout: number | undefined;
  let isScheduled = false;

  return {
    schedule: () => {
      if (isScheduled) return;
      isScheduled = true;

      const reconnectAttempts =
        optionsRef.current.reconnectAttempts ?? DEFAULT_RECONNECT_LIMIT;

      if (reconnectCount.current >= reconnectAttempts) {
        optionsRef.current.onReconnectStop &&
          optionsRef.current.onReconnectStop(reconnectAttempts);
        console.warn(`Max reconnect attempts of ${reconnectAttempts} exceeded`);
        return;
      }

      const nextReconnectInterval =
        typeof optionsRef.current.reconnectInterval === "function"
          ? optionsRef.current.reconnectInterval(reconnectCount.current)
          : optionsRef.current.reconnectInterval;

      reconnectTimeout = window.setTimeout(() => {
        reconnectCount.current++;
        reconnect();
      }, nextReconnectInterval ?? DEFAULT_RECONNECT_INTERVAL_MS);
    },

    cancel: () => {
      if (reconnectTimeout !== undefined) window.clearTimeout(reconnectTimeout);
    },
  };
};

const bindCloseHandler = (
  webSocketInstance: WebSocketLike,
  optionsRef: MutableRefObject<Options>,
  setReadyState: Setters["setReadyState"],
  reconnectScheduler: ReconnectScheduler
) => {
  if (isEventSourceSupported && webSocketInstance instanceof EventSource) {
    return;
  }
  assertIsWebSocket(webSocketInstance, optionsRef.current.skipAssert);

  webSocketInstance.onclose = (event: WebSocketEventMap["close"]) => {
    optionsRef.current.onClose && optionsRef.current.onClose(event);
    setReadyState(ReadyState.CLOSED);
    if (
      optionsRef.current.shouldReconnect &&
      optionsRef.current.shouldReconnect(event)
    ) {
      reconnectScheduler.schedule();
    }
  };
};

const bindErrorHandler = (
  webSocketInstance: WebSocketLike,
  optionsRef: MutableRefObject<Options>,
  setReadyState: Setters["setReadyState"],
  reconnectScheduler: ReconnectScheduler
) => {
  webSocketInstance.onerror = (error: WebSocketEventMap["error"]) => {
    optionsRef.current.onError && optionsRef.current.onError(error);
    if (isEventSourceSupported && webSocketInstance instanceof EventSource) {
      optionsRef.current.onClose &&
        optionsRef.current.onClose({
          ...error,
          code: 1006,
          reason: `An error occurred with the EventSource: ${error}`,
          wasClean: false,
        });

      setReadyState(ReadyState.CLOSED);
      webSocketInstance.close();
    }

    if (optionsRef.current.retryOnError) {
      reconnectScheduler.schedule();
    }
  };
};

export const attachListeners = (
  webSocketInstance: WebSocketLike,
  setters: Setters,
  optionsRef: MutableRefObject<Options>,
  reconnect: () => void,
  reconnectCount: MutableRefObject<number>,
  lastMessageTime: MutableRefObject<number>,
  sendMessage: SendMessage
): (() => void) => {
  const { setLastMessage, setReadyState } = setters;

  let interval: number;

  if (optionsRef.current.fromSocketIO) {
    interval = setUpSocketIOPing(sendMessage);
  }

  const reconnectScheduler = createReconnectScheduler(
    optionsRef,
    reconnect,
    reconnectCount
  );

  bindMessageHandler(
    webSocketInstance,
    optionsRef,
    setLastMessage,
    lastMessageTime
  );

  bindOpenHandler(
    webSocketInstance,
    optionsRef,
    setReadyState,
    reconnectCount,
    lastMessageTime
  );

  bindCloseHandler(
    webSocketInstance,
    optionsRef,
    setReadyState,
    reconnectScheduler
  );

  bindErrorHandler(
    webSocketInstance,
    optionsRef,
    setReadyState,
    reconnectScheduler
  );

  return () => {
    setReadyState(ReadyState.CLOSING);
    reconnectScheduler.cancel();

    // The hook closes this socket itself, so whatever it reports afterwards
    // (a close event arriving seconds later, an error on the way down) is
    // not a lost connection: it must neither schedule a reconnect nor reach
    // the callbacks of a connection that has already been replaced. Only
    // the state follows the socket down to CLOSED.
    webSocketInstance.onopen = null;
    webSocketInstance.onmessage = null;
    webSocketInstance.onerror = null;
    if (!(isEventSourceSupported && webSocketInstance instanceof EventSource)) {
      (webSocketInstance as WebSocket).onclose = () =>
        setReadyState(ReadyState.CLOSED);
    }

    closeWebSocket(webSocketInstance, optionsRef.current.defaulClosingCode);
    if (interval) clearInterval(interval);
  };
};
