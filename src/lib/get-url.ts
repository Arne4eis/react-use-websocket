import { MutableRefObject } from 'react';
import { parseSocketIOUrl, appendQueryParams } from './socket-io';
import { Options } from './types';
import { DEFAULT_RECONNECT_INTERVAL_MS, DEFAULT_RECONNECT_LIMIT } from './constants';

const waitFor = (duration: number) => new Promise(resolve => window.setTimeout(resolve, duration));

/**
 * Resolves the url of the next connection.
 *
 * `isActive` tells whether anybody still waits for this connection. A url
 * function usually makes a request (a one-time ticket, for example), and
 * retries must stop as soon as the component unmounts or stops connecting:
 * with unlimited `reconnectAttempts` they would otherwise go on forever.
 */
export const getUrl = async (
  url: string | (() => string | Promise<string>),
  optionsRef: MutableRefObject<Options>,
  retriedAttempts: number = 0,
  isActive: () => boolean = () => true,
): Promise<string | null> => {
  let convertedUrl: string;

  if (typeof url === 'function') {
    try {
      convertedUrl = await url();
    } catch (e) {
      if (!isActive()) return null;

      if (
        optionsRef.current.retryOnError
      ) {
        const reconnectLimit = optionsRef.current.reconnectAttempts ?? DEFAULT_RECONNECT_LIMIT;
        if (retriedAttempts < reconnectLimit) {
            const nextReconnectInterval = typeof optionsRef.current.reconnectInterval === 'function' ?
              optionsRef.current.reconnectInterval(retriedAttempts) :
              optionsRef.current.reconnectInterval;
    
            await waitFor(nextReconnectInterval ?? DEFAULT_RECONNECT_INTERVAL_MS);
            if (!isActive()) return null;

            return getUrl(url, optionsRef, retriedAttempts + 1, isActive);
          } else {
            optionsRef.current.onReconnectStop?.(retriedAttempts);
            return null;
          }
      } else {
        return null;
      }
    }
  } else {
    convertedUrl = url;
  }

  const parsedUrl = optionsRef.current.fromSocketIO ?
    parseSocketIOUrl(convertedUrl) :
    convertedUrl;

  const parsedWithQueryParams = optionsRef.current.queryParams ?
    appendQueryParams(
      parsedUrl,
      optionsRef.current.queryParams
    ) :
    parsedUrl;

  return parsedWithQueryParams;
};
