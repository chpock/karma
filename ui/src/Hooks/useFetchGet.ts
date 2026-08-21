import { useState, useEffect, useCallback, useRef } from "react";

import promiseRetry from "promise-retry";

import { CommonOptions, FetchRetryConfig } from "Common/Fetch";

type FetchFunctionT = (
  input: RequestInfo | URL,
  init?: RequestInit | undefined,
) => Promise<Response>;

export interface FetchGetOptionsT {
  autorun?: boolean;
  deps?: string[] | number[];
  fetcher?: null | FetchFunctionT;
}

interface ResponseState<T> {
  response: null | T;
  error: null | string;
  isLoading: boolean;
  isRetrying: boolean;
  retryCount: number;
}

export interface FetchGetResultT<T> {
  response: null | T;
  error: null | string;
  isLoading: boolean;
  isRetrying: boolean;
  retryCount: number;
  get: () => Promise<void>;
  cancelGet: () => void;
}

const useFetchGet = <T>(
  uri: string,
  { autorun = true, deps = [], fetcher = null }: FetchGetOptionsT = {},
): FetchGetResultT<T> => {
  const [response, setResponse] = useState<ResponseState<T>>({
    response: null,
    error: null,
    isLoading: autorun,
    isRetrying: false,
    retryCount: 0,
  });
  const requestGenerationRef = useRef(0);
  const controllerRef = useRef<AbortController | null>(null);

  const cancelGet = useCallback(() => {
    requestGenerationRef.current += 1;
    controllerRef.current?.abort();
    controllerRef.current = null;
    setResponse((r) => ({
      ...r,
      isLoading: false,
      isRetrying: false,
    }));
  }, []);

  const get = useCallback(async () => {
    const generation = requestGenerationRef.current + 1;
    requestGenerationRef.current = generation;
    controllerRef.current?.abort();
    const controller = new AbortController();
    controllerRef.current = controller;
    const isCurrent = () => requestGenerationRef.current === generation;

    try {
      if (isCurrent()) {
        setResponse((r) => ({
          ...r,
          isLoading: true,
          isRetrying: false,
          retryCount: 0,
        }));
      }

      const res = await promiseRetry(
        (retry: (err: Error) => Promise<Response>, n: number) =>
          (isCurrent()
            ? (fetcher || fetch)(uri, {
                ...{ method: "GET" },
                ...CommonOptions,
                mode: n <= FetchRetryConfig.retries ? "cors" : "no-cors",
                signal: controller.signal,
              } as RequestInit)
            : Promise.reject(new Error("request superseded"))
          ).catch((err: Error) => {
            if (!isCurrent()) throw err;
            setResponse((r) => ({
              ...r,
              isRetrying: true,
              retryCount: n,
            }));
            return retry(err);
          }),
        FetchRetryConfig,
      );

      if (res !== undefined && isCurrent()) {
        let body;
        const contentType = res.headers.get("content-type");
        try {
          if (contentType && contentType.indexOf("application/json") !== -1) {
            body = await res.json();
          } else {
            body = await res.text();
          }
        } catch {
          if (isCurrent()) {
            setResponse((r) => ({
              ...r,
              error: res.ok
                ? "invalid response body"
                : `${res.status} ${res.statusText || "request failed"}`,
              isLoading: false,
              isRetrying: false,
            }));
          }
          return;
        }

        if (isCurrent()) {
          if (res.ok) {
            setResponse({
              response: body,
              error: null,
              isLoading: false,
              isRetrying: false,
              retryCount: 0,
            });
          } else {
            setResponse({
              response: null,
              error: `${res.status} ${res.statusText || "request failed"}`,
              isLoading: false,
              isRetrying: false,
              retryCount: 0,
            });
          }
        }
      }
    } catch (error) {
      if (isCurrent()) {
        setResponse((r) => ({
          ...r,
          error:
            error instanceof Error ? error.message : "unknown request error",
          isLoading: false,
          isRetrying: false,
        }));
      }
    } finally {
      if (isCurrent()) {
        controllerRef.current = null;
      }
    }
  }, [uri, fetcher]);

  useEffect(() => {
    if (autorun) get();

    return () => cancelGet();
  }, [uri, get, cancelGet, autorun, ...deps]);

  return { get, cancelGet, ...response };
};

export { useFetchGet };
