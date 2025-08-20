-module(opentelemetry_cowboy).

-export([
         setup/0,
         setup/1,
         handle_event/4,
         is_public_endpoint/2,
         default_public_endpoint_fn/2
]).

-include_lib("opentelemetry_api/include/opentelemetry.hrl").

-define(TRACER_ID, ?MODULE).

setup() -> setup(#{}).

setup(Opts) when is_list(Opts) ->
    setup(maps:from_list(Opts));
setup(Opts) when is_map(Opts) ->
    attach_event_handlers(Opts),
    ok.

attach_event_handlers(Opts) ->
    Events = [
              [cowboy, request, early_error],
              [cowboy, request, start],
              [cowboy, request, stop],
              [cowboy, request, exception]
             ],
    telemetry:attach_many(opentelemetry_cowboy_handlers, Events, fun ?MODULE:handle_event/4, Opts).

handle_event([cowboy, request, start], _Measurements, #{req := Req} = Meta, Config) ->
    Headers = maps:get(headers, Req),
    

    {RemoteIP, _Port} = maps:get(peer, Req),
    Method = maps:get(method, Req),

    Attributes = #{
                  'http.client_ip' => client_ip(Headers, RemoteIP),
                  'http.flavor' => http_flavor(Req),
                  'http.host' => maps:get(host, Req),
                  'http.host.port' => maps:get(port, Req),
                  'http.method' => Method,
                  'http.scheme' => maps:get(scheme, Req),
                  'http.target' => maps:get(path, Req),
                  'http.user_agent' => maps:get(<<"user-agent">>, Headers, <<"">>),
                  'net.host.ip' => iolist_to_binary(inet:ntoa(RemoteIP)),
                  'net.transport' => 'IP.TCP'
                 },
    SpanName = iolist_to_binary([<<"HTTP ">>, Method]),

    % Only create links if we have a valid span context from trusted sources
    case is_public_endpoint(Req, Config) of
        false ->
            otel_propagator_text_map:extract(maps:to_list(Headers)),
            otel_telemetry:start_telemetry_span(?TRACER_ID, SpanName, Meta, #{
                attributes => Attributes,
                kind => ?SPAN_KIND_SERVER
            });
        true ->
            PropagatedCtx = otel_propagator_text_map:extract_to(otel_ctx:new(), maps:to_list(Headers)),
            SpanCtx = otel_tracer:current_span_ctx(PropagatedCtx),
            otel_telemetry:start_telemetry_span(?TRACER_ID, SpanName, Meta, #{
                attributes => Attributes,
                kind => ?SPAN_KIND_SERVER,
                links => opentelemetry:links([SpanCtx])
            })
    end;

handle_event([cowboy, request, stop], Measurements, Meta, _Config) ->
    Ctx = otel_telemetry:set_current_telemetry_span(?TRACER_ID, Meta),
    Status = maps:get(resp_status, Meta),
    Attributes = #{
                  'http.request_content_length' => maps:get(req_body_length, Measurements),
                  'http.response_content_length' => maps:get(resp_body_length, Measurements)
                 },
    otel_span:set_attributes(Ctx, Attributes),
    StatusCode = transform_status_to_code(Status),
    case StatusCode of
        undefined ->
            case maps:get(error, Meta, undefined) of
              {ErrorType, Error, Reason} ->
                otel_span:add_events(Ctx, [opentelemetry:event(ErrorType, #{error => Error, reason => Reason})]),
                otel_span:set_status(Ctx, opentelemetry:status(?OTEL_STATUS_ERROR, Reason));
              _ ->
                % do nothing first as I'm unsure how should we handle this
                ok
            end;
        StatusCode when StatusCode >= 500 ->
            otel_span:set_attribute(Ctx, 'http.status_code', StatusCode),
            otel_span:set_status(Ctx, opentelemetry:status(?OTEL_STATUS_ERROR, <<"">>));
        StatusCode when StatusCode >= 400 ->
            otel_span:set_attribute(Ctx, 'http.status_code', StatusCode);
        StatusCode when StatusCode < 400 ->
            otel_span:set_attribute(Ctx, 'http.status_code', StatusCode)
    end,
    otel_telemetry:end_telemetry_span(?TRACER_ID, Meta),
    otel_ctx:clear();

handle_event([cowboy, request, exception], Measurements, Meta, _Config) ->
    Ctx = otel_telemetry:set_current_telemetry_span(?TRACER_ID, Meta),
    #{
      kind := Kind,
      reason := Reason,
      stacktrace := Stacktrace,
      resp_status := Status
     } = Meta,
    otel_span:record_exception(Ctx, Kind, Reason, Stacktrace, []),
    otel_span:set_status(Ctx, opentelemetry:status(?OTEL_STATUS_ERROR, <<"">>)),
    StatusCode = transform_status_to_code(Status),
    otel_span:set_attributes(Ctx, #{
                                   'http.status_code' => StatusCode,
                                   'http.request_content_length' => maps:get(req_body_length, Measurements),
                                   'http.response_content_length' => maps:get(resp_body_length, Measurements)
                                  }),
    otel_telemetry:end_telemetry_span(?TRACER_ID, Meta),
    otel_ctx:clear();

handle_event([cowboy, request, early_error], Measurements, Meta, _Config) ->
    #{
      reason := {ErrorType, Error, Reason},
      resp_status := Status
     } = Meta,
    StatusCode = transform_status_to_code(Status),
    Attributes = #{
                   'http.status_code' => StatusCode,
                   'http.response_content_length' => maps:get(resp_body_length, Measurements)
                  },
    Opts = #{attributes => Attributes, kind => ?SPAN_KIND_SERVER},
    Ctx = otel_telemetry:start_telemetry_span(?TRACER_ID, <<"HTTP Error">>, Meta, Opts),
    otel_span:add_events(Ctx, [opentelemetry:event(ErrorType, #{error => Error, reason => Reason})]),
    otel_span:set_status(Ctx, opentelemetry:status(?OTEL_STATUS_ERROR, Reason)),
    otel_telemetry:end_telemetry_span(?TRACER_ID, Meta),
    otel_ctx:clear().

transform_status_to_code(Status) when is_binary(Status) ->
  [CodeString | _Message] = string:split(Status, " "),
  {Code, _Rest} = string:to_integer(CodeString),
  Code;
transform_status_to_code(Status) ->
  Status.

http_flavor(Req) ->
    case maps:get(version, Req, undefined) of
        'HTTP/1.0' -> '1.0';
        'HTTP/1.1' -> '1.1';
        'HTTP/2' -> '2.0';
        'SPDY' -> 'SPDY';
        'QUIC' -> 'QUIC';
        _ -> <<"">>
    end.

client_ip(Headers, RemoteIP) ->
  case maps:get(<<"x-forwarded-for">>, Headers, undefined) of
      undefined ->
          iolist_to_binary(inet:ntoa(RemoteIP));
      Addresses ->
          hd(binary:split(Addresses, <<",">>))
  end.

% Determine if this endpoint should distrust external traceparent headers
% This function implements the is_public_endpoint logic to prevent orphaned spans
is_public_endpoint(_Req, #{public_endpoint := true}) -> true;
is_public_endpoint(Req, #{public_endpoint_fn := {M, F, A}}) ->
    apply(M, F, [Req, A]);
is_public_endpoint(_Req, _Config) -> false.

% Default function that always returns false (internal endpoint)
default_public_endpoint_fn(_, _) -> false.
