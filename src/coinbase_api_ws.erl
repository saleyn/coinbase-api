-module(coinbase_api_ws).
-behaviour(gen_server).
-include("include/records.hrl").

-include_lib("kernel/include/logger.hrl").

%% Sandbox API: https://docs.cloud.coinbase.com/exchange/docs/sandbox

-define(WS_URI,         "wss://ws-feed.exchange.coinbase.com").
-define(WS_URI_SANDBOX, "wss://ws-feed-public.sandbox.exchange.coinbase.com").

-export([start_link/1, reconnect/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    callback  :: pid(),
    uri       :: string(),
    gun       :: any(),
    products  :: [binary()],
    channels  :: [binary()],
    auth_key  :: binary(),
    auth_pass :: binary(),
    auth_secr :: binary()
}).

start_link(Callback) ->
    gen_server:start_link(?MODULE, [Callback], []).

reconnect() ->
    gen_server:cast(?MODULE, reconnect).

init([Callback]) ->
    gen_server:cast(self(), reconnect),
    App            = application:get_application(),
    URI            = application:get_env(App, uri,      ?WS_URI_SANDBOX),
    Channels       = application:get_env(App, channels, [<<"full">>]),
    {ok, Products} = application:get_env(App, products),
    {ok, Secret}   =
      case filelib:is_file("auth.key.secret") of
        true ->
          {ok, _}  = os:cmd("git secret cat auth.key.secret");
        false ->
          {ok, F}  = application:get_env(App, auth_file),
          {ok, _}  = file:read_file(F)
      end,
    AuthData = re:split(Secret, "([^= ]+)\s*=\s*(.+)\n", [trim, group]),
    AuthList = [{K,V} || [_, K, V] <- AuthData],
    #{"CB-ACCESS-KEY"        := AuthKey,
      "CB-ACCESS-PASSPHRASE" := AuthPass,
      "CB-ACCESS-SECRET"     := AuthSecret} = maps:from_list(AuthList),
    State = #state{
        callback  = Callback,
        products  = Products,
        channels  = Channels,
        uri       = URI,
        auth_key  = AuthKey,
        auth_pass = AuthPass,
        auth_secr = AuthSecret
    },
    {ok, State}.

handle_call(Request, _From, State) ->
    logger:info("Unexpected call ~p~n", [Request]),
    {noreply, State}.

handle_cast(reconnect, State) ->
    {ok, State1} = reconnect_websocket(State),
    {noreply, State1};
handle_cast(Msg, State) ->
    logger:info("Unexpected cast ~p~n", [Msg]),
    {noreply, State}.

handle_info({gun_ws, Gun, {close, Code, Reason}}, State) ->
    logger:info("WS state -> closed: ~p: ~p~n", [Code, Reason]),
    gun:close(Gun),
    gen_server:cast(self(), reconnect),
    {noreply, State};
handle_info({gun_ws, Gun, close}, State) ->
    logger:info("WS state -> closed"),
    gun:close(Gun),
    gen_server:cast(self(), reconnect),
    {noreply, State};
handle_info({gun_ws, _Gun, Message}, State) ->
    handle_ws_json_message(State, Message),
    {noreply, State};
handle_info({gun_up, _Gun, _Proto}, State) ->
    {noreply, State};
handle_info({gun_down, Gun, _Proto, Reason, [], []}, State) ->
    logger:info("Gun down (reason: ~p)~n", [Reason]),
    gun:close(Gun),
    gen_server:cast(self(), reconnect),
    {noreply, State};
handle_info({gun_ws_upgrade, Gun, ok, _Headers}, State) ->
    send_ws_message(Gun, subscribe_message(State)),
    {noreply, State};

handle_info(Info, State) ->
    logger:info("Unexpected info ~p~n", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(OldVsn, State, _Extra) ->
    logger:info("~p updated from vsn ~p", [?MODULE, OldVsn]),
    {ok, State}.


%% Internal methods

reconnect_websocket(#state{gun=Pid} = State) when is_pid(Pid) ->
    gun:close(Pid),
    reconnect_websocket(State#state{gun=undefined});
reconnect_websocket(State) ->
    {ok, #{scheme := "wss", host := Host, port := 443, path := Fragment}} =
      url_string:parse(State#state.uri),
    {ok, Pid} = gun:open(Host, 443, #{protocols => [http]}),
    _StreamRef = gun:ws_upgrade(Pid, Fragment),
    {ok, State#state{gun=Pid}}.

subscribe_message(#state{products=Products, channels=Channels} = State) ->
    Time   = integer_to_binary(erlang:system_time(second)),
    Body   = <<"">>,
    Data   = <<Time/binary, "GET", "/users/self/verify", Body/binary>>,
    Hmac   = base64:decode(State#state.auth_secr),
    SignB  = crypto:mac(hmac, sha256, Hmac, Data),
    Sign   = base64:encode(SignB),
    Body   = [
     {type,       subscribe},
     {product_ids,Products},
     {channels,   [{name, C} || C <- Channels]},
     {signature,  Sign},
     {key,        State#state.auth_key},
     {passphrase, State#state.auth_pass},
     {timestamp,  Time}
    ].

send_ws_message(Gun, Message) ->
    JsonMessage = jsx:encode(Message),
    logger:info("Sending: ~p~n", [JsonMessage]),
    gun:ws_send(Gun, {text, JsonMessage}).

handle_ws_json_message(State, {text, Message}) ->
    DecodedMessage = jsx:decode(Message),
    handle_ws_message(
        State,
        parse_ws_message(
            proplists:get_value(<<"type">>, DecodedMessage),
            DecodedMessage
        )
    ).

handle_ws_message(_State, undefined) ->
    % Ignore messages that can't be parsed
    ok;
handle_ws_message(State, Message) ->
    Callback = State#state.callback,
    Callback ! {coinbase_api_ws, self(), Message},
    ok.

parse_ws_message(<<"heartbeat">>, Message) ->
    #coinbase_ws_heartbeat{
        sequence=proplists:get_value(<<"sequence">>, Message),
        last_trade_id=proplists:get_value(<<"last_trade_id">>, Message),
        product_id=proplists:get_value(<<"product_id">>, Message),
        time=proplists:get_value(<<"time">>, Message)
    };
parse_ws_message(<<"ticker">>, Message) ->
    #coinbase_ws_ticker{
        trade_id=proplists:get_value(<<"trade_id">>, Message),
        sequence=proplists:get_value(<<"sequence">>, Message),
        time=proplists:get_value(<<"time">>, Message),
        product_id=proplists:get_value(<<"product_id">>, Message),
        price=decimal:from_binary(proplists:get_value(<<"price">>, Message)),
        side =proplists:get_value(<<"side">>, Message),
        last_size=decimal:from_binary(proplists:get_value(<<"last_size">>, Message)),
        best_bid=decimal:from_binary(proplists:get_value(<<"best_bid">>, Message)),
        best_ask=decimal:from_binary(proplists:get_value(<<"best_ask">>, Message))
    };
parse_ws_message(Type, Message) ->
    logger:info("CB msg ~s: ~p~n", [Type, Message]),
    undefined.
