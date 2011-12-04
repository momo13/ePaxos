%%%-------------------------------------------------------------------
%%% @author Monika Moser <monika.m.moser@googlemail.com>
%%% @copyright 
%%% @doc Server responsible to coordinate Paxos participants ond a 
%%%      single node. (Acceptor, Proposer, Learner)
%%% @end
%%%-------------------------------------------------------------------

-module(ePaxos_server).
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {}).

init([]) ->
	{ok, #state{}}.
	
handle_call(_Request, _From, State) ->
	Reply = ok,
	{reply, Reply, State}.
	
handle_cast(_Msg, State) ->
	{noreply, State}.
	
handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.
	
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.