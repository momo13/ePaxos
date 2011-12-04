%%%-------------------------------------------------------------------
%%% @author Monika Moser <monika.m.moser@googlemail.com>
%%% @copyright 2011, Monika Moser
%%% @doc Paxos proposer
%%% @end
%%%-------------------------------------------------------------------

-module(paxos_proposer).

-behaviour(gen_server).

%% API
-export([]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {paxosID, quorum, acceptors, prepare_acks, accept_acks}).

%% TODO: make asynchronous and provie call back
propose(ProposerPID, ProposalID) ->
    gen_server:call(ProposerPID, {propose, ProposalID}).

%% TODO add a callback to the issuing process for the outcome
init([PaxosID, Acceptors, Quorum]) ->
    {ok, #state{paxosID = PaxosID, quorum = Quorum, acceptors = Acceptors, prepare_acks = [], accept_acks = []}}.

handle_call({propose, ProposalID}, _From, State) ->
    lists:foreach( fun(Acceptor) -> 
			   paxos_acceptor:prepare(Acceptor, State#state.paxosID, ProposalID) end, State#state.acceptors),
    Reply = ok,
    {reply, Reply, State};
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

-ifdef(TEST).

-endif.
