%%%-------------------------------------------------------------------
%%% @author Monika Moser <monika.m.moser@googlemail.com>
%%% @copyright (C) 2011, Monika Moser
%%% @doc
%%%
%%% @end
%%% Created :  4 Dec 2011 by Monika Moser <monika.m.moser@googlemail.com>
%%%-------------------------------------------------------------------

-module(paxos_proposer).

-behaviour(gen_server).

%% API
-export([propose/2, promise/2, accept/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {paxos_id, quorum, acceptors, prepare_acks, accept_acks}).

%%%===================================================================
%%% API
%%%===================================================================

propose(ProposerPID, ProposalID) ->
    gen_server:call(ProposerPID, {propose, ProposalID}).

promise(ProposerID, Message) ->
    gen_server:cast(ProposerID, Message).

accept(ProposerID, Message) ->
    gen_server:cast(ProposerID, Message).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% TODO add a callback to the issuing process for the outcome
init([PaxosID, Acceptors, Quorum]) ->
    {ok, #state{paxos_id = PaxosID, quorum = Quorum, acceptors = Acceptors, prepare_acks = [], accept_acks = []}}.

handle_call({propose, ProposalID}, _From, State) ->
    lists:foreach( fun(Acceptor) -> 
			   paxos_acceptor:prepare(Acceptor, State#state.paxos_id, ProposalID) end, State#state.acceptors),
    {reply, ok, State};
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


%%%===================================================================
%%% Tests
%%%===================================================================

-ifdef(TEST).

-endif.


