%%%-------------------------------------------------------------------
%%% @author Monika Moser <monika.m.moser@googlemail.com>
%%% @copyright 2011, Monika Moser
%%% @doc Paxos acceptor
%%% @end
%%%-------------------------------------------------------------------

-module(paxos_acceptor).

-behaviour(gen_server).

%% API
-export([prepare/3, accept/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("paxos_messages.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {paxosID, promisedProposal, acceptedProposal, acceptedVal}).

%%TODO: macke both prepare and accept asynchronous
prepare(AcceptorPID, PaxosID, ProposalID) ->
    gen_server:call(AcceptorPID, {prepare, PaxosID, ProposalID}). 

accept(AcceptorPID, PaxosID, ProposalID, Value) ->
    gen_server:call(AcceptorPID, {accept, PaxosID, ProposalID, Value}).


init([PaxosID]) ->
    {ok, #state{paxosID = PaxosID, promisedProposal = -1, acceptedProposal = -1, acceptedVal = nil}}.

handle_call({_, PaxosID, _}, _From, #state{paxosID = PID} = State) when PID /= PaxosID ->
    Reply = reject,
    {reply, Reply, State};

handle_call({prepare, _PaxosID, ProposalID}, _From, #state{promisedProposal = PP} = State) when ProposalID =< PP ->
    Reply = reject,
    {reply, Reply, State};
handle_call({prepare, _PaxosID, ProposalID}, _From, State) ->
    Reply = {promise, State#state.acceptedProposal, State#state.acceptedVal},
%%% TODO: make permanent. Store on disk!
    {reply, Reply, State#state{promisedProposal = ProposalID}};

handle_call({accept, _PaxosID, ProposalID, _Value}, _From, #state{promisedProposal = PP} = State) when ProposalID =< PP ->
    Reply = reject,
    {reply, Reply, State};
handle_call({accept, _PaxosID, ProposalID, Value}, _From, State)  ->
    Reply = accept,
    {reply, Reply, State#state{acceptedProposal = ProposalID, acceptedVal = Value}};

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

reject_any_message_with_wrong_paxosID_test() ->
    State = #state{paxosID = 1, promisedProposal = 2, acceptedProposal = -1, acceptedVal = nil},
    lists:foreach( fun(MessageType) ->
			   ?assertEqual({reply, reject, State}, handle_call({MessageType, 2, 1}, sender, State)) end, [prepare, accept]).

reject_prepare_with_smaller_proposal_number_test() ->
    State = #state{paxosID = 1, promisedProposal = 2, acceptedProposal = -1, acceptedVal = nil},
    ?assertEqual({reply, reject, State}, handle_call({prepare, 1, 1}, sender, State)).

reject_prepare_with_equal_proposal_number_test() ->
    State = #state{paxosID = 1, promisedProposal = 2, acceptedProposal = -1, acceptedVal = nil},
    ?assertEqual({reply, reject, State}, handle_call({prepare, 1, 2}, sender, State)).

promise_prepare_with_greater_proposal_number_test() ->
    State = #state{paxosID = 1, promisedProposal = 2, acceptedProposal = -1, acceptedVal = nil},
    ?assertEqual({reply, {promise, -1, nil}, State#state{promisedProposal = 3}}, handle_call({prepare, 1, 3}, sender, State)).

promise_prepare_with_greater_proposal_number_send_accepted_value_test() ->
    State = #state{paxosID = 1, promisedProposal = 2, acceptedProposal = 1, acceptedVal = 9},
    ?assertEqual({reply, {promise, 1, 9}, State#state{promisedProposal = 3}}, handle_call({prepare, 1, 3}, sender, State)).

-endif.
