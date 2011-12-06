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
-export([propose/2, promise/2, accepted/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("paxos_messages.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, { paxos_id,
		 quorum, 
		 acceptors, 
		 promises, 
		 accepts,
	         accepted }).

%%%===================================================================
%%% API
%%%===================================================================

propose(ProposerPID, ProposalID) ->
    gen_server:call(ProposerPID, {propose, ProposalID}).

promise(ProposerID, Message) ->
    gen_server:cast(ProposerID, Message).

accepted(ProposerID, Message) ->
    gen_server:cast(ProposerID, Message).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% TODO add a callback to the issuing process for the outcome
init([PaxosID, Acceptors, Quorum]) ->
    {ok, #state{ paxos_id = PaxosID,
		 quorum = Quorum, 
		 acceptors = Acceptors, 
		 promises = [], 
		 accepts = [],
		 accepted = false } }.

handle_call({propose, ProposalID}, _From, State) ->
    lists:foreach( fun(Acceptor) -> 
			   paxos_acceptor:prepare(Acceptor, State#state.paxos_id, ProposalID) end, State#state.acceptors),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({promise, PromiseMessage}, State) when 
      State#state.accepted orelse PromiseMessage#promise_message.paxos_id /= State#state.paxos_id ->
    {noreply, State};
handle_cast({promise, #promise_message{acceptor_ref = Acceptor} }, State)  ->
    case valid_acceptor(Acceptor, State) of
	true ->
	    %%TODO: check whether promises contain a quorum
	    {noreply, add_promise(Acceptor, State)};
	_ ->
	    {noreply, State}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
	
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

add_promise(Acceptor, State) ->
    add_promise(State#state.promises, Acceptor, State).

add_promise([], Promise, State) ->
    Promises = State#state.promises,
    State#state{promises = [Promise | Promises]};
add_promise([Promise | _ ], Promise, State) ->
    State;
add_promise([ _ | RPromises], Promise, State) ->
    add_promise(RPromises, Promise, State).

valid_acceptor(Acceptor, State) ->
    contains(State#state.acceptors, Acceptor).

contains([], _) ->
    false;
contains([Acceptor | _ ], Acceptor) ->
    true;
contains([_| Acceptors], Acceptor) ->
    contains(Acceptors, Acceptor).


%%%===================================================================
%%% Tests
%%%===================================================================

-ifdef(TEST).

test_state(PaxosID, Quorum, Acceptors) ->
    #state{ paxos_id = PaxosID, 
		 quorum = Quorum, 
		 acceptors = Acceptors, 
		 promises = [], 
		 accepts = [] }.

test_state(PaxosID, Quorum, Acceptors, Promises) ->
    #state{ paxos_id = PaxosID, 
		 quorum = Quorum, 
		 acceptors = Acceptors, 
		 promises = Promises, 
		 accepts = [] }.


ignore_promise_for_wrong_paxos_id_test() ->
    State = test_state(1, 1, []),
    ?assertEqual({noreply, State}, handle_cast({promise, #promise_message{paxos_id = 2}}, State)).

ignore_promise_if_proposal_was_already_accepted_test() ->
    State = test_state(1, 1, []),
    AcceptedState = State#state{accepted = true},
    ?assertEqual({noreply, AcceptedState}, handle_cast({promise, #promise_message{paxos_id = 1}}, AcceptedState)).

add_new_promise_test() ->
    State = test_state(1, 1, []),
    ?assertEqual(State#state{promises = [promise]}, add_promise(promise, State)).

ignore_existing_promise_test() ->
    State = test_state(1, 1, [], [promise]),
    ?assertEqual(State, add_promise(promise, State)).

append_to_existing_promise_test() ->
    State = test_state(1, 1, [], [promise1]),
    ?assertEqual(State#state{promises = [promise2, promise1]}, add_promise(promise2, State)).

valid_acceptor_found_test() ->
    State = test_state(1, 1, [acceptor1, acceptor2]),
    ?assert(valid_acceptor(acceptor1, State)).

invalid_acceptor_test() ->
    State = test_state(1, 1, [acceptor1, acceptor2]),
    ?assertNot(valid_acceptor(acceptor3, State)).

cast_add_promise_test() ->
    State = test_state(1, 1, [acceptor1, acceptor2, acceptor3], []),
    ?assertEqual({noreply, State#state{promises = [acceptor1] }}, handle_cast({promise, #promise_message{acceptor_ref = acceptor1, paxos_id = 1}}, State)).

cast_ignore_existing_promise_test() ->
    State = test_state(1, 1, [acceptor1, acceptor2, acceptor3], [acceptor1, acceptor2]),
    ?assertEqual({noreply, State}, handle_cast({promise, #promise_message{acceptor_ref = acceptor1, paxos_id = 1}}, State)).

cast_ignore_invalid_acceptor_promise_test() ->
    State = test_state(1, 1, [acceptor1, acceptor2, acceptor3], [acceptor1, acceptor2]),
    ?assertEqual({noreply, State}, handle_cast({promise, #promise_message{acceptor_ref = acceptor4, paxos_id = 1}}, State)).

-endif.


