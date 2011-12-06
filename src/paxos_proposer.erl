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
-export([propose/3, promise/2, accepted/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("paxos_messages.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, { paxos_id,
		 proposal_id,
		 proposal,
		 quorum, 
		 acceptors, 
		 promises, 
		 accepts,
	         accepted,
		 promised,
	         prepare_fun,
	         accept_fun }).

%%%===================================================================
%%% API
%%%===================================================================

propose(ProposerPID, ProposalID, Proposal) ->
    gen_server:call(ProposerPID, {propose, ProposalID, Proposal}).

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
		 promised = false,
		 accepted = false,
	         prepare_fun = fun paxos_acceptor:prepare/3,
	         accept_fun = fun paxos_acceptor:accept/3 } }.

%%TODO: ignore if already proposal issued / hwo to get proposalID
handle_call({propose, ProposalID, Proposal}, _From, #state{prepare_fun = PFun} = State) ->
    lists:foreach( fun(Acceptor) -> 
			   PFun(Acceptor, State#state.paxos_id, ProposalID) end, State#state.acceptors),
    {reply, ok, State#state{proposal_id = ProposalID, proposal = Proposal} };
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({promise, PromiseMessage}, State) when State#state.accepted orelse 
						   State#state.promised orelse 
						   PromiseMessage#promise_message.paxos_id /= State#state.paxos_id orelse
						   PromiseMessage#promise_message.proposal_id /= State#state.proposal_id ->
    {noreply, State};
handle_cast({promise, #promise_message{acceptor_ref = Acceptor} = PromiseMessage }, State)  ->
    case valid_acceptor(Acceptor, State) of
	true ->
	    %%TODO: check whether promises contain a quorum
	    NewState = request_accept_on_quorum(add_promise(PromiseMessage, State)),
	    {noreply, NewState};
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
add_promise([ Promise | _ ], Promise, State) ->
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

has_quorum(List, State) ->
    length(List) >= State#state.quorum.

pick_value(Promises, State) ->
    pick_value(Promises, {-1, nil}, State).

pick_value([], {-1, nil}, State) ->
    State#state.proposal;
pick_value([], {_, HighestProposal}, _) ->
    HighestProposal;
pick_value([Promise | Promises], {HighestNumber, Value}, State) when HighestNumber >= Promise#promise_message.accepted_proposal->
    pick_value(Promises, {HighestNumber, Value}, State);
pick_value([Promise | Promises], {_, _}, State) ->
    pick_value(Promises, {Promise#promise_message.accepted_proposal, Promise#promise_message.accepted_value}, State).


request_accept_on_quorum(State) ->
    request_accept_on_quorum(has_quorum(State#state.promises, State), State).

request_accept_on_quorum(false, State) ->
    State;
request_accept_on_quorum(true, State) ->
    AFun = State#state.accept_fun,
    Value = pick_value(State#state.promises, State),
    AcceptedMessage = #accept_message{ paxos_id = State#state.paxos_id,
				       proposal_id = State#state.proposal_id,
				       value = Value },
    lists:foreach( fun(Acceptor) -> 
			   AFun(Acceptor, self(), AcceptedMessage) end, State#state.acceptors),
    State#state{promised = true}.
  


%%%===================================================================
%%% Tests
%%%===================================================================

-ifdef(TEST).

%% TODO: Replace with tester
ignore(_, _, _) ->
    ignore.

test_state(PaxosID, ProposalID, Quorum, Acceptors) ->
    #state{ paxos_id = PaxosID,
	    proposal_id = ProposalID,
	    quorum = Quorum, 
	    acceptors = Acceptors, 
	    promises = [], 
	    accepts = [],
	    accept_fun = fun ignore/3
	  }.

test_state(PaxosID, ProposalID, Quorum, Acceptors, Promises) ->
    #state{ paxos_id = PaxosID, 
	    proposal_id = ProposalID,
	    quorum = Quorum, 
	    acceptors = Acceptors, 
	    promises = Promises, 
	    accepts = [],
	    accept_fun = fun ignore/3
	  }.


ignore_promise_for_wrong_paxos_id_test() ->
    State = test_state(1, 1, 1, []),
    ?assertEqual({noreply, State}, handle_cast({promise, #promise_message{paxos_id = 2, proposal_id = 1 }}, State)).

ignore_promise_for_wrong_proposal_id_test() ->
    State = test_state(1, 1, 1, []),
    ?assertEqual({noreply, State}, handle_cast({promise, #promise_message{proposal_id = 2, paxos_id = 1 }}, State)).

ignore_promise_if_proposal_was_already_promised_test() ->
    State = test_state(1, 1, 1, []),
    AcceptedState = State#state{promised = true},
    ?assertEqual({noreply, AcceptedState}, handle_cast({promise, #promise_message{paxos_id = 1, proposal_id = 1}}, AcceptedState)).

ignore_promise_if_proposal_was_already_accepted_test() ->
    State = test_state(1, 1, 1, []),
    AcceptedState = State#state{accepted = true},
    ?assertEqual({noreply, AcceptedState}, handle_cast({promise, #promise_message{paxos_id = 1, proposal_id = 1}}, AcceptedState)).

add_new_promise_test() ->
    State = test_state(1, 1, 1, []),
    ?assertEqual(State#state{promises = [promise]}, add_promise(promise, State)).

ignore_existing_promise_test() ->
    State = test_state(1, 1, 1, [], [promise]),
    ?assertEqual(State, add_promise(promise, State)).

append_to_existing_promise_test() ->
    State = test_state(1, 1, 1, [], [promise1]),
    ?assertEqual(State#state{promises = [promise2, promise1]}, add_promise(promise2, State)).

valid_acceptor_found_test() ->
    State = test_state(1, 1, 1, [acceptor1, acceptor2]),
    ?assert(valid_acceptor(acceptor1, State)).

invalid_acceptor_test() ->
    State = test_state(1, 1, 1, [acceptor1, acceptor2]),
    ?assertNot(valid_acceptor(acceptor3, State)).

cast_add_promise_test() ->
    PromiseMessage = #promise_message{acceptor_ref = acceptor1, paxos_id = 1, proposal_id = 1, accepted_proposal = -1, accepted_value = nil},
    State = test_state(1, 1, 1, [acceptor1, acceptor2, acceptor3], []),
    ?assertEqual({noreply, State#state{promises = [PromiseMessage], promised = true }}, handle_cast({promise, PromiseMessage}, State)).

cast_ignore_existing_promise_in_state_test() ->
    Quorum = 2,
    PromiseMessage = #promise_message{acceptor_ref = acceptor1, paxos_id = 1, proposal_id = 1, accepted_proposal = -1, accepted_value = nil},
    State = test_state(1, 1, Quorum, [acceptor1, acceptor2, acceptor3], [PromiseMessage]),
    ?assertEqual({noreply, State}, handle_cast({promise, PromiseMessage}, State)).

cast_ignore_invalid_acceptor_promise_test() ->
    State = test_state(1, 1, 1, [acceptor1, acceptor2, acceptor3], [acceptor1, acceptor2]),
    ?assertEqual({noreply, State}, handle_cast({promise, #promise_message{acceptor_ref = acceptor4, paxos_id = 1}}, State)).

has_quorum_test() ->
    State = test_state(1, 1, 3, []),
    ?assert(has_quorum([a1, a2, a3], State)),
    ?assertNot(has_quorum([a1], State)).

-endif.


