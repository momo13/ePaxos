%%%-------------------------------------------------------------------
%%% @author Monika Moser <monika.m.moser@googlemail.com>
%%% @copyright (C) 2011, Monika Moser
%%% @doc
%%%
%%% @end
%%% Created :  4 Dec 2011 by Monika Moser <monika.m.moser@googlemail.com>
%%%-------------------------------------------------------------------

-module(paxos_acceptor).

-behaviour(gen_server).

%% API
-export([prepare/3, accept/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("paxos_messages.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, { paxos_id, 
		 promised_proposal, 
		 accepted_proposal, 
		 accepted_val,
		 promise_fun,
		 accept_fun }).

%%%===================================================================
%%% API
%%%===================================================================

prepare(AcceptorPID, ProposerPID, PrepareMessage) ->
    gen_server:cast(AcceptorPID, {prepare, ProposerPID, PrepareMessage}). 

accept(AcceptorPID, ProposerPID, AcceptMessage) ->
    gen_server:cast(AcceptorPID, {accept, ProposerPID, AcceptMessage}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([ PaxosID ]) ->
    {ok, #state{ paxos_id = PaxosID, 
		 promised_proposal = -1, 
		 accepted_proposal = -1, 
		 accepted_val = nil,
	         promise_fun = fun paxos_proposer:promise/2,
		 accept_fun = fun paxos_proposer:accept/2 }}.


handle_cast({prepare, _ProposerPID, PrepareMessage}, State) when PrepareMessage#prepare_message.paxos_id /= State#state.paxos_id ->
    {noreply, State};
handle_cast({prepare, _ProposerPID, PrepareMessage}, State) when PrepareMessage#prepare_message.proposal_id =< State#state.promised_proposal ->
    {noreply, State};
handle_cast({prepare, ProposerPID, PrepareMessage}, #state{promise_fun = PFun} = State) ->
    Reply = {promise, #promise_message{ paxos_id = State#state.paxos_id, 
				        accepted_proposal = State#state.accepted_proposal, 
					accepted_value = State#state.accepted_val} },
    %%% TODO: make permanent. Store on disk!
    PFun(ProposerPID, Reply),
    {noreply, State#state{ promised_proposal = PrepareMessage#prepare_message.proposal_id } };
handle_cast({accept, _ProposerPID, AcceptMessage}, State) when AcceptMessage#accept_message.paxos_id /= State#state.paxos_id ->
    %% TODO: optimize send reject
    {noreply, State};
handle_cast({accept, _ProposerPID, AcceptMessage}, State) when AcceptMessage#accept_message.proposal_id =< State#state.promised_proposal ->
    %% TODO: optimize send reject
    {noreply, State};
handle_cast({accept, ProposerPID, #accept_message{proposal_id = Pid, value = Value}},  #state{accept_fun = AFun} =State) ->
    AFun(ProposerPID, {accept, Pid}),
    {noreply, State#state{accepted_proposal = Pid, accepted_val = Value}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%%===================================================================
%%% Tests
%%%===================================================================

-ifdef(TEST).

equality(Message, Message) ->
    true;
equality(_, _) ->
    false.

test_receiver(ExpectedMessage, Tester) ->
    spawn(fun() ->
		  receive
		      Message ->
			  Tester ! equality(ExpectedMessage, Message)
		  end
	  end).

test_send_fun(PID, M) ->
    PID ! M.

test_state(PaxosID, PromisedP, AcceptedP, AcceptedVal) ->
    #state{paxos_id = PaxosID, 
	   promised_proposal = PromisedP,
	   accepted_proposal = AcceptedP,
	   accepted_val = AcceptedVal,
	   promise_fun = fun test_send_fun/2, 
	   accept_fun = fun test_send_fun/2}.

reject_a_proposal_message_with_wrong_paxosID_test() ->
    State = test_state(1, 2, -1, nil),
    ?assertEqual({noreply, State}, handle_cast({prepare, self(), #prepare_message{ paxos_id = 2 }}, State)).

reject_an_accept_message_with_wrong_paxosID_test() ->
    State = test_state(1, 2, -1, nil),
    ?assertEqual({noreply, State}, handle_cast({accept, self(), #accept_message{ paxos_id = 2 }}, State)).

reject_prepare_with_smaller_proposal_number_test() ->
    State = test_state(1, 2, -1, nil), 
    ?assertEqual({noreply, State}, handle_cast({prepare, self(), #prepare_message{ paxos_id = 1, proposal_id = 1 }}, State)).

reject_prepare_with_equal_proposal_number_test() ->
    State = test_state(1, 2, -1, nil), 
    ?assertEqual({noreply, State}, handle_cast({prepare, self(), #prepare_message{ paxos_id = 1, proposal_id = 2 }}, State)).

promise_prepare_with_greater_proposal_number_test() ->
    State = test_state(1, 2, -1, nil),
    TestReceiver = test_receiver({promise, #promise_message { paxos_id = State#state.paxos_id,
							      accepted_proposal = State#state.accepted_proposal, 
							      accepted_value = State#state.accepted_val } }, self()),
    ?assertEqual({noreply, State#state{promised_proposal = 3}}, handle_cast({prepare, TestReceiver, #prepare_message{ paxos_id = 1, proposal_id = 3 }}, State)),
    TestResult = receive
		     TResult -> TResult
		 end,
    ?assert(TestResult).

promise_prepare_with_greater_proposal_number_send_accepted_value_test() ->
    State = test_state(1, 2, 1, 9), 
    TestReceiver = test_receiver({promise, #promise_message { paxos_id = State#state.paxos_id,
							      accepted_proposal = State#state.accepted_proposal, 
							      accepted_value = State#state.accepted_val } }, self()),
    ?assertEqual({noreply, State#state{promised_proposal = 3}}, handle_cast({prepare, TestReceiver, #prepare_message{ paxos_id = 1, proposal_id = 3 }}, State)),
    TestResult = receive
		     TResult -> TResult
		 end,
    ?assert(TestResult).

-endif.
