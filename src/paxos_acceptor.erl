%%%-------------------------------------------------------------------
%%% @author Monika Moser <monika.m.moser@googlemail.com>
%%% @copyright 2011, Monika Moser
%%% @doc Paxos acceptor
%%% @end
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

-record(state, {paxos_id, promised_proposal, accepted_proposal, accepted_val}).

prepare(AcceptorPID, ProposerPID, PrepareMessage) ->
    gen_server:cast(AcceptorPID, {prepare, ProposerPID, PrepareMessage}). 

accept(AcceptorPID, ProposerPID, AcceptMessage) ->
    gen_server:cast(AcceptorPID, {accept, ProposerPID, AcceptMessage}).


init([ PaxosID ]) ->
    {ok, #state{paxos_id = PaxosID, promised_proposal = -1, accepted_proposal = -1, accepted_val = nil}}.


handle_cast({prepare, _ProposerPID, PrepareMessage}, State) when PrepareMessage#prepare_message.paxos_id /= State#state.paxos_id ->
    %% TODO: optimize send reject
    {noreply, State};
handle_cast({prepare, _ProposerPID, PrepareMessage}, State) when PrepareMessage#prepare_message.proposal_id =< State#state.promised_proposal ->
    %% TODO: optimize send reject
    {noreply, State};
handle_cast({prepare, ProposerPID, PrepareMessage}, State) ->
    Reply = {promise, State#state.accepted_proposal, State#state.accepted_val},
    %%% TODO: make permanent. Store on disk!
    ProposerPID ! Reply,
    {noreply, State#state{promised_proposal = PrepareMessage#prepare_message.proposal_id}};
handle_cast({accept, _ProposerPID, AcceptMessage}, State) when AcceptMessage#accept_message.paxos_id /= State#state.paxos_id ->
    %% TODO: optimize send reject
    {noreply, State};
handle_cast({accept, _ProposerPID, AcceptMessage}, State) when AcceptMessage#accept_message.proposal_id =< State#state.promised_proposal ->
    %% TODO: optimize send reject
    {noreply, State};
handle_cast({accept, ProposerPID, #accept_message{proposal_id = Pid, value = Value}}, State) ->
    ProposerPID ! {accept, Pid},
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
    

reject_a_proposal_message_with_wrong_paxosID_test() ->
    State = #state{paxos_id = 1, promised_proposal = 2, accepted_proposal = -1, accepted_val = nil},
    ?assertEqual({noreply, State}, handle_cast({prepare, self(), #prepare_message{ paxos_id = 2 }}, State)).

reject_an_accept_message_with_wrong_paxosID_test() ->
    State = #state{paxos_id = 1, promised_proposal = 2, accepted_proposal = -1, accepted_val = nil},
    ?assertEqual({noreply, State}, handle_cast({accept, self(), #accept_message{ paxos_id = 2 }}, State)).

reject_prepare_with_smaller_proposal_number_test() ->
    State = #state{paxos_id = 1, promised_proposal = 2, accepted_proposal = -1, accepted_val = nil},
    ?assertEqual({noreply, State}, handle_cast({prepare, self(), #prepare_message{ paxos_id = 1, proposal_id = 1 }}, State)).

reject_prepare_with_equal_proposal_number_test() ->
    State = #state{paxos_id = 1, promised_proposal = 2, accepted_proposal = -1, accepted_val = nil},
    ?assertEqual({noreply, State}, handle_cast({prepare, self(), #prepare_message{ paxos_id = 1, proposal_id = 2 }}, State)).

promise_prepare_with_greater_proposal_number_test() ->
    State = #state{paxos_id = 1, promised_proposal = 2, accepted_proposal = -1, accepted_val = nil},
    TestReceiver = test_receiver({promise, State#state.accepted_proposal, State#state.accepted_val}, self()),
    ?assertEqual({noreply, State#state{promised_proposal = 3}}, handle_cast({prepare, TestReceiver, #prepare_message{ paxos_id = 1, proposal_id = 3 }}, State)),
    TestResult = receive
		     TResult -> TResult
		 end,
    ?assert(TestResult).

promise_prepare_with_greater_proposal_number_send_accepted_value_test() ->
    State = #state{paxos_id = 1, promised_proposal = 2, accepted_proposal = 1, accepted_val = 9},
    TestReceiver = test_receiver({promise, State#state.accepted_proposal, State#state.accepted_val}, self()),
    ?assertEqual({noreply, State#state{promised_proposal = 3}}, handle_cast({prepare, TestReceiver, #prepare_message{ paxos_id = 1, proposal_id = 3 }}, State)),
    TestResult = receive
		     TResult -> TResult
		 end,
    ?assert(TestResult).

-endif.
